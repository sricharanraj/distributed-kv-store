package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// sparseIndexStride controls how often we record an offset into the sparse
// index: every Nth key gets an entry, trading a bit of scan work for a much
// smaller in-memory index.
const sparseIndexStride = 32

// indexEntry is one sparse-index checkpoint: key -> byte offset in the data file.
type indexEntry struct {
	key    string
	offset int64
}

// sstable is an immutable, sorted, on-disk segment produced by flushing the
// memtable (or by compacting older segments). Reads consult the bloom
// filter first, then binary-search the sparse index, then scan a small
// window of the data file.
type sstable struct {
	id        int64
	dataPath  string
	bloom     *bloomFilter
	index     []indexEntry
	minKey    string
	maxKey    string
}

// record layout in the data file (little-endian):
//
//	[1 byte tombstone][4 byte keyLen][key][4 byte valLen][value]
func writeSSTable(dir string, id int64, entries []entry) (*sstable, error) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	dataPath := sstablePath(dir, id)
	f, err := os.Create(dataPath)
	if err != nil {
		return nil, fmt.Errorf("create sstable: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	bf := newBloomFilter(len(entries), 0.01)
	st := &sstable{id: id, dataPath: dataPath, bloom: bf}

	var offset int64
	for i, e := range entries {
		bf.Add(e.key)
		if i == 0 {
			st.minKey = e.key
		}
		st.maxKey = e.key

		if i%sparseIndexStride == 0 {
			st.index = append(st.index, indexEntry{key: e.key, offset: offset})
		}

		var tomb byte
		if e.tombstone {
			tomb = 1
		}
		n, err := w.Write([]byte{tomb})
		if err != nil {
			return nil, err
		}
		offset += int64(n)

		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(e.key)))
		w.Write(lenBuf[:])
		w.Write([]byte(e.key))
		offset += 4 + int64(len(e.key))

		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(e.value)))
		w.Write(lenBuf[:])
		w.Write(e.value)
		offset += 4 + int64(len(e.value))
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}

	if err := os.WriteFile(bloomPath(dir, id), bf.serialize(), 0o644); err != nil {
		return nil, err
	}
	if err := writeIndexFile(indexPath(dir, id), st.index); err != nil {
		return nil, err
	}
	return st, nil
}

// get looks up key in this segment. found=false means "not in this segment";
// tombstone=true means the key was deleted here (callers must stop the
// newest-to-oldest search rather than fall through to older segments).
func (st *sstable) get(key string) (value []byte, found bool, tombstone bool, err error) {
	if key < st.minKey || key > st.maxKey {
		return nil, false, false, nil
	}
	if !st.bloom.MightContain(key) {
		return nil, false, false, nil
	}

	// Binary search the sparse index for the last checkpoint <= key.
	i := sort.Search(len(st.index), func(i int) bool { return st.index[i].key > key })
	if i == 0 {
		return nil, false, false, nil
	}
	start := st.index[i-1].offset

	f, err := os.Open(st.dataPath)
	if err != nil {
		return nil, false, false, err
	}
	defer f.Close()
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, false, false, err
	}
	r := bufio.NewReader(f)

	for j := 0; j < sparseIndexStride; j++ {
		tombByte, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, false, err
		}
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, false, false, err
		}
		klen := binary.LittleEndian.Uint32(lenBuf[:])
		kb := make([]byte, klen)
		if _, err := io.ReadFull(r, kb); err != nil {
			return nil, false, false, err
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, false, false, err
		}
		vlen := binary.LittleEndian.Uint32(lenBuf[:])
		vb := make([]byte, vlen)
		if _, err := io.ReadFull(r, vb); err != nil {
			return nil, false, false, err
		}

		k := string(kb)
		if k == key {
			return vb, true, tombByte == 1, nil
		}
		if k > key {
			return nil, false, false, nil
		}
	}
	return nil, false, false, nil
}

func writeIndexFile(path string, idx []indexEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	var lenBuf [4]byte
	var offBuf [8]byte
	for _, e := range idx {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(e.key)))
		w.Write(lenBuf[:])
		w.Write([]byte(e.key))
		binary.LittleEndian.PutUint64(offBuf[:], uint64(e.offset))
		w.Write(offBuf[:])
	}
	return w.Flush()
}

func loadIndexFile(path string) ([]indexEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var idx []indexEntry
	pos := 0
	for pos < len(data) {
		klen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		key := string(data[pos : pos+int(klen)])
		pos += int(klen)
		offset := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		idx = append(idx, indexEntry{key: key, offset: int64(offset)})
	}
	return idx, nil
}

// loadSSTable reconstructs an sstable's bloom filter and sparse index from
// disk (used on engine startup) without re-scanning the whole data file.
func loadSSTable(dir string, id int64) (*sstable, error) {
	bloomData, err := os.ReadFile(bloomPath(dir, id))
	if err != nil {
		return nil, err
	}
	idx, err := loadIndexFile(indexPath(dir, id))
	if err != nil {
		return nil, err
	}
	st := &sstable{
		id:       id,
		dataPath: sstablePath(dir, id),
		bloom:    deserializeBloom(bloomData),
		index:    idx,
	}
	if len(idx) > 0 {
		st.minKey = idx[0].key
		st.maxKey = idx[len(idx)-1].key
	}
	// Rescan the tail to find the true max key (index only records checkpoints).
	entries, err := readAllEntries(st.dataPath)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		st.minKey = entries[0].key
		st.maxKey = entries[len(entries)-1].key
	}
	return st, nil
}

func readAllEntries(dataPath string) ([]entry, error) {
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var out []entry
	for {
		tombByte, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}
		klen := binary.LittleEndian.Uint32(lenBuf[:])
		kb := make([]byte, klen)
		if _, err := io.ReadFull(r, kb); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}
		vlen := binary.LittleEndian.Uint32(lenBuf[:])
		vb := make([]byte, vlen)
		if _, err := io.ReadFull(r, vb); err != nil {
			return nil, err
		}
		out = append(out, entry{key: string(kb), value: vb, tombstone: tombByte == 1})
	}
	return out, nil
}

func sstablePath(dir string, id int64) string { return fmt.Sprintf("%s/%06d.sst", dir, id) }
func bloomPath(dir string, id int64) string    { return fmt.Sprintf("%s/%06d.bloom", dir, id) }
func indexPath(dir string, id int64) string    { return fmt.Sprintf("%s/%06d.idx", dir, id) }
