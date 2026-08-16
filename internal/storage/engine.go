package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Config controls flush and compaction behaviour.
type Config struct {
	DataDir string
	// MemtableFlushBytes is the approximate memtable size that triggers a
	// flush to a new SSTable.
	MemtableFlushBytes int
	// CompactionTrigger is the number of SSTables that triggers a merge
	// into a single compacted segment.
	CompactionTrigger int
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:            dataDir,
		MemtableFlushBytes: 4 << 20, // 4MB
		CompactionTrigger:  6,
	}
}

// Engine is the embeddable storage engine: memtable + WAL for durability,
// immutable SSTables on disk for the bulk of the data, and background
// compaction to bound the number of segments a read has to check.
//
// Concurrency: a single sync.RWMutex guards the memtable and the SSTable
// list. Reads take the read lock (multiple readers proceed concurrently);
// writes and flush/compaction take the write lock.
type Engine struct {
	cfg Config
	mu  sync.RWMutex

	mem     *skipList
	wal     *WAL
	tables  []*sstable // newest first
	nextID  int64

	closed bool
}

// Open creates or recovers an engine rooted at cfg.DataDir: it loads any
// existing SSTables and replays the WAL to rebuild the memtable.
func Open(cfg Config) (*Engine, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	e := &Engine{cfg: cfg, mem: newSkipList()}

	ids, err := existingSSTableIDs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		st, err := loadSSTable(cfg.DataDir, id)
		if err != nil {
			return nil, fmt.Errorf("load sstable %d: %w", id, err)
		}
		e.tables = append(e.tables, st)
		if id >= e.nextID {
			e.nextID = id + 1
		}
	}
	// newest first
	sort.Slice(e.tables, func(i, j int) bool { return e.tables[i].id > e.tables[j].id })

	walPath := filepath.Join(cfg.DataDir, "wal.log")
	if err := replayWAL(walPath, func(op opCode, key string, value []byte) {
		if op == opPut {
			e.mem.Put(key, value)
		} else {
			e.mem.Delete(key)
		}
	}); err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	wal, err := openWAL(walPath)
	if err != nil {
		return nil, err
	}
	e.wal = wal

	return e, nil
}

// Put writes key=value durably (WAL fsync) and applies it to the memtable.
func (e *Engine) Put(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if err := e.wal.append(opPut, key, value); err != nil {
		return err
	}
	e.mem.Put(key, value)
	return e.maybeFlushLocked()
}

// Delete writes a tombstone for key.
func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if err := e.wal.append(opDelete, key, nil); err != nil {
		return err
	}
	e.mem.Delete(key)
	return e.maybeFlushLocked()
}

// ErrNotFound is returned by Get when the key does not exist (or was deleted).
var ErrNotFound = fmt.Errorf("key not found")

// Get looks up key, checking the memtable first, then SSTables newest to oldest.
func (e *Engine) Get(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if v, found, tomb := e.mem.Get(key); found {
		if tomb {
			return nil, ErrNotFound
		}
		return v, nil
	}
	for _, st := range e.tables {
		v, found, tomb, err := st.get(key)
		if err != nil {
			return nil, err
		}
		if found {
			if tomb {
				return nil, ErrNotFound
			}
			return v, nil
		}
	}
	return nil, ErrNotFound
}

// Scan returns all live keys (and values) with the given prefix, merging the
// memtable and every SSTable. It is O(total keys) and intended for small
// administrative scans, not hot-path queries.
func (e *Engine) Scan(prefix string) (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// oldest to newest so newer entries win
	merged := map[string]entry{}
	for i := len(e.tables) - 1; i >= 0; i-- {
		entries, err := readAllEntries(e.tables[i].dataPath)
		if err != nil {
			return nil, err
		}
		for _, en := range entries {
			merged[en.key] = en
		}
	}
	for _, en := range e.mem.All() {
		merged[en.key] = en
	}

	out := map[string][]byte{}
	for k, en := range merged {
		if strings.HasPrefix(k, prefix) && !en.tombstone {
			out[k] = en.value
		}
	}
	return out, nil
}

// maybeFlushLocked flushes the memtable to a new SSTable if it has grown
// past the configured threshold, then triggers compaction if needed.
// Caller must hold e.mu (write lock).
func (e *Engine) maybeFlushLocked() error {
	if e.mem.bytes < e.cfg.MemtableFlushBytes {
		return nil
	}
	return e.flushLocked()
}

func (e *Engine) flushLocked() error {
	if e.mem.size == 0 {
		return nil
	}
	id := e.nextID
	e.nextID++

	st, err := writeSSTable(e.cfg.DataDir, id, e.mem.All())
	if err != nil {
		return fmt.Errorf("flush sstable: %w", err)
	}
	e.tables = append([]*sstable{st}, e.tables...)
	e.mem = newSkipList()
	if err := e.wal.truncate(); err != nil {
		return err
	}

	if len(e.tables) >= e.cfg.CompactionTrigger {
		return e.compactLocked()
	}
	return nil
}

// compactLocked merges every SSTable into one, keeping the newest value per
// key and dropping tombstones (since after a full compaction there is no
// older segment left for a tombstone to shadow). Caller must hold e.mu.
func (e *Engine) compactLocked() error {
	merged := map[string]entry{}
	// oldest to newest so newer overwrites older
	for i := len(e.tables) - 1; i >= 0; i-- {
		entries, err := readAllEntries(e.tables[i].dataPath)
		if err != nil {
			return err
		}
		for _, en := range entries {
			merged[en.key] = en
		}
	}

	live := make([]entry, 0, len(merged))
	for _, en := range merged {
		if !en.tombstone {
			live = append(live, en)
		}
	}

	oldTables := e.tables
	id := e.nextID
	e.nextID++
	newTable, err := writeSSTable(e.cfg.DataDir, id, live)
	if err != nil {
		return err
	}

	e.tables = []*sstable{newTable}
	for _, st := range oldTables {
		os.Remove(st.dataPath)
		os.Remove(bloomPath(e.cfg.DataDir, st.id))
		os.Remove(indexPath(e.cfg.DataDir, st.id))
	}
	return nil
}

// Flush forces the current memtable to disk (used by tests and graceful shutdown).
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushLocked()
}

// Compact forces a full compaction regardless of the configured trigger.
func (e *Engine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.tables) < 2 {
		return nil
	}
	return e.compactLocked()
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.wal.close()
}

func existingSSTableIDs(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for _, de := range entries {
		name := de.Name()
		if strings.HasSuffix(name, ".sst") {
			idStr := strings.TrimSuffix(name, ".sst")
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err == nil {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}
