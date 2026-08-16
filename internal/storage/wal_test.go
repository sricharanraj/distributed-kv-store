package storage

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	if err := w.append(opPut, "a", []byte("1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.append(opPut, "b", []byte("2")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.append(opDelete, "a", nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	type rec struct {
		op  opCode
		key string
		val string
	}
	var got []rec
	if err := replayWAL(path, func(op opCode, key string, value []byte) {
		got = append(got, rec{op, key, string(value)})
	}); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	want := []rec{
		{opPut, "a", "1"},
		{opPut, "b", "2"},
		{opDelete, "a", ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replay mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	w.append(opPut, "a", []byte("1"))
	if err := w.truncate(); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	w.close()

	var count int
	replayWAL(path, func(op opCode, key string, value []byte) { count++ })
	if count != 0 {
		t.Fatalf("expected empty log after truncate, got %d records", count)
	}
}

func TestReplayMissingFileIsNoop(t *testing.T) {
	if err := replayWAL(filepath.Join(t.TempDir(), "nope.log"), func(op opCode, key string, value []byte) {
		t.Fatal("should not be called")
	}); err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
}
