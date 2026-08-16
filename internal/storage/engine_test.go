package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func openTestEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestEnginePutGetDelete(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	e := openTestEngine(t, cfg)

	if err := e.Put("a", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, err := e.Get("a")
	if err != nil || string(v) != "1" {
		t.Fatalf("Get(a) = %q, err=%v", v, err)
	}

	if err := e.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := e.Get("a"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestEngineFlushAndReadFromSSTable(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.MemtableFlushBytes = 1 // flush immediately
	e := openTestEngine(t, cfg)

	if err := e.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(e.tables) == 0 {
		t.Fatalf("expected at least one sstable after flush")
	}
	v, err := e.Get("k1")
	if err != nil || string(v) != "v1" {
		t.Fatalf("Get(k1) after flush = %q, err=%v", v, err)
	}
}

func TestEngineCrashRecoveryReplaysWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MemtableFlushBytes = 1 << 30 // never flush, force WAL replay path

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e1.Put("a", []byte("1"))
	e1.Put("b", []byte("2"))
	e1.Delete("a")
	// Simulate a crash: close the WAL file handle without a clean flush.
	e1.wal.close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("re-Open after crash: %v", err)
	}
	defer e2.Close()

	if _, err := e2.Get("a"); err != ErrNotFound {
		t.Fatalf("expected 'a' deleted after replay, got err=%v", err)
	}
	v, err := e2.Get("b")
	if err != nil || string(v) != "2" {
		t.Fatalf("expected 'b'=2 after replay, got %q err=%v", v, err)
	}
}

func TestEngineCompactionDropsTombstonesAndDuplicates(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.MemtableFlushBytes = 1
	cfg.CompactionTrigger = 1000 // don't auto-compact; we force it
	e := openTestEngine(t, cfg)

	e.Put("a", []byte("1")) // sstable 1
	e.Put("a", []byte("2")) // sstable 2 (overwrites)
	e.Delete("a")           // sstable 3 (tombstone)
	e.Put("b", []byte("keep"))

	if len(e.tables) < 3 {
		t.Fatalf("expected multiple sstables before compaction, got %d", len(e.tables))
	}

	if err := e.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(e.tables) != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", len(e.tables))
	}

	if _, err := e.Get("a"); err != ErrNotFound {
		t.Fatalf("expected 'a' to stay deleted after compaction, err=%v", err)
	}
	v, err := e.Get("b")
	if err != nil || string(v) != "keep" {
		t.Fatalf("expected 'b'=keep after compaction, got %q err=%v", v, err)
	}
}

func TestEngineScanPrefix(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.MemtableFlushBytes = 64 // force a mix of memtable + sstable entries
	e := openTestEngine(t, cfg)

	for i := 0; i < 20; i++ {
		e.Put(fmt.Sprintf("user:%d", i), []byte("v"))
	}
	e.Put("other:1", []byte("v"))

	results, err := e.Scan("user:")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 20 {
		t.Fatalf("expected 20 matching keys, got %d", len(results))
	}
	if _, ok := results["other:1"]; ok {
		t.Fatalf("scan should not include non-matching prefix")
	}
}

func TestEngineReopenPersistsData(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MemtableFlushBytes = 1

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 50; i++ {
		e1.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}
	e1.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer e2.Close()

	for i := 0; i < 50; i++ {
		v, err := e2.Get(fmt.Sprintf("k%d", i))
		want := fmt.Sprintf("v%d", i)
		if err != nil || string(v) != want {
			t.Fatalf("Get(k%d) = %q, err=%v; want %q", i, v, err, want)
		}
	}
}

func TestEngineDataDirIsCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	cfg := DefaultConfig(dir)
	e := openTestEngine(t, cfg)
	if err := e.Put("a", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}
