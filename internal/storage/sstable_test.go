package storage

import "testing"

func TestSSTableWriteAndGet(t *testing.T) {
	dir := t.TempDir()
	entries := []entry{
		{key: "apple", value: []byte("red")},
		{key: "banana", value: []byte("yellow")},
		{key: "cherry", value: []byte("dark red")},
		{key: "date", value: nil, tombstone: true},
	}
	st, err := writeSSTable(dir, 1, entries)
	if err != nil {
		t.Fatalf("writeSSTable: %v", err)
	}

	v, found, tomb, err := st.get("banana")
	if err != nil || !found || tomb || string(v) != "yellow" {
		t.Fatalf("get(banana) = %q found=%v tomb=%v err=%v", v, found, tomb, err)
	}

	_, found, tomb, err = st.get("date")
	if err != nil || !found || !tomb {
		t.Fatalf("get(date) expected tombstone, found=%v tomb=%v err=%v", found, tomb, err)
	}

	_, found, _, err = st.get("nonexistent")
	if err != nil || found {
		t.Fatalf("get(nonexistent) expected not found, got found=%v err=%v", found, err)
	}
}

func TestSSTableLoadFromDisk(t *testing.T) {
	dir := t.TempDir()
	entries := make([]entry, 0, 200)
	for i := 0; i < 200; i++ {
		entries = append(entries, entry{key: keyFor(i), value: []byte(keyFor(i))})
	}
	if _, err := writeSSTable(dir, 7, entries); err != nil {
		t.Fatalf("writeSSTable: %v", err)
	}

	st, err := loadSSTable(dir, 7)
	if err != nil {
		t.Fatalf("loadSSTable: %v", err)
	}
	for i := 0; i < 200; i += 17 {
		k := keyFor(i)
		v, found, tomb, err := st.get(k)
		if err != nil || !found || tomb || string(v) != k {
			t.Fatalf("get(%q) = %q found=%v tomb=%v err=%v", k, v, found, tomb, err)
		}
	}
}

func keyFor(i int) string {
	const alphabet = "0123456789"
	digits := []byte{alphabet[i/100], alphabet[(i/10)%10], alphabet[i%10]}
	return "k" + string(digits)
}
