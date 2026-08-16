package storage

import "testing"

func TestSkipListPutGet(t *testing.T) {
	s := newSkipList()
	s.Put("b", []byte("2"))
	s.Put("a", []byte("1"))
	s.Put("c", []byte("3"))

	for _, tc := range []struct{ key, want string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		v, found, tomb := s.Get(tc.key)
		if !found || tomb || string(v) != tc.want {
			t.Fatalf("Get(%q) = %q, found=%v tomb=%v; want %q", tc.key, v, found, tomb, tc.want)
		}
	}

	if _, found, _ := s.Get("missing"); found {
		t.Fatalf("expected missing key to be absent")
	}
}

func TestSkipListOverwrite(t *testing.T) {
	s := newSkipList()
	s.Put("a", []byte("1"))
	s.Put("a", []byte("2"))
	v, found, _ := s.Get("a")
	if !found || string(v) != "2" {
		t.Fatalf("expected overwritten value 2, got %q found=%v", v, found)
	}
	if s.size != 1 {
		t.Fatalf("expected size 1 after overwrite, got %d", s.size)
	}
}

func TestSkipListDeleteTombstone(t *testing.T) {
	s := newSkipList()
	s.Put("a", []byte("1"))
	s.Delete("a")
	v, found, tomb := s.Get("a")
	if !found || !tomb {
		t.Fatalf("expected tombstone for deleted key, got value=%q found=%v tomb=%v", v, found, tomb)
	}
}

func TestSkipListAllSortedOrder(t *testing.T) {
	s := newSkipList()
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for _, k := range keys {
		s.Put(k, []byte(k))
	}
	all := s.All()
	want := []string{"alpha", "bravo", "charlie", "delta"}
	if len(all) != len(want) {
		t.Fatalf("expected %d entries, got %d", len(want), len(all))
	}
	for i, e := range all {
		if e.key != want[i] {
			t.Fatalf("entry %d: got key %q, want %q", i, e.key, want[i])
		}
	}
}
