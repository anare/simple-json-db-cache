package simple_json_db_cache

import (
	"bytes"
	"testing"
)

func TestCollectionCacheCalcSize(t *testing.T) {
	cache := NewCollectionCache(10, 1024)

	if size := cache.CalcSize("ab", []byte("123")); size != 5 {
		t.Fatalf("unexpected size: got %d want 5", size)
	}
}

func TestCollectionCacheSetGetClonesBytes(t *testing.T) {
	cache := NewCollectionCache(10, 1024)
	in := []byte("value")
	cache.Set("k", in)
	in[0] = 'X'

	out, ok := cache.Get("k")
	if !ok {
		t.Fatal("expected key in cache")
	}
	if string(out) != "value" {
		t.Fatalf("expected stored value to be cloned, got %q", string(out))
	}

	out[0] = 'Y'
	out2, ok := cache.Get("k")
	if !ok {
		t.Fatal("expected key in cache on second read")
	}
	if string(out2) != "value" {
		t.Fatalf("expected returned value to be cloned, got %q", string(out2))
	}
}

func TestCollectionCacheEvictsByObjectLimitLRU(t *testing.T) {
	cache := NewCollectionCache(2, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected a to exist")
	}
	cache.Set("c", []byte("3"))

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected b to be evicted as LRU")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected a to remain in cache")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected c to remain in cache")
	}
}

func TestCollectionCacheEvictsByByteLimit(t *testing.T) {
	cache := NewCollectionCache(0, 6)
	cache.Set("a", []byte("11"))
	cache.Set("b", []byte("22"))
	cache.Set("c", []byte("33"))

	if _, ok := cache.Get("a"); ok {
		t.Fatal("expected a to be evicted by byte limit")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Fatal("expected b to remain")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected c to remain")
	}
}

func TestCollectionCacheOversizedItemIsRejectedAndReplacesExisting(t *testing.T) {
	cache := NewCollectionCache(10, 5)
	cache.Set("k", []byte("1"))
	if _, ok := cache.Get("k"); !ok {
		t.Fatal("expected initial key to exist")
	}

	cache.Set("k", []byte("12345"))
	if _, ok := cache.Get("k"); ok {
		t.Fatal("expected oversized value to remove existing key")
	}
}

func TestCollectionCacheSetUpdatesExistingEntry(t *testing.T) {
	cache := NewCollectionCache(10, 1024)
	cache.Set("k", []byte("1"))
	cache.Set("k", []byte("22"))

	if len(cache.items) != 1 {
		t.Fatalf("expected a single cache entry, got %d", len(cache.items))
	}
	if cache.currentBytes != len("k")+len("22") {
		t.Fatalf("unexpected byte count: got %d", cache.currentBytes)
	}

	out, ok := cache.Get("k")
	if !ok {
		t.Fatal("expected updated key to exist")
	}
	if string(out) != "22" {
		t.Fatalf("unexpected updated value: %q", string(out))
	}
}

func TestCollectionCacheWarmAndGetAllIfComplete(t *testing.T) {
	cache := NewCollectionCache(10, 1024)
	source := map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	}
	cache.Warm(source)

	all, ok := cache.GetAllIfComplete()
	if !ok {
		t.Fatal("expected full snapshot")
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}

	source["a"][0] = 'X'
	if string(all["a"]) != "1" {
		t.Fatalf("expected warm to clone source bytes, got %q", string(all["a"]))
	}

	all["b"][0] = 'Y'
	again, ok := cache.GetAllIfComplete()
	if !ok {
		t.Fatal("expected full snapshot on second read")
	}
	if !bytes.Equal(again["b"], []byte("2")) {
		t.Fatalf("expected snapshot reads to be cloned, got %q", string(again["b"]))
	}
}

func TestCollectionCacheWarmIncompleteWhenEvictedOrOversized(t *testing.T) {
	cache := NewCollectionCache(1, 10)
	cache.Warm(map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	})
	if _, ok := cache.GetAllIfComplete(); ok {
		t.Fatal("expected incomplete snapshot when warm-up evicts")
	}

	cache2 := NewCollectionCache(10, 3)
	cache2.Warm(map[string][]byte{"big": []byte("1234")})
	if _, ok := cache2.GetAllIfComplete(); ok {
		t.Fatal("expected incomplete snapshot when warm-up skips oversized item")
	}
}

func TestCollectionCacheWarmReplacesExistingState(t *testing.T) {
	cache := NewCollectionCache(10, 1024)
	cache.Set("stale", []byte("x"))

	cache.Warm(map[string][]byte{"fresh": []byte("y")})

	if _, ok := cache.Get("stale"); ok {
		t.Fatal("expected warm-up to purge stale entries first")
	}
	all, ok := cache.GetAllIfComplete()
	if !ok {
		t.Fatal("expected a complete snapshot after warm-up")
	}
	if len(all) != 1 || string(all["fresh"]) != "y" {
		t.Fatalf("unexpected warmed snapshot: %+v", all)
	}
}

func TestCollectionCacheDeleteAndPurge(t *testing.T) {
	cache := NewCollectionCache(10, 1024)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Delete("a")
	if _, ok := cache.Get("a"); ok {
		t.Fatal("expected a to be deleted")
	}

	cache.Purge()
	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected cache to be empty after purge")
	}
	if _, ok := cache.GetAllIfComplete(); ok {
		t.Fatal("expected full snapshot flag to be reset after purge")
	}
}
