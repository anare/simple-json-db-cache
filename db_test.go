package simple_json_db_cache

import "testing"

func TestNewDBUsesDefaultCacheSettings(t *testing.T) {
	withTempWorkingDir(t)

	db, err := NewDB("db")
	if err != nil {
		t.Fatalf("NewDB returned error: %v", err)
	}

	if db.CacheMaxBytes != defaultCacheMaxBytes || db.CacheMaxObjects != defaultCacheMaxObjects {
		t.Fatalf("default cache config mismatch: got bytes=%d objects=%d", db.CacheMaxBytes, db.CacheMaxObjects)
	}
}

func TestNewDBUsesDefaultObjectLimitWhenOnlyByteLimitProvided(t *testing.T) {
	withTempWorkingDir(t)

	db, err := NewDB("db", 2048)
	if err != nil {
		t.Fatalf("NewDB returned error: %v", err)
	}

	if db.CacheMaxBytes != 2048 || db.CacheMaxObjects != defaultCacheMaxObjects {
		t.Fatalf("partial cache config mismatch: got bytes=%d objects=%d", db.CacheMaxBytes, db.CacheMaxObjects)
	}
}

func TestNewDBAndGetDB(t *testing.T) {
	withTempWorkingDir(t)

	db, err := NewDB("db", 1024, 10)
	if err != nil {
		t.Fatalf("NewDB returned error: %v", err)
	}
	if db.GetDB() == nil {
		t.Fatal("GetDB returned nil")
	}
	if db.CacheMaxBytes != 1024 || db.CacheMaxObjects != 10 {
		t.Fatalf("cache config mismatch: got bytes=%d objects=%d", db.CacheMaxBytes, db.CacheMaxObjects)
	}
}

func TestNewDBInvalidPath(t *testing.T) {
	withTempWorkingDir(t)

	_, err := NewDB("db/invalid", 0, 0)
	if err == nil {
		t.Fatal("expected error for invalid db path")
	}
}

func TestDBNewCollectionUsesCacheConfig(t *testing.T) {
	withTempWorkingDir(t)

	db, err := NewDB("db", 64, 1)
	if err != nil {
		t.Fatalf("NewDB returned error: %v", err)
	}

	baseCol, err := db.GetDB().Collection("users")
	if err != nil {
		t.Fatalf("Collection returned error: %v", err)
	}
	if err := baseCol.Create("u1", []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Create u1 failed: %v", err)
	}
	if err := baseCol.Create("u2", []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Create u2 failed: %v", err)
	}

	col := db.NewCollection("users")
	if col.cache.maxBytes != 64 || col.cache.maxObjects != 1 {
		t.Fatalf("collection cache config mismatch: got bytes=%d objects=%d", col.cache.maxBytes, col.cache.maxObjects)
	}
	if col.Name() != "users" {
		t.Fatalf("unexpected collection name: %s", col.Name())
	}

	if _, ok := col.cache.GetAllIfComplete(); ok {
		t.Fatal("expected incomplete full snapshot because warm-up should evict with maxObjects=1")
	}
}

func TestDBNewCollectionWarmsCompleteSnapshotWhenWithinLimits(t *testing.T) {
	withTempWorkingDir(t)

	db, err := NewDB("db", 1024, 10)
	if err != nil {
		t.Fatalf("NewDB returned error: %v", err)
	}

	baseCol, err := db.GetDB().Collection("users")
	if err != nil {
		t.Fatalf("Collection returned error: %v", err)
	}
	if err := baseCol.Create("u1", []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Create u1 failed: %v", err)
	}

	col := db.NewCollection("users")
	all, ok := col.cache.GetAllIfComplete()
	if !ok {
		t.Fatal("expected warm-up to keep a complete full snapshot")
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 warmed record, got %d", len(all))
	}
	if string(all["u1"]) != `{"name":"a"}` {
		t.Fatalf("unexpected warmed value: %s", string(all["u1"]))
	}
}
