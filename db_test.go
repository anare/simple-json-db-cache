package simple_json_db_cache

import "testing"

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
