package simple_json_db_cache

import "testing"

type testDoc struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func newTestCollection(t *testing.T, maxObjects, maxBytes int, name string) *Collection {
	t.Helper()
	withTempWorkingDir(t)

	db, err := NewDB("db", maxBytes, maxObjects)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	return db.NewCollection(name)
}

func TestCollectionSetAndGetFromCache(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")

	want := testDoc{Name: "alice", Age: 30}
	if err := col.Set("u1", want); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete in underlying db failed: %v", err)
	}

	var got testDoc
	if err := col.Get("u1", &got); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected value: got %+v want %+v", got, want)
	}
}

func TestCollectionGetMissLoadsFromDBThenCaches(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	if err := col.Collection().Create("u1", []byte(`{"name":"bob","age":41}`)); err != nil {
		t.Fatalf("Create in underlying db failed: %v", err)
	}

	var first testDoc
	if err := col.Get("u1", &first); err != nil {
		t.Fatalf("Get first failed: %v", err)
	}
	if first.Name != "bob" || first.Age != 41 {
		t.Fatalf("unexpected first value: %+v", first)
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete in underlying db failed: %v", err)
	}

	var second testDoc
	if err := col.Get("u1", &second); err != nil {
		t.Fatalf("Get second failed: %v", err)
	}
	if second != first {
		t.Fatalf("expected cached value on second get, got %+v want %+v", second, first)
	}
}

func TestCollectionSetMarshalError(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	err := col.Set("u1", map[string]interface{}{"bad": make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestCollectionGetUnmarshalError(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	if err := col.Collection().Create("u1", []byte("not-json")); err != nil {
		t.Fatalf("Create in underlying db failed: %v", err)
	}

	var got testDoc
	err := col.Get("u1", &got)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestCollectionDelete(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	if err := col.Set("u1", testDoc{Name: "a"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := col.Delete("u1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if col.Exists("u1") {
		t.Fatal("expected deleted item not to exist")
	}

	var got testDoc
	err := col.Get("u1", &got)
	if err == nil {
		t.Fatal("expected get error for deleted item")
	}
}

func TestCollectionExistsUsesCache(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	if err := col.Set("u1", testDoc{Name: "cached"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete in underlying db failed: %v", err)
	}

	if !col.Exists("u1") {
		t.Fatal("expected Exists to return true from cache")
	}
}

func TestCollectionGetAllUsesCompleteSnapshotCache(t *testing.T) {
	withTempWorkingDir(t)
	db, err := NewDB("db", 1024, 100)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	base, err := db.GetDB().Collection("users")
	if err != nil {
		t.Fatalf("Collection failed: %v", err)
	}
	if err := base.Create("u1", []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Create u1 failed: %v", err)
	}
	if err := base.Create("u2", []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Create u2 failed: %v", err)
	}

	col := db.NewCollection("users")
	first := col.GetAll()
	if len(first) != 2 {
		t.Fatalf("expected 2 records, got %d", len(first))
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete u1 failed: %v", err)
	}
	if err := col.Collection().Delete("u2"); err != nil {
		t.Fatalf("Delete u2 failed: %v", err)
	}

	second := col.GetAll()
	if len(second) != 2 {
		t.Fatalf("expected cached full snapshot, got %d records", len(second))
	}
}

func TestCollectionGetAllDoesNotUseIncompleteSnapshot(t *testing.T) {
	withTempWorkingDir(t)
	db, err := NewDB("db", 1024, 1)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	base, err := db.GetDB().Collection("users")
	if err != nil {
		t.Fatalf("Collection failed: %v", err)
	}
	if err := base.Create("u1", []byte(`{"name":"a"}`)); err != nil {
		t.Fatalf("Create u1 failed: %v", err)
	}
	if err := base.Create("u2", []byte(`{"name":"b"}`)); err != nil {
		t.Fatalf("Create u2 failed: %v", err)
	}

	col := db.NewCollection("users")
	first := col.GetAll()
	if len(first) != 2 {
		t.Fatalf("expected 2 records from db, got %d", len(first))
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete u1 failed: %v", err)
	}
	if err := col.Collection().Delete("u2"); err != nil {
		t.Fatalf("Delete u2 failed: %v", err)
	}

	second := col.GetAll()
	if len(second) != 0 {
		t.Fatalf("expected db fetch after incomplete cache, got %d records", len(second))
	}
}

func TestCollectionUpdate(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	if err := col.Set("u1", testDoc{Name: "before", Age: 1}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if err := col.Update("u1", testDoc{Name: "after", Age: 2}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if err := col.Collection().Delete("u1"); err != nil {
		t.Fatalf("Delete in underlying db failed: %v", err)
	}

	var got testDoc
	if err := col.Get("u1", &got); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Name != "after" || got.Age != 2 {
		t.Fatalf("unexpected updated value: %+v", got)
	}
}

func TestCollectionUpdateMarshalError(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	err := col.Update("u1", map[string]interface{}{"bad": make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestCollectionBackendCreateErrorPropagates(t *testing.T) {
	col := newTestCollection(t, 100, 1024, "users")
	err := col.Set("dir/item", testDoc{Name: "bad id"})
	if err == nil {
		t.Fatal("expected create error for invalid id containing path separator")
	}

	var got testDoc
	if getErr := col.Get("dir/item", &got); getErr == nil {
		t.Fatal("expected get to fail for invalid id")
	}
}
