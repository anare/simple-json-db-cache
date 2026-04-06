package filejsondb

import (
	"testing"

	"github.com/kuetix/uuid"
)

func TestCollectionGetIndexes(t *testing.T) {
	col := newTestCollection(t, 1024, 100, "users")

	nameHash := uuid.Id("alice")
	payload := map[string]interface{}{
		nameHash: map[string]interface{}{
			"id":    "u1",
			"value": "alice",
			"hash":  nameHash,
			"index": "name",
			"type":  "string",
		},
	}
	if err := col.Set("u1.idx", payload); err != nil {
		t.Fatalf("Set index payload failed: %v", err)
	}

	idxName, indexes, err := col.LoadIndexes("u1")
	if err != nil {
		t.Fatalf("LoadIndexes failed: %v", err)
	}
	if idxName != "u1.idx" {
		t.Fatalf("unexpected index document name: %s", idxName)
	}

	entry, ok := indexes[nameHash].(map[string]interface{})
	if !ok {
		t.Fatalf("expected hashed index entry %q to exist", nameHash)
	}
	if entry["id"] != "u1" || entry["value"] != "alice" || entry["index"] != "name" || entry["type"] != "string" {
		t.Fatalf("unexpected index entry: %+v", entry)
	}
}

func TestCollectionAddIndexCreatesAndMergesIndexes(t *testing.T) {
	col := newTestCollection(t, 1024, 100, "users")

	if err := col.AddIndex("u1", map[string]interface{}{"name": "alice", "age": 30, "meta": map[string]string{"x": "y"}}, "name", "age", "meta"); err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	_, indexes, err := col.LoadIndexes("alice")
	if err != nil {
		t.Fatalf("LoadIndexes after first AddIndex failed: %v", err)
	}

	nameHash := uuid.Id("alice")
	nameEntry, ok := indexes[nameHash].(map[string]interface{})
	if !ok {
		t.Fatalf("expected name hash %q to exist", nameHash)
	}
	if nameEntry["id"] != "u1" || nameEntry["hash"] != nameHash || nameEntry["index"] != "name" || nameEntry["type"] != "string" || nameEntry["value"] != "alice" {
		t.Fatalf("unexpected name index entry: %+v", nameEntry)
	}

	ageHash := uuid.Id("30")
	_, indexes, err = col.LoadIndexes(ageHash)
	if err != nil {
		t.Fatalf("LoadIndexes after first AddIndex failed: %v", err)
	}
	ageEntry, ok := indexes[ageHash].(map[string]interface{})
	if !ok {
		t.Fatalf("expected age hash %q to exist", ageHash)
	}
	if ageEntry["id"] != "u1" || ageEntry["hash"] != ageHash || ageEntry["index"] != "age" || ageEntry["type"] != "int" || ageEntry["value"] != float64(30) {
		t.Fatalf("unexpected age index entry: %+v", ageEntry)
	}

	if err := col.AddIndex("u1", map[string]interface{}{"name": "alice", "active": true}, "name", "active"); err != nil {
		t.Fatalf("AddIndex merge failed: %v", err)
	}

	_, merged, err := col.LoadIndexes("true")
	if err != nil {
		t.Fatalf("LoadIndexes after merge failed: %v", err)
	}

	activeHash := uuid.Id("true")
	activeEntry, ok := merged[activeHash].(map[string]interface{})
	if !ok {
		t.Fatalf("expected active hash %q to exist", activeHash)
	}
	if activeEntry["id"] != "u1" || activeEntry["hash"] != activeHash || activeEntry["index"] != "active" || activeEntry["type"] != "bool" || activeEntry["value"] != true {
		t.Fatalf("unexpected active index entry: %+v", activeEntry)
	}
}
