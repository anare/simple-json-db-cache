package simple_json_db_cache

import (
	"encoding/json"
	"fmt"
	"sync"

	jsondb "github.com/pnkj-kmr/simple-json-db"
)

type Collection struct {
	db         *DB
	collection jsondb.Collection
	name       string
	cache      *CollectionCache
	mu         sync.RWMutex
}

func NewCollection(db *DB, cache *CollectionCache, name string) *Collection {
	col, err := db.GetDB().Collection(name)
	if err != nil {
		panic(err)
	}
	cache.Warm(col.GetAllByName())

	return &Collection{
		db:         db,
		collection: col,
		name:       name,
		cache:      cache,
	}
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) DB() jsondb.DB {
	return c.db.GetDB()
}

func (c *Collection) Collection() jsondb.Collection {
	return c.collection
}

func (c *Collection) String() string {
	return fmt.Sprintf("Collection: %s", c.name)
}

func (c *Collection) Set(id string, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := c.collection.Create(id, v); err != nil {
		return err
	}

	c.cache.Set(id, v)
	return nil
}

func (c *Collection) Get(id string, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.cache.Get(id); ok {
		return json.Unmarshal(cached, value)
	}

	v, err := c.collection.Get(id)
	if err != nil {
		return err
	}
	c.cache.Set(id, v)

	return json.Unmarshal(v, value)
}

func (c *Collection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.collection.Delete(id); err != nil {
		return err
	}
	c.cache.Delete(id)
	return nil
}

func (c *Collection) Exists(id string) bool {
	if _, ok := c.cache.Get(id); ok {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cache.Get(id); ok {
		return true
	}

	_, err := c.collection.Get(id)
	return err == nil
}

func (c *Collection) GetAll() map[string][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.cache.GetAllIfComplete(); ok {
		return cached
	}

	data := c.collection.GetAllByName()
	c.cache.Warm(data)
	return data
}

// Update updates an existing item by deleting and recreating it
func (c *Collection) Update(id string, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Delete first, then create - this is the pattern used by simple-json-Db
	// for updates since Create might fail if item exists
	// if err := c.collection.Delete(id); err != nil {
	// 	return err
	// }

	if err := c.collection.Create(id, v); err != nil {
		return err
	}

	c.cache.Set(id, v)
	return nil
}
