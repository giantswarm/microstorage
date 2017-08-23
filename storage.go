package microstorage

import (
	"context"
)

// KV is a key-value pair.
type KV struct {
	// Key should take a format of path separated by slashes "/". E.g.
	// "/a/b/c". Storage implementations should validate the key before
	// storing them. Key sanitized before inserting to the storage. I.e.
	// leading slash can be added and trailing slash can be removed. E.g.
	// "/a/b/c", "a/b/c/", "a/b/c/", and "/a/b/c/" represent the same key.
	Key string
	// Val is an arbitrary value stored under the Key.
	Val string
}

func NewKV(key, val string) KV {
	return KV{
		Key: key,
		Val: val,
	}
}

// Storage represents the abstraction for underlying storage backends.
type Storage interface {
	// Put stores the given value under the given key. If the value
	// under the key already exists Put overrides it.
	Put(ctx context.Context, kvs ...KV) error
	// Delete removes the value stored under the given key.
	Delete(ctx context.Context, key string) error
	// Exists checks if a value under the given key exists or not.
	Exists(ctx context.Context, key string) (bool, error)
	// List does a lookup for all key-values stored under the key, and
	// returns the relative key path, if any.
	// E.g: listing /foo/, with the key /foo/bar, returns bar.
	List(ctx context.Context, key string) ([]KV, error)
	// Search does a lookup for the value stored under key and returns it,
	// if any.
	Search(ctx context.Context, key string) (KV, error)
}
