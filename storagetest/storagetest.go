package storagetest

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/giantswarm/microstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test is Storage conformance test.
func Test(t *testing.T, storage microstorage.Storage) {
	testBasicCRUD(t, storage)
	testPutIdempotent(t, storage)
	testPutMultiple(t, storage)
	testDeleteNotExisting(t, storage)
	testInvalidKey(t, storage)
	testList(t, storage)
	testListNested(t, storage)
	testListInvalid(t, storage)
}

func testBasicCRUD(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testBasicCRUD"

		ctx = context.TODO()

		baseKey = name + "-key"
		value   = name + "-value"
	)

	for _, key := range validKeyVariations(baseKey) {
		kv := microstorage.NewKV(key, value)

		ok, err := storage.Exists(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.False(t, ok, "%s: kv=%#v", name, kv)

		_, err = storage.Search(ctx, kv.Key)
		require.NotNil(t, err, "%s: kv=%#v", name, kv)
		require.True(t, microstorage.IsNotFound(err), "%s: key=%s expected IsNotFoundError", name, kv.Key)

		err = storage.Put(ctx, kv)

		ok, err = storage.Exists(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.True(t, ok, "%s: kv=%#v", name, kv)

		gotKV, err := storage.Search(ctx, key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.Equal(t, sanitize(kv), gotKV, "%s: kv=%#v", name, kv)

		err = storage.Delete(ctx, key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)

		ok, err = storage.Exists(ctx, key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.False(t, ok, "%s: kv=%#v", name, kv)

		_, err = storage.Search(ctx, key)
		require.NotNil(t, err, "%s: kv=%#v", name, kv)
		require.True(t, microstorage.IsNotFound(err), "%s: key=%s expected IsNotFoundError", name, kv.Key)
	}
}

func testPutIdempotent(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testPutIdempotent"

		ctx = context.TODO()

		baseKey        = name + "-key"
		value          = name + "-value"
		overridenValue = name + "-overriden-value"
	)

	for _, key := range validKeyVariations(baseKey) {
		kv := microstorage.NewKV(key, value)

		ok, err := storage.Exists(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.False(t, ok, "%s: kv=%#v", name, kv)

		// First Put call.

		err = storage.Put(ctx, microstorage.NewKV(kv.Key, value))
		require.NoError(t, err, "%s: kv=%#v", name, kv)

		gotKV, err := storage.Search(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.Equal(t, sanitize(kv), gotKV, "%s: kv=%#v", name, kv)

		// Second Put call with the same value.

		err = storage.Put(ctx, kv)
		require.NoError(t, err, "%s: kv=%#v", name, kv)

		gotKV, err = storage.Search(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, kv)
		require.Equal(t, sanitize(kv), gotKV, "%s: kv=%#v", name, kv)

		// Third Put call with overriding value.

		overridenKV := microstorage.NewKV(kv.Key, overridenValue)
		err = storage.Put(ctx, overridenKV)
		require.NoError(t, err, "%s: kv=%#v", name, overridenKV)

		gotKV, err = storage.Search(ctx, kv.Key)
		require.NoError(t, err, "%s: kv=%#v", name, overridenKV)
		require.Equal(t, sanitize(overridenKV), gotKV, "%s: kv=%#s", name, overridenKV)
	}
}

func testPutMultiple(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testPutMultiple"

		ctx = context.TODO()

		kvs = []microstorage.KV{
			{
				Key: name + "-key-1",
				Val: name + "-value-1",
			},
			{
				Key: name + "-key-1/nested/key",
				Val: name + "-value-1",
			},
			{
				Key: name + "-key-2",
				Val: name + "-value-2",
			},
			{
				Key: name + "-key-3",
				Val: name + "-value-3",
			},
		}
	)

	for _, kv := range kvs {
		ok, err := storage.Exists(ctx, kv.Key)
		require.NoError(t, err, "%s: key=%s", name, kv.Key)
		require.False(t, ok, "%s: key=%s", name, kv.Key)
	}

	storage.Put(ctx, kvs...)

	for _, kv := range kvs {
		gotKV, err := storage.Search(ctx, kv.Key)
		require.NoError(t, err, "%s: key=%s", name, kv.Key)
		require.Equal(t, kv, gotKV, "%s: key=%s", name, kv.Key)
	}
}

func testDeleteNotExisting(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testDeleteNotExisting"

		ctx = context.TODO()

		baseKey = name + "-key"
	)

	for _, key := range validKeyVariations(baseKey) {
		err := storage.Delete(ctx, key)
		require.NoError(t, err, name, "key=%s", key)
	}
}

func testInvalidKey(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testInvalidKey"

		ctx = context.TODO()

		value = name + "-value"
	)

	keys := []string{
		"",
		"/",
		"//",
		"///",
		"////",
		"key//",
		"//key",
		"//key/",
		"/key//",
		"/key//",
		"in//between",
		"in///////between",
	}

	for _, key := range keys {
		err := storage.Put(ctx, microstorage.NewKV(key, value))
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		err = storage.Delete(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		_, err = storage.Exists(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		// List is special and can take "/" as a key.
		if key != "/" {
			_, err = storage.List(ctx, key)
			assert.NotNil(t, err, "%s key=%s", name, key)
			assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)
		}

		_, err = storage.Search(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)
	}
}

func testList(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testList"

		ctx = context.TODO()

		baseKey = name + "-key"
		value   = name + "-value"
	)

	for _, key0 := range validKeyVariations(baseKey) {
		key1 := path.Join(key0, "one")
		key2 := path.Join(key0, "two")

		err := storage.Put(ctx, microstorage.NewKV(key1, value))
		assert.Nil(t, err, "%s: key=%s", name, key1)

		err = storage.Put(ctx, microstorage.NewKV(key2, value))
		assert.Nil(t, err, "%s: key=%s", name, key2)

		kvs := []microstorage.KV{
			microstorage.NewKV("one", value),
			microstorage.NewKV("two", value),
		}
		sort.Sort(kvSlice(kvs))

		gotKVs, err := storage.List(ctx, key0)
		assert.NoError(t, err, "%s: key=%s", name, key0)
		sort.Sort(kvSlice(gotKVs))
		assert.Equal(t, kvs, gotKVs, "%s: key=%s", name, key0)
	}
}

func testListNested(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testListNested"

		ctx = context.TODO()

		baseKey = name + "-key"
		value   = name + "-value"
	)

	for _, key := range validKeyVariations(baseKey) {
		kvs := []microstorage.KV{
			microstorage.NewKV(path.Join(key, "nested/one"), value),
			microstorage.NewKV(path.Join(key, "nested/two"), value),
			microstorage.NewKV(path.Join(key, "extremaly/nested/three"), value),
		}

		for _, kv := range kvs {
			err := storage.Put(ctx, kv)
			assert.Nil(t, err, "%s: kv=%#v", name, kv)
		}

		keyAll := "/"
		gotKVs, err := storage.List(ctx, keyAll)
		assert.NoError(t, err, "%s: key=%#v", name, keyAll)

		for _, kv := range kvs {
			kv = sanitize(kv)
			// Skip leading slash. This is like in file system
			// root. When create `touch /file`, listing it `ls /`
			// outputs `file`.
			kv.Key = kv.Key[1:]
			assert.Contains(t, gotKVs, kv, "%s: key=%#v", name, keyAll)
		}
	}
}

func testListInvalid(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testListInvalid"

		ctx = context.TODO()

		baseKey = name + "-key"
		value   = name + "-value"
	)

	for _, key0 := range validKeyVariations(baseKey) {
		key1 := path.Join(key0, "one")
		key2 := path.Join(key0, "two")

		err := storage.Put(ctx, microstorage.NewKV(key1, value))
		assert.Nil(t, err, "%s: key=%s", name, key1)

		err = storage.Put(ctx, microstorage.NewKV(key2, value))
		assert.Nil(t, err, "%s: key=%s", name, key2)

		// baseKey is key0 prefix.
		//
		// We have keys like:
		//
		// - /testListInvalid-key-XXXX/one
		// - /testListInvalid-key-XXXX/two
		//
		// Listing /testListInvalid-key should fail.
		list, err := storage.List(ctx, baseKey)
		assert.NoError(t, err, "%s: key=%s", name, baseKey)
		assert.Empty(t, list, "%s: key=%s", name, baseKey)
	}
}

var validKeyVariationsIDGen int64

func validKeyVariations(key string) []string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}

	next := func() string {
		return fmt.Sprintf("%s-%04d", key, atomic.AddInt64(&validKeyVariationsIDGen, 1))
	}

	return []string{
		next(),
		"/" + next(),
		next() + "/",
		"/" + next() + "/",
	}
}

func sanitize(kv microstorage.KV) microstorage.KV {
	k, err := microstorage.SanitizeKey(kv.Key)
	if err != nil {
		panic(err)
	}
	kv.Key = k
	return kv
}

type kvSlice []microstorage.KV

func (p kvSlice) Len() int           { return len(p) }
func (p kvSlice) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p kvSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
