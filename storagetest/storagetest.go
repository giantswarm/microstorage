package storagetest

import (
	"context"
	"fmt"
	"path"
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
	testDeleteNotExisting(t, storage)
	testInvalidKey(t, storage)
	testList(t, storage)
}

func testBasicCRUD(t *testing.T, storage microstorage.Storage) {
	var (
		name = "testBasicCRUD"

		ctx = context.TODO()

		baseKey = name + "-key"
		value   = name + "-value"
	)

	for _, key := range validKeyVariations(baseKey) {
		ok, err := storage.Exists(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.False(t, ok, "%s: key=%s", name, key)

		v, err := storage.Search(ctx, key)
		require.NotNil(t, err, "%s: key=%s", name, key)
		require.True(t, microstorage.IsNotFound(err), "%s: key=%s expected IsNotFoundError", name, key)

		err = storage.Put(ctx, key, value)
		require.NoError(t, err, "%s: key=%s", name, key)

		ok, err = storage.Exists(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.True(t, ok, "%s: key=%s", name, key)

		v, err = storage.Search(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.Equal(t, value, v, "%s: key=%s", name, key)

		err = storage.Delete(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)

		ok, err = storage.Exists(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.False(t, ok, "%s: key=%s", name, key)

		v, err = storage.Search(ctx, key)
		require.NotNil(t, err, "%s: key=%s", name, key)
		require.True(t, microstorage.IsNotFound(err), "%s: key=%s expected IsNotFoundError", name, key)
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
		ok, err := storage.Exists(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.False(t, ok, "%s: key=%s", name, key)

		// First Put call.

		err = storage.Put(ctx, key, value)
		require.NoError(t, err, "%s: key=%s", name, key)

		v, err := storage.Search(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.Equal(t, value, v, "%s: key=%s", name, key)

		// Second Put call with the same value.

		err = storage.Put(ctx, key, value)
		require.NoError(t, err, "%s: key=%s", name, key)

		v, err = storage.Search(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.Equal(t, value, v, "%s: key=%s", name, key)

		// Third Put call with overriding value.

		err = storage.Put(ctx, key, overridenValue)
		require.NoError(t, err, "%s: key=%s", name, key)

		v, err = storage.Search(ctx, key)
		require.NoError(t, err, "%s: key=%s", name, key)
		require.Equal(t, overridenValue, v, "%s: key=%s", name, key)
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
		err := storage.Create(ctx, key, value)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		err = storage.Put(ctx, key, value)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		err = storage.Delete(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		_, err = storage.Exists(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

		_, err = storage.List(ctx, key)
		assert.NotNil(t, err, "%s key=%s", name, key)
		assert.True(t, microstorage.IsInvalidKey(err), "%s: expected InvalidKeyError for key=%s", name, key)

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

		err := storage.Create(ctx, key1, value)
		assert.Nil(t, err, "%s: key=%s", name, key1)

		err = storage.Create(ctx, key2, value)
		assert.Nil(t, err, "%s: key=%s", name, key2)

		wkeys := []string{
			"one",
			"two",
		}

		keys, err := storage.List(ctx, key0)
		assert.Nil(t, err, "%s: key=%s", name, key0)
		assert.Equal(t, wkeys, keys, "%s: key=%s", name, key0)
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
