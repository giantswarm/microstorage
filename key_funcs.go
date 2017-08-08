package microstorage

import (
	"strings"

	"github.com/giantswarm/microerror"
)

// SanitizeKey ensures the key has leading slash and does not have trailing
// slash. It fails with InvalidKeyError when key is invalid.
func SanitizeKey(key string) (string, error) {
	if !isValidKey(key) {
		return "", microerror.Maskf(InvalidKeyError, "key=%s", key)
	}

	if key[0] != '/' {
		key = "/" + key
	}
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}

	return key, nil
}

// isValidKey check if this storage key is valid, i.e. does not contain double
// slashes, is not empty, and does not contain only slashes.
func isValidKey(key string) bool {
	if key == "" || key == "/" {
		return false
	}
	return !strings.Contains(key, "//")
}
