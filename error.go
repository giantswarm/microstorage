package microstorage

import (
	"github.com/juju/errgo"
)

var InvalidConfigError = errgo.New("invalid config")

// IsInvalidConfig represents the error matcher for public use. Code using the
// storage internally should use this public key matcher to verify if some
// storage error is of type "key not found", instead of using a specific error
// matching of some specific storage implementation. This public error matcher
// groups all necessary error matchers of more specific storage
// implementations.
func IsInvalidConfig(err error) bool {
	return errgo.Cause(err) == InvalidConfigError
}

var NotFoundError = errgo.New("not found")

// IsNotFound represents the error matcher for public use. Code using the
// storage internally should use this public key matcher to verify if some
// storage error is of type "key not found", instead of using a specific error
// matching of some specific storage implementation.
func IsNotFound(err error) bool {
	return errgo.Cause(err) == NotFoundError
}
