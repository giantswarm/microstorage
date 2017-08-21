package microstoragetest

import (
	"fmt"

	"github.com/giantswarm/microstorage"
	"github.com/giantswarm/microstorage/memory"
)

// Must creates a configured storage ready to be used in tests.
func Must() microstorage.Storage {
	config := memory.DefaultConfig()
	storage, err := memory.New(config)
	if err != nil {
		panic(fmt.Sprintf("%#v", err))
	}
	return storage
}
