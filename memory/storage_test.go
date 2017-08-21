package memory

import (
	"testing"

	"github.com/giantswarm/microstorage/microstoragetest"
)

func Test_Storage(t *testing.T) {
	storage, err := New(DefaultConfig())
	if err != nil {
		t.Fatal("expected", nil, "got", err)
	}
	microstoragetest.Test(t, storage)
}
