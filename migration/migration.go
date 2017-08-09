package migration

import (
	"context"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/microstorage"
)

func Migrate(ctx context.Context, src, dest microstorage.Storage) error {
	keys, err := src.List(ctx, "/")
	if err != nil {
		return microerror.Maskf(err, "listing keys of source storage")
	}

	for _, key := range keys {
		v, err := src.Search(ctx, key)
		if err != nil {
			return microerror.Maskf(err, "getting key=%s value from source storage", key)
		}

		err = dest.Put(ctx, key, v)
		if err != nil {
			return microerror.Maskf(err, "putting key=%s value to destination storage", key)
		}
	}

	return nil
}
