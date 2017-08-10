package migrator

import (
	"context"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/microstorage"
)

const (
	repoSlug = "github.com/giantswarm/microstorage"

	migrationKey = repoSlug + "/migration"
	migrationVal = "v1"
)

type Config struct {
	Logger micrologger.Logger
}

// DefaultConfig creates a new configuration with the default settings.
func DefaultConfig() Config {
	return Config{
		Logger: nil, // Required.
	}
}

type Migrator struct {
	logger micrologger.Logger
}

func New(config Config) (*Migrator, error) {
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Logger is empty")
	}

	m := &Migrator{
		logger: config.Logger,
	}

	return m, nil
}

func (m *Migrator) Migrate(ctx context.Context, dst, src microstorage.Storage) error {
	keys, err := src.List(ctx, "/")
	if microstorage.IsNotFound(err) {
		return nil
	} else if err != nil {
		return microerror.Maskf(err, "listing keys of source storage")
	}

	// Check if the migration is already done.
	{
		v, err := dst.Search(ctx, migrationKey)
		if microstorage.IsNotFound(err) {
			// This means migration hasn't been done yet.
		} else if err != nil {
			return microerror.Maskf(err, "searching destination sotrage for migration key=%s", migrationKey)
		} else {
			if v == migrationVal {
				return nil
			}
		}
	}

	for _, key := range keys {
		v, err := src.Search(ctx, key)
		if err != nil {
			return microerror.Maskf(err, "getting key=%s value from source storage", key)
		}

		err = dst.Put(ctx, key, v)
		if err != nil {
			return microerror.Maskf(err, "putting key=%s value to destination storage", key)
		}
	}

	// Set the migration done mark.
	err = dst.Put(ctx, migrationKey, migrationVal)
	if err != nil {
		return microerror.Maskf(err, "putting migraiton key=%s into the destination storage", migrationKey)
	}

	return nil
}
