package migrator

import (
	"context"
	"fmt"

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
	var err error

	m.logger.Log("debug", "checking if migraiton is already done")
	{
		v, err := dst.Search(ctx, migrationKey)
		if microstorage.IsNotFound(err) {
			// This means migration hasn't been done yet.
		} else if err != nil {
			return microerror.Maskf(err, "dst storage: searching key=%s", migrationKey)
		} else if v == migrationVal {
			m.logger.Log("info", "migration already done")
			return nil
		}
	}

	m.logger.Log("debug", "listing all keys")
	var keys []string
	{
		keys, err = src.List(ctx, "/")
		if microstorage.IsNotFound(err) {
			m.logger.Log("debug", "src sotrage is empty")
			keys = []string{} // The flow must continue so migration mark is set.
		} else if err != nil {
			return microerror.Maskf(err, "src storage: listing key=/")
		}
	}

	m.logger.Log("debug", fmt.Sprintf("transfering %d entries", len(keys)))
	for _, key := range keys {
		v, err := src.Search(ctx, key)
		if err != nil {
			return microerror.Maskf(err, "src storage: getting key=%s", key)
		}

		err = dst.Put(ctx, key, v)
		if err != nil {
			return microerror.Maskf(err, "dst storage: putting key=%s", key)
		}
	}

	m.logger.Log("debug", "setting migration mark")
	{
		err = dst.Put(ctx, migrationKey, migrationVal)
		if err != nil {
			return microerror.Maskf(err, "dst storage: putting key=%s", migrationKey)
		}
	}

	m.logger.Log("info", fmt.Sprintf("transfered %d entries", len(keys)))
	return nil
}
