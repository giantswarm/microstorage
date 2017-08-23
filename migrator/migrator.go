package migrator

import (
	"context"
	"fmt"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/microstorage"
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

	m.logger.Log("debug", "listing all keys")
	srcKVs, err := src.List(ctx, "/")
	if err != nil {
		return microerror.Maskf(err, "src storage: listing key=/")
	}
	dstKVs, err := dst.List(ctx, "/")
	if err != nil {
		return microerror.Maskf(err, "dst storage: listing key=/")
	}

	var unmigrated []microstorage.KV
	{
		existingKeys := map[string]bool{}
		for _, kv := range dstKVs {
			existingKeys[kv.Key] = true
		}

		for _, kv := range srcKVs {
			if existingKeys[kv.Key] {
				continue
			}
			unmigrated = append(unmigrated, kv)
		}
	}

	m.logger.Log("debug", fmt.Sprintf("migrating %d/%d entries", len(unmigrated), len(srcKVs)))
	err = dst.Put(ctx, unmigrated...)
	if err != nil {
		return microerror.Maskf(err, "src storage: putting %d entries", len(unmigrated))
	}

	m.logger.Log("info", fmt.Sprintf("migrated %d/%d entries", len(unmigrated), len(srcKVs)))
	return nil
}
