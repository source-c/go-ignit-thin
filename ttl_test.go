package ignite

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"testing"
	"time"
)

type TtlTestSuite struct {
	testing2.IgniteTestSuite
	cli *Client
}

func TestTtlTestSuite(t *testing.T) {
	suite.Run(t, new(TtlTestSuite))
}

func (suite *TtlTestSuite) SetupSuite() {
	_, err := suite.StartIgnite(testing2.WithInstanceIndex(0))
	if err != nil {
		suite.T().Fatal("Failed to start ignite instance", err)
	}
	suite.cli, err = StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
}

func (suite *TtlTestSuite) TearDownSuite() {
	_ = suite.cli.Close(context.Background())
	suite.KillAllGrids()
}

func (suite *TtlTestSuite) TestCreationPolicy() {
	fixtures := []struct {
		name     string
		supplier func() (*Cache, error)
	}{
		{"cache_config", func() (*Cache, error) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer func() {
				cancel()
			}()
			cache, err0 := suite.cli.GetOrCreateCache(ctx, cacheName)
			if err0 != nil {
				return nil, err0
			}
			return cache.WithExpiryPolicy(1*time.Second, DurationZero, DurationZero), nil
		}},
		{"cache_decorator", func() (*Cache, error) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer func() {
				cancel()
			}()
			return suite.cli.CreateCacheWithConfiguration(ctx,
				CreateCacheConfiguration(cacheName, WithExpiryPolicy(1*time.Second, DurationZero, DurationZero)))
		}},
	}

	for _, fixture := range fixtures {
		suite.T().Run(fixture.name, func(t *testing.T) {
			ctx := context.Background()
			cache, err := fixture.supplier()
			require.Nil(suite.T(), err)
			defer func() {
				_ = suite.cli.DestroyCache(ctx, cacheName)
			}()

			err = cache.Put(ctx, "test", "test")
			require.Nil(suite.T(), err)
			<-time.After(1200 * time.Millisecond)
			contains, err := cache.ContainsKey(ctx, "test")
			require.Nil(suite.T(), err)
			require.False(suite.T(), contains)
		})
	}
}
