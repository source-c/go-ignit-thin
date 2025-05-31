package ignite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"github.com/source-c/go-ignit-thin/logger"
	"log"
	"os"
	"testing"
	"time"
)

const (
	cacheName      = "new-cache"
	defaultAddress = "localhost"
)

type BasicTestSuite struct {
	testing2.IgniteTestSuite
}

func StartTestClient(ctx context.Context, opts ...ClientConfigurationOption) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dummyCfg := clientConfiguration{}
	for _, opt := range opts {
		_ = opt(&dummyCfg)
	}
	if dummyCfg.addressesSupplier == nil {
		opts = append(opts, WithAddresses(defaultAddress))
	}
	if dummyCfg.logger == nil {
		sink, _ := logger.NewSink(log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds), logger.DebugLevel) // level is ok, error can be ignored.
		opts = append(opts, WithLoggingSink(sink))
	}
	return Start(ctx, opts...)
}

func DestroyAllCaches(t *testing.T, cli *Client) {
	ctx := context.Background()
	caches, err := cli.CacheNames(ctx)
	require.Nil(t, err)
	for _, cache := range caches {
		err = cli.DestroyCache(ctx, cache)
		require.Nil(t, err)
	}
}

func TestBasicTestSuite(t *testing.T) {
	suite.Run(t, new(BasicTestSuite))
}

func (suite *BasicTestSuite) SetupSuite() {
	_, err := suite.StartIgnite()
	if err != nil {
		suite.T().Fatal("Failed to start ignite instance", err)
	}
}

func (suite *BasicTestSuite) TearDownSuite() {
	suite.KillAllGrids()
}

func (suite *BasicTestSuite) TearDownTest() {
	cli, err := StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
	defer func() {
		_ = cli.Close(context.Background())
	}()
	DestroyAllCaches(suite.T(), cli)
}

func (suite *BasicTestSuite) TestCorrectAddresses() {
	cli, err := Start(context.Background())
	require.Nil(suite.T(), cli)
	require.Error(suite.T(), err, "address supplier is nil")

	cli, err = Start(context.Background(), WithAddresses())
	require.Nil(suite.T(), cli)
	require.Error(suite.T(), err, "addresses are empty")

	cli, err = Start(context.Background(), WithAddressSupplier(func(_ context.Context) ([]string, error) {
		return []string{}, nil
	}))
	require.Nil(suite.T(), cli)
	require.Error(suite.T(), err, "addresses are empty")
}

func (suite *BasicTestSuite) TestSettingProtocol() {
	for _, protoVer := range []ProtocolVersion{{0, 9, 0}, {1, 8, 0}, {1, 9, 0}} {
		_, err := StartTestClient(context.Background(), WithProtocolContext(protoVer))
		require.Error(suite.T(), err, fmt.Sprintf("version %s is not supported", protoVer.String()))
	}

	correctVer := ProtocolVersion{1, 0, 0}
	cli, err := StartTestClient(context.Background(), WithProtocolContext(correctVer))
	defer func() {
		_ = cli.Close(context.Background())
	}()
	require.Nil(suite.T(), err)
	ver, err := cli.Version()
	require.Nil(suite.T(), err)
	require.Equal(suite.T(), correctVer.String(), ver)
}

func (suite *BasicTestSuite) TestCacheSize() {
	cli, err := StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
	defer func() {
		_ = cli.Close(context.Background())
	}()

	var cache *Cache
	ctx := context.Background()
	cache, err = cli.GetOrCreateCache(ctx, cacheName)
	if err != nil {
		suite.T().Fatal("failed to obtain cache", err)
	}

	var cacheSz uint64
	cacheSz, err = cache.Size(ctx)
	require.Nil(suite.T(), err)
	require.Equal(suite.T(), uint64(0), cacheSz)
	err = cache.Put(ctx, "test", "test")
	require.Nil(suite.T(), err)
	val, err := cache.Get(ctx, "test")
	require.Nil(suite.T(), err)
	require.Equal(suite.T(), "test", val)
	cacheSz, err = cache.Size(ctx)
	require.Nil(suite.T(), err)
	require.Equal(suite.T(), uint64(1), cacheSz)
}

func (suite *BasicTestSuite) TestCacheNames() {
	cli, err := StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
	defer func() {
		_ = cli.Close(context.Background())
	}()

	var names []string
	ctx := context.Background()
	names, err = cli.CacheNames(ctx)

	require.Nil(suite.T(), err)
	require.Equal(suite.T(), 0, len(names))

	var cache *Cache
	cache, err = cli.CreateCache(ctx, cacheName)

	require.Nil(suite.T(), err)
	require.NotNil(suite.T(), cache)
	require.Equal(suite.T(), cacheName, cache.Name())

	var cli0 *Client
	cli0, err = StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}

	defer func() {
		_ = cli0.Close(context.Background())
	}()

	cache, err = cli0.CreateCache(ctx, cacheName)

	require.Error(suite.T(), err)
	require.Nil(suite.T(), cache)

	cache, err = cli0.GetOrCreateCache(ctx, cacheName)

	require.Nil(suite.T(), err)
	require.NotNil(suite.T(), cache)

	names, err = cli.CacheNames(ctx)

	require.Nil(suite.T(), err)
	require.Equal(suite.T(), 1, len(names))
	require.Equal(suite.T(), cacheName, (names)[0])
}

func (suite *BasicTestSuite) TestDestroyCache() {
	cli, err := StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
	require.NotNil(suite.T(), cli)
	defer func() {
		_ = cli.Close(context.Background())
	}()

	ctx := context.Background()
	namesBefore, err := cli.CacheNames(ctx)
	if err != nil {
		suite.T().Fatal(err)
	}

	toDestroy := "to-destroy"

	var cache *Cache
	cache, err = cli.CreateCache(ctx, toDestroy)
	if err != nil {
		suite.T().Fatal(err)
	}

	require.NotNil(suite.T(), cache)
	require.Equal(suite.T(), toDestroy, cache.Name())

	err = cli.DestroyCache(ctx, toDestroy)
	if err != nil {
		suite.T().Fatal(err)
	}

	var names []string
	names, err = cli.CacheNames(ctx)
	if err != nil {
		suite.T().Fatal(err)
	}

	require.Equal(suite.T(), len(namesBefore), len(names))
}

func (suite *BasicTestSuite) TestTimeouts() {
	cli, err := StartTestClient(context.Background(), WithRequestTimeout(10*time.Second))
	require.Nil(suite.T(), err)
	defer func() {
		_ = cli.Close(context.Background())
	}()

	cache, err := cli.GetOrCreateCache(context.Background(), cacheName)
	require.Nil(suite.T(), err)

	// Put large entry that causes cancelling
	largeEntry := testing2.MakeByteArrayPayload(10 * 1024 * 1024)

	// Should cancel because timeout is too small
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = cache.Put(ctx, "key", largeEntry)
	require.Error(suite.T(), err)

	// Retry with default large timeout
	err = cache.Put(context.Background(), "key", largeEntry)
	require.Nil(suite.T(), err)
}
