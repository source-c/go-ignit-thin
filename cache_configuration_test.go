package ignite

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"testing"
	"time"
)

type CacheConfigTestSuite struct {
	testing2.IgniteTestSuite
	cli *Client
}

func TestCacheConfigTestSuite(t *testing.T) {
	suite.Run(t, new(CacheConfigTestSuite))
}

func (suite *CacheConfigTestSuite) SetupSuite() {
	_, err := suite.StartIgnite()
	if err != nil {
		suite.T().Fatal("Failed to start ignite instance", err)
	}
	suite.cli, err = StartTestClient(context.Background())
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
}

func (suite *CacheConfigTestSuite) TearDownTest() {
	DestroyAllCaches(suite.T(), suite.cli)
}

func (suite *CacheConfigTestSuite) TearDownSuite() {
	_ = suite.cli.Close(context.Background())
	suite.KillAllGrids()
}

var testCacheCfg = CreateCacheConfiguration(cacheName,
	WithBackupsCount(2),
	WithCacheMode(PartitionedCacheMode),
	WithCacheAtomicityMode(TransactionalAtomicityMode),
	WithCopyOnRead(true),
	WithDataRegionName("default"),
	WithEagerTtl(false),
	WithCacheGroupName(cacheName+"-grp"),
	WithMaxConcurrentAsyncOperations(100),
	WithMaxQueryIteratorsCount(100),
	WithOnHeapCacheEnabled(true),
	WithPartitionLossPolicy(ReadWriteSafeLossPolicy),
	WithQueryDetailsMetricsSize(100),
	WithQueryParallelism(20),
	WithReadFromBackup(true),
	WithRebalanceMode(SyncRebalanceMode),
	WithRebalanceOrder(2),
	WithSqlEscapeAll(true),
	WithStatsEnabled(true),
	WithSqlIndexMaxInlineSize(200),
	WithSqlSchema("PUBLIC"),
	WithWriteSynchronizationMode(FullSyncSynchronizationMode),
	WithExpiryPolicy(1*time.Second, 2*time.Second, 3*time.Second),
	WithCacheKeyConfiguration("PersonId", "BUCKET_ID"),
	WithQueryEntity("PersonId", "Person", WithTableName("QUERY_CACHE"),
		WithKeyFieldName("KEY"),
		WithValueFieldName("VAL"),
		WithQueryField("ID", "java.lang.Long", WithKey()),
		WithQueryField("BUCKET_ID", "java.lang.Long", WithKey()),
		WithQueryField("KEY", "PersonId"),
		WithQueryField("VAL", "Person"),
		WithQueryField("NAME", "java.lang.String", WithNotNull(), WithDefaultValue("")),
		WithQueryField("SALARY", "java.math.BigDecimal",
			WithPrecision(10), WithScale(2), WithNotNull()),
		WithQueryField("PERSON.AGE", "java.lang.Short", WithNotNull(), WithDefaultValue(int32(2))),
		WithFieldAlias("PERSON.AGE", "PERSON_AGE"),
		WithIndex("NAME_IDX", WithIndexType(Sorted), WithInlineSize(200), WithIndexField(IndexField{Name: "NAME", Asc: true})),
		WithIndex("NAME_AGE_IDX", WithIndexField(IndexField{Name: "PERSON_AGE", Asc: true}), WithIndexField(IndexField{Name: "NAME", Asc: true})),
	),
)

func (suite *CacheConfigTestSuite) TestCacheConfig() {
	fixtures := []struct {
		name          string
		cacheCreation func() (*Cache, error)
	}{
		{
			"CreateWithConfiguration",
			func() (*Cache, error) {
				ctx := context.Background()
				return suite.cli.CreateCacheWithConfiguration(ctx, testCacheCfg)
			},
		},
		{
			"GetOrCreateWithConfiguration",
			func() (*Cache, error) {
				ctx := context.Background()
				_, err := suite.cli.GetOrCreateCacheWithConfiguration(ctx, testCacheCfg)
				require.Nil(suite.T(), err)
				return suite.cli.GetOrCreateCacheWithConfiguration(ctx, testCacheCfg)
			},
		},
	}
	for _, f := range fixtures {
		suite.T().Run(f.name, func(t *testing.T) {
			ctx := context.Background()
			cache, err := f.cacheCreation()
			defer func() {
				_ = suite.cli.DestroyCache(ctx, cacheName)
			}()
			require.Nil(suite.T(), err)
			readCcfg, err := cache.Configuration(ctx)
			require.Nil(suite.T(), err)
			requireEqualsCacheConfiguration(suite.T(), &testCacheCfg, &readCcfg)
		})
	}
}

func (suite *CacheConfigTestSuite) TestCopyCacheConfig() {
	cfg := testCacheCfg.Copy(WithCacheName("test-1"))
	require.Equal(suite.T(), "test-1", cfg.Name())
	cfg = cfg.Copy(WithCacheName(testCacheCfg.Name()))
	requireEqualsCacheConfiguration(suite.T(), &testCacheCfg, &cfg)

	ctx := context.Background()
	cache, err := suite.cli.CreateCacheWithConfiguration(ctx, cfg)
	require.Nil(suite.T(), err)
	readCfg, err := cache.Configuration(ctx)
	require.Nil(suite.T(), err)
	requireEqualsCacheConfiguration(suite.T(), &readCfg, &cfg)
}

func requireEqualsCacheConfiguration(t *testing.T, c1 *CacheConfiguration, c2 *CacheConfiguration) {
	if c1 == nil && c2 == nil {
		return
	}
	require.True(t, c1 != nil || c2 != nil)
	require.Equal(t, c1.Name(), c2.Name())
	require.Equal(t, c1.Backups(), c2.Backups())
	require.Equal(t, c1.CacheMode(), c2.CacheMode())
	require.Equal(t, c1.CacheAtomicityMode(), c2.CacheAtomicityMode())
	require.Equal(t, c1.IsCopyOnRead(), c2.IsCopyOnRead())
	require.Equal(t, c1.DataRegionName(), c2.DataRegionName())
	require.Equal(t, c1.IsEagerTtl(), c2.IsEagerTtl())
	require.Equal(t, c1.CacheGroupName(), c2.CacheGroupName())
	require.Equal(t, c1.MaxConcurrentAsyncOperations(), c2.MaxConcurrentAsyncOperations())
	require.Equal(t, c1.MaxQueryIteratorsCount(), c2.MaxQueryIteratorsCount())
	require.Equal(t, c1.IsOnHeapCacheEnabled(), c2.IsOnHeapCacheEnabled())
	require.Equal(t, c1.PartitionLossPolicy(), c2.PartitionLossPolicy())
	require.Equal(t, c1.QueryDetailsMetricsSize(), c2.QueryDetailsMetricsSize())
	require.Equal(t, c1.QueryParallelism(), c2.QueryParallelism())
	require.Equal(t, c1.IsReadFromBackup(), c2.IsReadFromBackup())
	require.Equal(t, c1.RebalanceMode(), c2.RebalanceMode())
	require.Equal(t, c1.RebalanceOrder(), c2.RebalanceOrder())
	require.Equal(t, c1.IsSqlEscapeAll(), c2.IsSqlEscapeAll())
	require.Equal(t, c1.IsStatsEnabled(), c2.IsStatsEnabled())
	require.Equal(t, c1.SqlIndexMaxInlineSize(), c2.SqlIndexMaxInlineSize())
	require.Equal(t, c1.SqlSchema(), c2.SqlSchema())
	require.Equal(t, c1.WriteSynchronizationMode(), c2.WriteSynchronizationMode())
	requireEqualsExpiryPolicy(t, c1.ExpiryPolicy(), c2.ExpiryPolicy())
	requireSliceEquals(t, c1.KeyConfiguration(), c2.KeyConfiguration(), requireEqualsKeyConfig)
	requireSliceEquals(t, c1.QueryEntities(), c2.QueryEntities(), requireEqualsQueryEntity)
}

func requireEqualsExpiryPolicy(t *testing.T, p1 *ExpiryPolicy, p2 *ExpiryPolicy) {
	if p1 == nil && p2 == nil {
		return
	}
	require.True(t, p1 != nil || p2 != nil)
	require.Equal(t, p1.Update(), p2.Update())
	require.Equal(t, p1.Access(), p2.Access())
	require.Equal(t, p1.Creation(), p2.Creation())
}

func requireEqualsKeyConfig(t *testing.T, k1 *CacheKeyConfig, k2 *CacheKeyConfig) {
	require.Equal(t, k1.TypeName(), k2.TypeName())
	require.Equal(t, k1.AffinityKeyFieldName(), k2.AffinityKeyFieldName())
}

func requireEqualsQueryEntity(t *testing.T, e1 *QueryEntity, e2 *QueryEntity) {
	require.Equal(t, e1.KeyType(), e2.KeyType())
	require.Equal(t, e1.ValueType(), e2.ValueType())
	require.Equal(t, e1.TableName(), e2.TableName())
	require.Equal(t, e1.KeyFieldName(), e2.KeyFieldName())
	require.Equal(t, e1.ValueFieldName(), e2.ValueFieldName())
	requireSliceEquals(t, e1.Fields(), e2.Fields(), requireEqualsQueryField)
	requireSliceEquals(t, e1.Indexes(), e2.Indexes(), requireEqualsQueryIndex)

	var aliasMap1 map[string]string
	var aliasMap2 map[string]string
	if len(e1.Aliases()) > len(e2.Aliases()) {
		aliasMap1 = e1.Aliases()
		aliasMap2 = e2.Aliases()
	} else {
		aliasMap1 = e2.Aliases()
		aliasMap2 = e1.Aliases()
	}
	for name, alias1 := range aliasMap1 {
		alias2, ok := aliasMap2[name]
		if !ok {
			continue
		}
		require.Equal(t, alias1, alias2, "e1 aliases: %v, e2 aliases: %v", e1.Aliases(), e2.Aliases())
	}
}

func requireEqualsQueryField(t *testing.T, f1 *QueryField, f2 *QueryField) {
	require.Equal(t, f1.Name(), f2.Name())
	require.Equal(t, f1.TypeName(), f2.TypeName())
	require.Equal(t, f1.IsKey(), f2.IsKey())
	require.Equal(t, f1.IsNotNull(), f2.IsNotNull())
	require.Equal(t, f1.Precision(), f2.Precision())
	require.Equal(t, f1.DefaultValue(), f2.DefaultValue())
	require.Equal(t, f1.Scale(), f2.Scale())
}

func requireEqualsQueryIndex(t *testing.T, q1 *QueryIndex, q2 *QueryIndex) {
	require.Equal(t, q1.Name(), q2.Name())
	require.Equal(t, q1.Type(), q2.Type())
	require.Equal(t, q1.InlineSize(), q2.InlineSize())
	requireSliceEquals(t, q1.Fields(), q2.Fields(), func(t *testing.T, f1 *IndexField, f2 *IndexField) {
		require.Equal(t, f1.Name, f2.Name)
		require.Equal(t, f1.Asc, f2.Asc)
	})
}

func requireSliceEquals[T any](t *testing.T, s1 []T, s2 []T, eq func(t *testing.T, el1 *T, el2 *T)) {
	require.Equal(t, len(s1), len(s2))
	for i := 0; i < len(s1); i++ {
		eq(t, &s1[i], &s2[i])
	}
}
