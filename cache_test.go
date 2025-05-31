package ignite

import (
	"context"
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"math"
	"sort"
	"testing"
	"time"
)

type CacheTestSuite struct {
	testing2.IgniteTestSuite
	client *Client
	cache  *Cache
}

func TestCacheTestSuite(t *testing.T) {
	suite.Run(t, new(CacheTestSuite))
}

func (suite *CacheTestSuite) SetupSuite() {
	_, err := suite.StartIgnite()
	if err != nil {
		suite.T().Fatal("Failed to start ignite instance", err)
	}
	suite.client, err = StartTestClient(context.Background(), WithAddresses(defaultAddress))
	if err != nil {
		suite.T().Fatal("failed to start client", err)
	}
	suite.cache, err = suite.client.GetOrCreateCache(context.Background(), "test")
	if err != nil {
		suite.T().Fatal("failed to create test cache", err)
	}
}

func (suite *CacheTestSuite) TearDownTest() {
	err := suite.cache.ClearAll(context.Background())
	if err != nil {
		suite.T().Fatal("failed to clear cache", err)
	}
	sz, err := suite.cache.Size(context.Background())
	if err != nil {
		suite.T().Fatal("failed to clear cache", err)
	}
	require.Equal(suite.T(), uint64(0), sz, fmt.Sprintf("cache is not empty: size=%d", sz))
}

func (suite *CacheTestSuite) TearDownSuite() {
	suite.KillAllGrids()
	if suite.client != nil {
		_ = suite.client.Close(context.Background())
		suite.client = nil
	}
}

func (suite *CacheTestSuite) TestPutGetSingle() {
	timestamp := time.Now().Truncate(0) // remove monotonic part.
	values := []interface{}{
		true, false, int8(-10), int16(-1024), int32(-1 * 1 << 30), int64(-1 * 1 << 40), math.Float32frombits(0xfd0f),
		math.Float64frombits(0xffdd00ef), testing2.MakeRandomString(100), testing2.MakeByteArrayPayload(1024),
		uuid.New(), NewTime(timestamp), NewDate(timestamp), timestamp,
	}
	fixtures := make([]struct {
		key interface{}
		val interface{}
	}, 0)
	for _, k := range values {
		for _, v := range values {
			fixtures = append(fixtures, struct {
				key interface{}
				val interface{}
			}{key: k, val: v})
		}
	}
	for _, f := range fixtures {
		suite.T().Run(fmt.Sprintf("put/get single key=%v,val=%v", f.key, f.val), func(t *testing.T) {
			suite.putGetSingleTest(t, f.key, f.val, f.val)
		})
	}
}

func (suite *CacheTestSuite) TestPutGetDecimals() {
	decimals := []struct {
		coeff string
		exp   int32
	}{
		{"FFDEADBEEFDEADBEEFDEADBEEF", 10},
		{"-FFDEADBEEFDEADBEEFDEADBEEF", 10},
		{"FFDEADBEEFDEADBEEFDEADBEEF", -10},
		{"-FFDEADBEEFDEADBEEFDEADBEEF", -10},
		{"7FDEADBEEFDEADBEEFDEADBEEF", 10},
		{"7FDEADBEEFDEADBEEFDEADBEEF", -10},
		{"-7FDEADBEEFDEADBEEFDEADBEEF", 10},
		{"-7FDEADBEEFDEADBEEFDEADBEEF", -10},
		{"80DEADBEEFDEADBEEFDEADBEEF", 10},
		{"-80DEADBEEFDEADBEEFDEADBEEF", 10},
		{"80DEADBEEFDEADBEEFDEADBEEF", -10},
		{"-80DEADBEEFDEADBEEFDEADBEEF", -10},
		{"0", 0},
		{"0", -10},
		{"0", 10},
		{"-80", 0},
		{"80", 0},
		{"7F", 0},
		{"-7F", 0},
	}

	for _, dec := range decimals {
		coeff := new(apd.BigInt)
		coeff.SetString(dec.coeff, 16)
		val := apd.NewWithBigInt(coeff, dec.exp)
		suite.T().Run(fmt.Sprintf("put/get decimal key=%v,val=%v", val, val), func(t *testing.T) {
			suite.putGetSingleTest(t, val, val, val)
		})
	}
}

func (suite *CacheTestSuite) TestPutGetSingleUnsignedIntegers() {
	fixtures := []struct {
		key    interface{}
		val    interface{}
		retVal interface{}
	}{
		{uint8(10), uint8(10), int8(10)},
		{uint16(0xFFF), uint16(0xFFF), uint16(0xFFF)}, // exception for java char (the only unsigned type in java)
		{uint32(0xFFFFF), uint32(0xFFFFF), int32(0xFFFFF)},
		{uint64(0xFFFFFFFFF), uint64(0xFFFFFFFFF), int64(0xFFFFFFFFF)},
	}
	for _, f := range fixtures {
		suite.T().Run(fmt.Sprintf("put/get single key=%v,val=%v", f.key, f.val), func(t *testing.T) {
			suite.putGetSingleTest(t, f.key, f.val, f.retVal)
		})
	}
}

func (suite *CacheTestSuite) TestComplexOperations() {
	key := "key"
	oldVal := "val"
	newVal := "newVal"

	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			"GetAndPut", func(t *testing.T) {
				res, err := suite.cache.GetAndPut(context.Background(), key, oldVal)
				require.Nil(t, err)
				require.Nil(t, res)

				res, err = suite.cache.GetAndPut(context.Background(), key, newVal)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, newVal, res)
			},
		},
		{
			"GetAndReplace", func(t *testing.T) {
				res, err := suite.cache.GetAndReplace(context.Background(), key, newVal)
				require.Nil(t, err)
				require.Nil(t, res)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Nil(t, res)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				res, err = suite.cache.GetAndReplace(context.Background(), key, newVal)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, newVal, res)
			},
		},
		{
			"PutIfAbsent", func(t *testing.T) {
				ok, err := suite.cache.PutIfAbsent(context.Background(), key, oldVal)
				require.Nil(t, err)
				require.True(t, ok)

				ok, err = suite.cache.PutIfAbsent(context.Background(), key, newVal)
				require.Nil(t, err)
				require.False(t, ok)

				res, err := suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)
			},
		},
		{
			"GetAndPutIfAbsent", func(t *testing.T) {
				res, err := suite.cache.GetAndPutIfAbsent(context.Background(), key, oldVal)
				require.Nil(t, err)
				require.Nil(t, res)

				res, err = suite.cache.GetAndPutIfAbsent(context.Background(), key, newVal)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)
			},
		},
		{
			"GetAndRemove", func(t *testing.T) {
				res, err := suite.cache.GetAndRemove(context.Background(), key)
				require.Nil(t, err)
				require.Nil(t, res)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				res, err = suite.cache.GetAndRemove(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Nil(t, res)
			},
		},
		{
			"Replace/ReplaceIfEquals", func(t *testing.T) {
				ok, err := suite.cache.Replace(context.Background(), key, oldVal)
				require.Nil(t, err)
				require.False(t, ok)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				ok, err = suite.cache.Replace(context.Background(), key, newVal)
				require.Nil(t, err)
				require.True(t, ok)

				res, err := suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, newVal, res)

				ok, err = suite.cache.ReplaceIfEquals(context.Background(), key, oldVal, newVal)
				require.Nil(t, err)
				require.False(t, ok)

				ok, err = suite.cache.ReplaceIfEquals(context.Background(), key, newVal, oldVal)
				require.Nil(t, err)
				require.True(t, ok)

				res, err = suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)
			},
		},
		{
			"Remove/RemoveIfEquals", func(t *testing.T) {
				ok, err := suite.cache.Remove(context.Background(), key)
				require.Nil(t, err)
				require.False(t, ok)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				ok, err = suite.cache.RemoveIfEquals(context.Background(), key, newVal)
				require.Nil(t, err)
				require.False(t, ok)

				res, err := suite.cache.Get(context.Background(), key)
				require.Nil(t, err)
				require.Equal(t, oldVal, res)

				ok, err = suite.cache.RemoveIfEquals(context.Background(), key, oldVal)
				require.Nil(t, err)
				require.True(t, ok)

				ok, err = suite.cache.ContainsKey(context.Background(), key)
				require.Nil(t, err)
				require.False(t, ok)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				ok, err = suite.cache.Remove(context.Background(), key)
				require.Nil(t, err)
				require.True(t, ok)

				ok, err = suite.cache.ContainsKey(context.Background(), key)
				require.Nil(t, err)
				require.False(t, ok)
			},
		},
		{
			"Clear", func(t *testing.T) {
				err := suite.cache.Clear(context.Background(), key)
				require.Nil(t, err)

				err = suite.cache.Put(context.Background(), key, oldVal)
				require.Nil(t, err)

				err = suite.cache.Clear(context.Background(), key)
				require.Nil(t, err)

				ok, err := suite.cache.ContainsKey(context.Background(), key)
				require.Nil(t, err)
				require.False(t, ok)
			},
		},
	}

	for _, test := range tests {
		suite.T().Run(test.name, func(t *testing.T) {
			sz, err := suite.cache.Size(context.Background())
			require.Nil(t, err)
			require.Equal(t, uint64(0), sz, "cache must be empty")
			test.test(t)
			defer func() {
				_ = suite.cache.ClearAll(context.Background())
			}()
		})
	}
}

func (suite *CacheTestSuite) TestBulkOperations() {
	const entrySz = 1000
	keys := make([]interface{}, 0)
	values := make([]interface{}, 0)
	kv := make([]KeyValue, 0)
	kvSorter := func(data []KeyValue) {
		sort.Slice(data, func(i, j int) bool {
			return data[i].Key.(string) > data[j].Key.(string)
		})
	}
	dataSorter := func(data []interface{}) {
		sort.Slice(data, func(i, j int) bool {
			return data[i].(string) > data[j].(string)
		})
	}

	for i := 0; i < entrySz; i++ {
		str := testing2.MakeRandomString(100)
		keys = append(keys, str)
		values = append(values, str)
		kv = append(kv, KeyValue{str, str})
	}
	dataSorter(keys)
	dataSorter(values)
	kvSorter(kv)

	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			"GetAll", func(t *testing.T) {
				actualValues, err := suite.cache.GetAll(context.Background(), keys...)
				require.Nil(t, err)

				kvSorter(actualValues)
				require.Equal(t, kv, actualValues)
			},
		},
		{
			"ContainsKeys", func(t *testing.T) {
				ok, err := suite.cache.ContainsKeys(context.Background(), keys...)
				require.Nil(t, err)
				require.True(t, ok)

				err = suite.cache.ClearKeys(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)

				ok, err = suite.cache.ContainsKeys(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)
				require.False(t, ok)

				ok, err = suite.cache.ContainsKeys(context.Background(), keys[(entrySz>>2):]...)
				require.Nil(t, err)
				require.True(t, ok)
			},
		},
		{
			"ClearKeys/ClearAll", func(t *testing.T) {
				err := suite.cache.ClearKeys(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)

				actualValues, err := suite.cache.GetAll(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)
				require.Equal(t, 0, len(actualValues))

				actualValues, err = suite.cache.GetAll(context.Background(), keys[(entrySz>>2):]...)
				require.Nil(t, err)
				kvSorter(actualValues)
				require.Equal(t, kv[(entrySz>>2):], actualValues)

				err = suite.cache.ClearAll(context.Background())
				require.Nil(t, err)

				sz, err := suite.cache.Size(context.Background())
				require.Nil(t, err)
				require.Equal(t, uint64(0), sz, "cache must be empty")
			},
		},
		{
			"RemoveKeys/RemoveAll", func(t *testing.T) {
				err := suite.cache.RemoveKeys(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)

				actualValues, err := suite.cache.GetAll(context.Background(), keys[:(entrySz>>2)]...)
				require.Nil(t, err)
				require.Equal(t, 0, len(actualValues))

				actualValues, err = suite.cache.GetAll(context.Background(), keys[(entrySz>>2):]...)
				require.Nil(t, err)
				kvSorter(actualValues)
				require.Equal(t, kv[(entrySz>>2):], actualValues)

				err = suite.cache.RemoveAll(context.Background())
				require.Nil(t, err)

				sz, err := suite.cache.Size(context.Background())
				require.Nil(t, err)
				require.Equal(t, uint64(0), sz, "cache must be empty")
			},
		},
	}

	for _, test := range tests {
		suite.T().Run(test.name, func(t *testing.T) {
			sz, err := suite.cache.Size(context.Background())
			require.Nil(t, err)
			require.Equal(t, uint64(0), sz, "cache must be empty")

			err = suite.cache.PutAll(context.Background(), kv...)
			require.Nil(t, err)

			sz, err = suite.cache.Size(context.Background())
			require.Nil(t, err)
			require.Equal(t, uint64(len(keys)), sz, "cache must be empty")

			test.test(t)
			defer func() {
				_ = suite.cache.ClearAll(context.Background())
			}()
		})
	}
}

func (suite *CacheTestSuite) putGetSingleTest(t *testing.T, key interface{}, val interface{}, retVal interface{}) {
	defer func() {
		_ = suite.cache.ClearAll(context.Background())
	}()
	sz, err := suite.cache.Size(context.Background())
	require.Nil(t, err)
	require.Equal(t, uint64(0), sz, "cache must be empty")

	ok, err := suite.cache.ContainsKey(context.Background(), key)
	require.Nil(t, err)
	require.False(t, ok)

	err = suite.cache.Put(context.Background(), key, val)
	require.Nil(t, err, "failed to put value", err)

	ok, err = suite.cache.ContainsKey(context.Background(), key)
	require.Nil(t, err)
	require.True(t, ok)

	actualVal, err := suite.cache.Get(context.Background(), key)
	require.Nil(t, err, "failed to get value", err)
	require.Equal(t, retVal, actualVal)
}
