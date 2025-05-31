package ignite

import (
	"context"
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type BinaryObjectTestSuite struct {
	testing2.IgniteTestSuite
	cli *Client
}

func TestBinaryObjectsCacheOperations(t *testing.T) {
	suite.Run(t, new(BinaryObjectTestSuite))
}

func (suite *BinaryObjectTestSuite) TestBasic() {
	suite.runTests([]struct {
		f         func(*testing.T, *Client, *Cache)
		isIndexed bool
	}{
		{testPutGetAllBinaryObject, true},
		{testNestedBinaryObject, false},
		{testLargeBinaryObject, false},
		{testDifferentFieldTypes, false},
		{testUnregisteredTypes, false},
	}...)
}

func (suite *BinaryObjectTestSuite) TestMergeMetadata() {
	suite.runTests(struct {
		f         func(*testing.T, *Client, *Cache)
		isIndexed bool
	}{testMergeMetadata_WithClearRegistry, false})
	suite.runTests(struct {
		f         func(*testing.T, *Client, *Cache)
		isIndexed bool
	}{testMergeMetadata_WithoutClearRegistry, false})
}

func testPutGetAllBinaryObject(t *testing.T, cli *Client, cache *Cache) {
	keyValues := make([]KeyValue, 0)
	keyValuesMap := make(map[int]KeyValue)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key, err := cli.CreateBinaryObject(ctx, "PERSON_KEY",
			WithField("id", i),
			WithField("bucket_id", i%2),
			WithAffinityKeyName("bucket_id"),
		)
		require.NoError(t, err)
		value, err := cli.CreateBinaryObject(ctx, "PERSON",
			WithField("name", fmt.Sprintf("name_%d", i)),
			WithField("age", int16(10)),
		)
		require.NoError(t, err)
		keyValues = append(keyValues, KeyValue{key, value})
		keyValuesMap[i] = KeyValue{key, value}
	}
	err := cache.PutAll(ctx, keyValues...)
	require.NoError(t, err)
	cfg, err := cache.Configuration(ctx)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	sz, err := cache.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), sz)

	keys := make([]interface{}, 0)
	for i := 0; i < 1000; i++ {
		keys = append(keys, keyValues[i].Key)
	}

	cli.marsh.clearRegistry() // Test retrieving binary info from server

	keyValues0, err := cache.GetAll(ctx, keys...)
	require.NoError(t, err)
	require.Equal(t, 1000, len(keyValues0))

	for _, kv0 := range keyValues0 {
		key0 := kv0.Key.(BinaryObject)
		value0 := kv0.Value.(BinaryObject)
		var id0 int64
		err := NewFieldGetter[int64](key0, "id").Get(ctx, &id0)
		require.NoError(t, err)

		kv, ok := keyValuesMap[int(id0)]
		require.True(t, ok)
		key := kv.Key.(BinaryObject)
		value := kv.Value.(BinaryObject)

		RequireBinaryObjectsEqual(t, key0, key)
		RequireBinaryObjectsEqual(t, value0, value)
	}
}

func testLargeBinaryObject(t *testing.T, cli *Client, cache *Cache) {
	for _, sz := range []int{0, 1024, 10 * 1024, 100 * 1024} {
		t.Run(fmt.Sprintf("%s-%d", t.Name(), sz), func(t *testing.T) {
			var val BinaryObject
			var err error
			ctx := context.Background()
			if sz != 0 {
				val, err = cli.CreateBinaryObject(ctx, "BLOB",
					WithField("data0", testing2.MakeByteArrayPayload(sz)),
					WithField("data1", testing2.MakeByteArrayPayload(sz)),
				)
			} else {
				val, err = cli.CreateBinaryObject(ctx, "BLOB", WithNullField("data0", ByteArrayType))
			}
			require.NoError(t, err)
			err = cache.Put(ctx, "blob", val)
			require.NoError(t, err)
			val0, err := cache.Get(ctx, "blob")
			require.NoError(t, err)
			RequireBinaryObjectsEqual(t, val, val0.(BinaryObject))
		})
	}
}

func testNestedBinaryObject(t *testing.T, cli *Client, cache *Cache) {
	ctx := context.Background()
	inner, err := cli.CreateBinaryObject(ctx, "INNER", WithField("id", 10))
	require.NoError(t, err)
	outer, err := cli.CreateBinaryObject(ctx, "OUTER", WithField("id", 10), WithField("obj", inner))
	require.NoError(t, err)
	err = cache.Put(ctx, "outer", outer)
	require.NoError(t, err)
	cli.marsh.clearRegistry()

	val, err := cache.Get(ctx, "outer")
	require.NoError(t, err)

	outer1 := val.(BinaryObject)
	outer1Type, err := outer1.Type(ctx)
	require.NoError(t, err)
	require.Equal(t, "OUTER", outer1Type.TypeName())
	require.Equal(t, []string{"id", "obj"}, outer1Type.Fields())

	val, err = outer1.Field(ctx, "obj")
	require.NoError(t, err)

	inner1 := val.(BinaryObject)
	inner1Type, err := inner1.Type(ctx)
	require.NoError(t, err)
	require.Equal(t, "INNER", inner1Type.TypeName())
	require.Equal(t, []string{"id"}, inner1Type.Fields())

	innerFldVal, err := inner1.Field(ctx, "id")
	require.NoError(t, err)
	require.Equal(t, int64(10), innerFldVal)
}

func testDifferentFieldTypes(t *testing.T, cli *Client, cache *Cache) {
	timestamp := time.Now().Truncate(0) // remove monotonic part.
	fields := []struct {
		name  string
		value interface{}
	}{
		{"bool", true}, {"byte", byte(10)}, {"int8", int8(10)},
		{"char", uint16(10)}, {"short", int16(10)}, {"int", int32(10)},
		{"uint", uint32(10)}, {"long", int64(10)}, {"ulong", uint64(10)},
		{"goInt", 10}, {"goUInt", uint(10)},
		{"float", float32(10.0)}, {"double", float64(10.0)},
		{"boolArray", createRandPrimitiveArray(func() bool { return rand.Intn(3) != 0 })},
		{"byteArray", createRandPrimitiveArray(func() byte { return byte(rand.Intn(1<<8 - 1)) })},
		{"signedByteArray", createRandPrimitiveArray(func() int8 { return int8(rand.Intn(1<<8 - 1)) })},
		{"shortArray", createRandPrimitiveArray(func() int16 { return int16(rand.Intn(1<<16 - 1)) })},
		{"charArray", createRandPrimitiveArray(func() uint16 { return uint16(rand.Intn(1<<16 - 1)) })},
		{"uintArray", createRandPrimitiveArray(func() uint { return uint(rand.Uint64()) })},
		{"intArray", createRandPrimitiveArray(func() int { return int(rand.Int63()) })},
		{"uint32Array", createRandPrimitiveArray(func() uint32 { return rand.Uint32() })},
		{"int32Array", createRandPrimitiveArray(func() int32 { return rand.Int31() })},
		{"uint64Array", createRandPrimitiveArray(func() uint64 { return rand.Uint64() })},
		{"int64Array", createRandPrimitiveArray(func() int64 { return rand.Int63() })},
		{"floatArray", createRandPrimitiveArray(func() float32 { return rand.Float32() })},
		{"doubleArray", createRandPrimitiveArray(func() float64 { return rand.Float64() })},
		{"string", "test"}, {"uuid", uuid.New()}, {"time", NewTime(timestamp)},
		{"date", NewDate(timestamp)}, {"timestamp", timestamp},
		{"decimal", apd.New(100500, -3)},
		{"stringPArray", createPSlice("test1", "test2", "test3")},
		{"stringArray", []string{"test1", "test2", "test3"}},
		{"uuidPArray", createPSlice(uuid.New(), uuid.New(), uuid.New())},
		{"uuidArray", []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}},
		{"timePArray", createPSlice(NewTime(timestamp), NewTime(timestamp.Add(time.Minute*10)), NewTime(timestamp.Add(time.Minute*20)))},
		{"timeArray", []Time{NewTime(timestamp), NewTime(timestamp.Add(time.Minute * 10)), NewTime(timestamp.Add(time.Minute * 20))}},
		{"datePArray", createPSlice(NewDate(timestamp), NewDate(timestamp.Add(time.Minute*10)), NewDate(timestamp.Add(time.Minute*20)))},
		{"dateArray", []Date{NewDate(timestamp), NewDate(timestamp.Add(time.Minute * 10)), NewDate(timestamp.Add(time.Minute * 20))}},
		{"timeStampPArray", createPSlice(timestamp, timestamp.Add(time.Minute*10), timestamp.Add(time.Minute*20))},
		{"timeStampArray", []time.Time{timestamp, timestamp.Add(time.Minute * 10), timestamp.Add(time.Minute * 20)}},
		{"decimalPArray", []*apd.Decimal{apd.New(100500, -3), apd.New(0, 3), apd.New(31415926, 7)}},
		{"decimalArray", fromPSlice([]*apd.Decimal{apd.New(100500, -3), apd.New(0, 3), apd.New(31415926, 7)})},
		{"binaryObject", createTestBinaryObject(t, cli, 100500)},
		{"singletonList", NewSingletonList("test")},
		{"objectArray", []BinaryObject{createTestBinaryObject(t, cli, 10), createTestBinaryObject(t, cli, 20), nil}},
		{"objectArrayMixed", []interface{}{createTestBinaryObject(t, cli, 10), "test", nil}},
		{"arrayList", NewArrayList("test", "test")},
		{"arrayListMixed", NewArrayList([]interface{}{createTestBinaryObject(t, cli, 10), nil, "test"}...)},
		{"linkedList", NewLinkedList("test", "test")},
		{"linkedListMixed", NewLinkedList([]interface{}{createTestBinaryObject(t, cli, 10), nil, "test"}...)},
		{"hashSet", NewHashSet("test1", "test2")},
		{"hashSetMixed", NewHashSet([]interface{}{createTestBinaryObject(t, cli, 10), nil, "test"}...)},
		{"linkedHashSet", NewHashSet("test1", "test2")},
		{"linkedHashSetMixed", NewHashSet([]interface{}{createTestBinaryObject(t, cli, 10), nil, "test"}...)},
		{"hashMap", ToHashMap(map[string]int32{"test1": 1, "test2": 2})},
		{"hashMapMixed", NewHashMap([]KeyValue{{"test1", createTestBinaryObject(t, cli, 10)}, {int32(10), "test"}}...)},
		{"linkedHashMap", ToLinkedHashMap(map[string]int32{"test1": 1, "test2": 2})},
		{"linkedHashMapMixed", NewLinkedHashMap([]KeyValue{{"test1", createTestBinaryObject(t, cli, 10)}, {int32(10), "test"}}...)},
		{"hashMapGo", map[string]BinaryObject{"test1": createTestBinaryObject(t, cli, 10), "test2": createTestBinaryObject(t, cli, 20)}},
	}
	ctx := context.Background()
	opts := make([]func(*binaryObjectOptions), 0)
	for _, field := range fields {
		opts = append(opts, WithField(field.name, field.value))
	}
	obj, err := cli.CreateBinaryObject(ctx, "MANYFIELDS", opts...)
	require.NoError(t, err)
	for _, field := range fields {
		val, err := obj.Field(ctx, field.name)
		require.NoError(t, err, field.name)
		t.Logf("testing field %s: expected=%v, actual=%v", field.name, field.value, val)
		RequireIgniteTypesEqual(t, field.value, val)
	}

	err = cache.Put(ctx, "key", obj)
	require.NoError(t, err)

	cli.marsh.clearRegistry()

	obj1, err := cache.Get(ctx, "key")
	require.NoError(t, err)
	RequireBinaryObjectsEqual(t, obj, obj1.(BinaryObject))
}

func testMergeMetadata_WithClearRegistry(t *testing.T, cli *Client, cache *Cache) {
	testMergeMetadata(t, cli, cache, true)
}

func testMergeMetadata_WithoutClearRegistry(t *testing.T, cli *Client, cache *Cache) {
	testMergeMetadata(t, cli, cache, false)
}

func testUnregisteredTypes(t *testing.T, cli *Client, cache *Cache) {
	ctx := context.Background()
	typeName := "UNREGISTERED"
	expTypId := cli.marsh.binaryIdMapper().TypeId(typeName)
	exp, err := cli.CreateBinaryObject(ctx, typeName, WithField("id", uuid.New()), func(options *binaryObjectOptions) {
		options.isRegistered = false
	})
	require.Equal(t, int32(unregisteredType), exp.(*binaryObjectImpl).getRawTypeId())
	require.NoError(t, err)
	typ, err := exp.Type(ctx)
	require.NoError(t, err)
	require.Equal(t, typeName, typ.TypeName())
	require.Equal(t, expTypId, typ.TypeId())

	err = cache.Put(ctx, "key", exp)
	require.NoError(t, err)
	val, err := cache.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, int32(unregisteredType), val.(*binaryObjectImpl).getRawTypeId())
	RequireBinaryObjectsEqual(t, exp, val.(BinaryObject))
}

func testMergeMetadata(t *testing.T, cli *Client, _ *Cache, clearRegistry bool) {
	ctx := context.Background()
	objChecker := func(obj BinaryObject, typeName string, fields []string, fieldVals ...struct {
		name  string
		value interface{}
	}) {
		objT, err := obj.Type(ctx)
		require.NoError(t, err)
		require.Equal(t, typeName, objT.TypeName())
		require.Equal(t, fields, objT.Fields())
		for _, fv := range fieldVals {
			val, err := obj.Field(ctx, fv.name)
			require.NoError(t, err)
			require.Equal(t, fv.value, val)
		}
	}
	objSchemaSizeChecker := func(obj BinaryObject, expectedSz int) {
		objT, err := obj.Type(ctx)
		require.NoError(t, err)
		meta, err := cli.marsh.getMetadata(ctx, objT.TypeId())
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.Equal(t, expectedSz, len(meta.schemas))
	}

	runChecks := func(schemaCnt int, fields []string, checks []func(int, []string)) {
		for _, check := range checks {
			check(schemaCnt, fields)
		}
	}
	checks := make([]func(int, []string), 0)

	// Empty object, schema with no fields
	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	objEmpty, err := cli.CreateBinaryObject(ctx, "MERGED")
	require.NoError(t, err)
	checks = append(checks, func(schemaCnt int, fields []string) {
		objSchemaSizeChecker(objEmpty, schemaCnt)
		objChecker(objEmpty, "MERGED", fields, []struct {
			name  string
			value interface{}
		}{
			{name: "id", value: nil},
			{"name", nil},
		}...)
	})
	runChecks(1, []string{}, checks)

	// New schema should be added since new field was added
	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	objOneField, err := cli.CreateBinaryObject(ctx, "MERGED", WithField("id", 10))
	require.NoError(t, err)
	checks = append(checks, func(schemaCnt int, fields []string) {
		objSchemaSizeChecker(objOneField, schemaCnt)
		objChecker(objOneField, "MERGED", fields, []struct {
			name  string
			value interface{}
		}{
			{name: "id", value: int64(10)},
			{"name", nil},
		}...)
	})
	runChecks(2, []string{"id"}, checks)

	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	_, err = cli.CreateBinaryObject(ctx, "MERGED", WithField("id", 10), WithAffinityKeyName("id"))
	require.Error(t, err) // should fail if affinity key name was changed
	runChecks(2, []string{"id"}, checks)

	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	_, err = cli.CreateBinaryObject(ctx, "MERGED", WithField("id", "test"))
	require.Error(t, err) // should fail if field type was changed
	runChecks(2, []string{"id"}, checks)

	// New schema should be added since new field was added
	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	objTwoField, err := cli.CreateBinaryObject(ctx, "MERGED", WithField("id", 10), WithField("name", "name"))
	require.NoError(t, err)
	checks = append(checks, func(schemaCnt int, fields []string) {
		objSchemaSizeChecker(objTwoField, schemaCnt)
		objChecker(objTwoField, "MERGED", fields, []struct {
			name  string
			value interface{}
		}{
			{name: "id", value: int64(10)},
			{"name", "name"},
		}...)
	})
	runChecks(3, []string{"id", "name"}, checks)

	// No new schema should be added.
	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	objOneFieldAfterTwo, err := cli.CreateBinaryObject(ctx, "MERGED", WithField("id", 10))
	require.NoError(t, err)
	checks = append(checks, func(schemaCnt int, fields []string) {
		objSchemaSizeChecker(objOneFieldAfterTwo, schemaCnt)
		objChecker(objOneFieldAfterTwo, "MERGED", fields, []struct {
			name  string
			value interface{}
		}{
			{name: "id", value: int64(10)},
			{"name", nil},
		}...)
	})
	runChecks(3, []string{"id", "name"}, checks)

	// New schema should be added if field order is different.
	if clearRegistry {
		cli.marsh.clearRegistry()
	}
	objTwoFieldDiffOrder, err := cli.CreateBinaryObject(ctx, "MERGED", WithField("name", "name"), WithField("id", 10))
	require.NoError(t, err)
	checks = append(checks, func(schemaCnt int, fields []string) {
		objSchemaSizeChecker(objTwoFieldDiffOrder, schemaCnt)
		objChecker(objTwoField, "MERGED", fields, []struct {
			name  string
			value interface{}
		}{
			{name: "id", value: int64(10)},
			{"name", "name"},
		}...)
	})
	runChecks(4, []string{"id", "name"}, checks)
}

func (suite *BinaryObjectTestSuite) createIndexedCache() *Cache {
	cfg := CreateCacheConfiguration("PERSON", WithCacheKeyConfiguration("PERSON_KEY", "bucket_id"),
		WithQueryEntity("PERSON_KEY", "PERSON", WithTableName("PERSON"),
			WithQueryField("ID", "java.lang.Long", WithKey()),
			WithQueryField("BUCKET_ID", "java.lang.Long", WithKey()),
			WithQueryField("NAME", "java.lang.String", WithNotNull(), WithDefaultValue("")),
			WithQueryField("AGE", "java.lang.Short", WithNotNull(), WithDefaultValue(int16(2))),
			WithIndex("NAME_IDX", WithIndexType(Sorted), WithInlineSize(200), WithIndexField(IndexField{Name: "NAME", Asc: true})),
			WithIndex("NAME_AGE_IDX", WithIndexField(IndexField{Name: "AGE", Asc: true}), WithIndexField(IndexField{Name: "NAME", Asc: true})),
		))
	cache, err := suite.cli.CreateCacheWithConfiguration(context.Background(), cfg)
	require.NoError(suite.T(), err)
	return cache
}

func (suite *BinaryObjectTestSuite) createSimpleCache() *Cache {
	cache, err := suite.cli.CreateCache(context.Background(), "SIMPLE")
	require.NoError(suite.T(), err)
	return cache
}

func (suite *BinaryObjectTestSuite) runTests(tests ...struct {
	f         func(*testing.T, *Client, *Cache)
	isIndexed bool
}) {
	defer func() {
		suite.KillAllGrids()
	}()
	for _, compactFooter := range []bool{true, false} {
		_, err := suite.StartIgnite(testing2.WithCompactFooter(compactFooter))
		require.NoError(suite.T(), err)
		suite.cli, err = StartTestClient(context.Background())
		require.NoError(suite.T(), err)
		require.Equal(suite.T(), compactFooter, suite.cli.marsh.isCompactFooter())
		for _, fixture := range tests {
			fName := runtime.FuncForPC(reflect.ValueOf(fixture.f).Pointer()).Name()
			suite.T().Run(fmt.Sprintf("%s/compactFooter-%t", fName, compactFooter), func(t *testing.T) {
				var cache *Cache
				if fixture.isIndexed {
					cache = suite.createIndexedCache()
				} else {
					cache = suite.createSimpleCache()
				}
				fixture.f(t, suite.cli, cache)
				DestroyAllCaches(t, suite.cli)
			})
		}
		_ = suite.cli.Close(context.Background())
		suite.KillAllGrids()
	}
}
