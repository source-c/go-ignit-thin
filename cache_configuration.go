package ignite

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// CacheAtomicityMode controls whether cache should maintain fully transactional semantics or more light-weight atomic behavior.
type CacheAtomicityMode int32

const (
	TransactionalAtomicityMode CacheAtomicityMode = 0 // Enables fully ACID-compliant transactional cache behavior.
	AtomicAtomicityMode        CacheAtomicityMode = 1 // Enables atomic-only cache behaviour.
)

// CacheMode specifies caching modes.
type CacheMode int32

const (
	ReplicatedCacheMode  CacheMode = 1 // Specifies fully replicated cache behaviour. In this mode all the keys are distributed to all participating nodes.
	PartitionedCacheMode CacheMode = 2 // Specifies partitioned cache behaviour.
)

// PartitionLossPolicy defines how a cache will behave in a case when one or more partitions are lost because of a node(s) failure.
type PartitionLossPolicy int32

const (
	// ReadOnlySafeLossPolicy all writes to the cache will be failed with an exception, reads will only be allowed for keys in non-lost partitions.
	// Reads from lost partitions will be failed with an exception.
	ReadOnlySafeLossPolicy PartitionLossPolicy = 0
	// ReadOnlyAllLossPolicy all writes to the cache will be failed with an exception. All reads will proceed as if all partitions were in a consistent state.
	// The result of reading from a lost partition is undefined and may be different on different nodes in the cluster.
	ReadOnlyAllLossPolicy PartitionLossPolicy = 1
	// ReadWriteSafeLossPolicy all reads and writes will be allowed for keys in valid partitions. All reads and writes for keys
	// in lost partitions will be failed with an exception.
	ReadWriteSafeLossPolicy PartitionLossPolicy = 2
	// ReadWriteAllLossPolicy all reads and writes will proceed as if all partitions were in a consistent state. The result of reading
	// from a lost partition is undefined and may be different on different nodes in the cluste
	ReadWriteAllLossPolicy PartitionLossPolicy = 3
	// IgnoreLossPolicy if a partition was lost silently ignore it and allow any operations with a partition. Partition loss events are not fired if using this mode.
	// For pure in-memory caches the policy will work only when baseline auto adjust is enabled with zero timeout.
	// If persistence is enabled, the policy is always ignored. ReadWriteSafeLossPolicy is used instead.
	IgnoreLossPolicy PartitionLossPolicy = 4
)

// CacheWriteSynchronizationMode indicates how Ignite should wait for write replies from other nodes. Default
// value is PrimarySyncSynchronizationMode, which means that Ignite will wait for write or commit to complete on
// primary node, but will not wait for backups to be updated.
type CacheWriteSynchronizationMode int32

const (
	// FullSyncSynchronizationMode indicates that Ignite should wait for write or commit replies from all nodes.
	// This behavior guarantees that whenever any of the atomic or transactional writes
	// complete, all other participating nodes which cache the written data have been updated.
	FullSyncSynchronizationMode CacheWriteSynchronizationMode = 0
	// FullAsyncSynchronizationMode indicates that Ignite will not wait for write or commit responses from participating nodes,
	// which means that remote nodes may get their state updated a bit after any of the cache write methods
	// complete, or after transaction commit completes.
	FullAsyncSynchronizationMode CacheWriteSynchronizationMode = 1
	// PrimarySyncSynchronizationMode only makes sense for PartitionedCacheMode and ReplicatedCacheMode.
	// When enabled, Ignite will wait for write or commit to complete on primary node, but will not wait for
	// backups to be updated.
	PrimarySyncSynchronizationMode CacheWriteSynchronizationMode = 2
)

// CacheRebalanceMode specifies how distributed caches will attempt to rebalance all necessary values from other grid nodes.
type CacheRebalanceMode int32

const (
	// SyncRebalanceMode means that distributed caches will not start until all necessary data
	// is loaded from other available grid nodes.
	SyncRebalanceMode CacheRebalanceMode = 0
	// AsyncRebalanceMode means that distributed caches will start immediately and will load all necessary
	// data from other available grid nodes in the background.
	AsyncRebalanceMode CacheRebalanceMode = 1
	// NoneRebalanceMode means that no rebalancing will take place which means that caches will be either loaded on
	// demand from persistent store whenever data is accessed, or will be populated explicitly.
	NoneRebalanceMode CacheRebalanceMode = 2
)

// CacheKeyConfig defines various aspects of cache keys without explicit usage of annotations on user classes.
type CacheKeyConfig struct {
	typeName      string
	affKeyFldName string
}

// TypeName returns affinity key type name.
func (c *CacheKeyConfig) TypeName() string {
	return c.typeName
}

// AffinityKeyFieldName returns affinity key field name.
func (c *CacheKeyConfig) AffinityKeyFieldName() string {
	return c.affKeyFldName
}

// QueryEntity is a description of [Cache] entry (composed of key and value) in a way of how it must be indexed and can be queried.
type QueryEntity struct {
	keyType    string
	valType    string
	tblName    string
	keyFldName string
	valFldName string
	fields     []QueryField
	aliases    map[string]string
	indexes    []QueryIndex
}

// KeyType returns key type for this query pair.
func (q *QueryEntity) KeyType() string {
	return q.keyType
}

// ValueType returns value type for this query pair.
func (q *QueryEntity) ValueType() string {
	return q.valType
}

// TableName returns table name for this query entity.
func (q *QueryEntity) TableName() string {
	return q.tblName
}

// KeyFieldName returns key field name.
func (q *QueryEntity) KeyFieldName() string {
	return q.keyFldName
}

// ValueFieldName returns value field name.
func (q *QueryEntity) ValueFieldName() string {
	return q.valFldName
}

// Fields returns query fields for this query pair. The order of fields is important as it defines the order
// of columns returned by the 'select *' queries.
func (q *QueryEntity) Fields() []QueryField {
	return q.fields
}

// Aliases returns aliases map.
func (q *QueryEntity) Aliases() map[string]string {
	return q.aliases
}

// Indexes returns index entities.
func (q *QueryEntity) Indexes() []QueryIndex {
	return q.indexes
}

func (q *QueryEntity) Copy() QueryEntity {
	ret := QueryEntity{
		keyType:    q.keyType,
		valType:    q.valType,
		tblName:    q.tblName,
		keyFldName: q.keyFldName,
		valFldName: q.valFldName,
	}
	if len(q.fields) > 0 {
		ret.fields = make([]QueryField, len(q.fields))
		copy(ret.fields, q.fields)
	}
	if len(q.aliases) > 0 {
		ret.aliases = make(map[string]string, len(q.aliases))
		for orig, alias := range q.aliases {
			ret.aliases[orig] = alias
		}
	}
	if len(q.indexes) > 0 {
		szIndexes := len(q.indexes)
		ret.indexes = make([]QueryIndex, szIndexes)
		for i := 0; i < szIndexes; i++ {
			ret.indexes[i] = q.indexes[i].Copy()
		}
	}
	return ret
}

// QueryField defines an index field in an indexed cache.
type QueryField struct {
	name      string
	typeName  string
	isKey     bool
	isNotNull bool
	precision int
	scale     int
	dfltVal   interface{}
}

// Name returns field name.
func (q *QueryField) Name() string {
	return q.name
}

// TypeName returns field type name.
func (q *QueryField) TypeName() string {
	return q.typeName
}

// IsKey returns true if field belongs to the key.
func (q *QueryField) IsKey() bool {
	return q.isKey
}

// IsNotNull returns true if field is not nullable.
func (q *QueryField) IsNotNull() bool {
	return q.isNotNull
}

// DefaultValue returns a default value of the field.
func (q *QueryField) DefaultValue() interface{} {
	return q.dfltVal
}

// Precision returns precision of the field, default is -1
func (q *QueryField) Precision() int {
	return q.precision
}

// Scale returns scale of the field, default is -1
func (q *QueryField) Scale() int {
	return q.scale
}

// IndexType defines type of the query index.
type IndexType int8

const (
	Sorted     IndexType = 0 // Sorted index, default.
	FullText   IndexType = 1 // FullText index.
	GeoSpatial IndexType = 2 // GeoSpatial index.
)

// IndexField defines field participating in index.
type IndexField struct {
	Name string // Name field name.
	Asc  bool   // Asc defines sort order, ascending if true, descending if false.
}

// QueryIndex defines query index metadata.
type QueryIndex struct {
	name     string
	idxType  IndexType
	inlineSz int
	fields   []IndexField
}

// Name returns name of the index.
func (q *QueryIndex) Name() string {
	return q.name
}

// Type returns index type.
func (q *QueryIndex) Type() IndexType {
	return q.idxType
}

// InlineSize returns inline size of the index, -1 means that size is determined automatically, 0 means that inlining is disabled.
func (q *QueryIndex) InlineSize() int {
	return q.inlineSz
}

// Fields return indexed fields.
func (q *QueryIndex) Fields() []IndexField {
	return q.fields
}

func (q *QueryIndex) Copy() QueryIndex {
	ret := QueryIndex{
		name:     q.name,
		idxType:  q.idxType,
		inlineSz: q.inlineSz,
	}
	szFlds := len(q.fields)
	if szFlds > 0 {
		ret.fields = make([]IndexField, szFlds)
		copy(ret.fields, q.fields)
	}
	return ret
}

type propertyCode = int16

const (
	cacheNameProp              propertyCode = 0
	cacheModeProp              propertyCode = 1
	cacheAtomicityModeProp     propertyCode = 2
	backupsProp                propertyCode = 3
	writeSyncModeProp          propertyCode = 4
	copyOnReadProp             propertyCode = 5
	readFromBackupProp         propertyCode = 6
	dataRegionNameProp         propertyCode = 100
	onHeapCacheEnabledProp     propertyCode = 101
	queryEntitiesProp          propertyCode = 200
	queryParallelismProp       propertyCode = 201
	queryDetailsMetricSizeProp propertyCode = 202
	sqlSchemaProp              propertyCode = 203
	sqlIndexMaxInlineSizeProp  propertyCode = 204
	sqlEscapeAllProp           propertyCode = 205
	maxQueryIteratorsProp      propertyCode = 206
	rebalanceModeProp          propertyCode = 300
	rebalanceOrderProp         propertyCode = 305
	groupNameProp              propertyCode = 400
	cacheKeyConfigProp         propertyCode = 401
	maxAsyncOpsProp            propertyCode = 403
	partitionLossPolicyProp    propertyCode = 404
	eagerTtlProp               propertyCode = 405
	statsEnabledProp           propertyCode = 406
	expirePolicyProp           propertyCode = 407
)

// CacheConfiguration defines all configuration parameters of the ignite cache.
type CacheConfiguration struct {
	props map[int16]interface{}
}

type CacheConfigurationOption func(*CacheConfiguration)

// CreateCacheConfiguration creates cache configuration, name specifies cache name, opts specify other cache parameters.
func CreateCacheConfiguration(name string, opts ...CacheConfigurationOption) CacheConfiguration {
	ret := CacheConfiguration{
		props: make(map[int16]interface{}),
	}
	ret.props[cacheNameProp] = name
	for _, opt := range opts {
		opt(&ret)
	}
	return ret
}

func (config *CacheConfiguration) marshal(ctx context.Context, marshaller marshaller, writer BinaryOutputStream) error {
	protoCtx := marshaller.protocolContext()
	origPos := writer.Position()
	writer.WriteInt32(0)
	writer.WriteInt16(int16(len(config.props)))
	for propCode, propValue := range config.props {
		writer.WriteInt16(propCode)
		switch val := propValue.(type) {
		case string:
			marshalString(writer, val)
		case bool:
			writer.WriteBool(val)
		case int:
			writer.WriteInt32(int32(val))
		case int32:
			writer.WriteInt32(val)
		case int64:
			writer.WriteInt64(val)
		case CacheMode:
			writer.WriteInt32(int32(val))
		case CacheAtomicityMode:
			writer.WriteInt32(int32(val))
		case PartitionLossPolicy:
			writer.WriteInt32(int32(val))
		case CacheWriteSynchronizationMode:
			writer.WriteInt32(int32(val))
		case CacheRebalanceMode:
			writer.WriteInt32(int32(val))
		case *ExpiryPolicy:
			{
				if !protoCtx.SupportsExpiryPolicy() {
					return fmt.Errorf("expiry policies are not supported on protocol version %v", protoCtx.Version())
				}
				if val == nil {
					writer.WriteBool(false)
				} else {
					writer.WriteBool(true)
					writer.WriteInt64(durationToMillis(val.Creation()))
					writer.WriteInt64(durationToMillis(val.Update()))
					writer.WriteInt64(durationToMillis(val.Access()))
				}
			}
		case []CacheKeyConfig:
			{
				err := writeSequence(writer, len(val), func(output BinaryOutputStream, idx int) error {
					keyCfg := val[idx]
					marshalString(output, keyCfg.TypeName())
					marshalString(output, keyCfg.AffinityKeyFieldName())
					return nil
				})
				if err != nil {
					return err
				}
			}
		case []QueryEntity:
			{
				err := writeSequence(writer, len(val), func(output BinaryOutputStream, idx int) error {
					entity := val[idx]
					return marshalQueryEntity(ctx, marshaller, output, &entity)
				})
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("property is not supported %d, %v", propCode, propValue)
		}
	}
	finalPos := writer.Position()
	writer.SetPosition(origPos)
	writer.WriteInt32(int32(finalPos - origPos - 4))
	writer.SetPosition(finalPos)
	return nil
}

func unmarshalCacheConfiguration(ctx context.Context, marshaller marshaller, reader BinaryInputStream) (CacheConfiguration, error) {
	var err error
	protoCtx := marshaller.protocolContext()
	reader.ReadInt32() // Skip unneeded length field
	props := make(map[int16]interface{})
	conf := CacheConfiguration{
		props: props,
	}
	props[cacheAtomicityModeProp] = CacheAtomicityMode(reader.ReadInt32())
	props[backupsProp] = int(reader.ReadInt32())
	props[cacheModeProp] = CacheMode(reader.ReadInt32())
	props[copyOnReadProp] = reader.ReadBool()
	if err = conf.setStringProperty(reader, dataRegionNameProp); err != nil {
		return conf, err
	}
	props[eagerTtlProp] = reader.ReadBool()
	props[statsEnabledProp] = reader.ReadBool()
	if err = conf.setStringProperty(reader, groupNameProp); err != nil {
		return conf, err
	}
	reader.ReadInt64() // Skip deprecated DefaultLockTimeout
	props[maxAsyncOpsProp] = int(reader.ReadInt32())
	props[maxQueryIteratorsProp] = int(reader.ReadInt32())
	if err = conf.setStringProperty(reader, cacheNameProp); err != nil {
		return conf, err
	}
	props[onHeapCacheEnabledProp] = reader.ReadBool()
	props[partitionLossPolicyProp] = PartitionLossPolicy(reader.ReadInt32())
	props[queryDetailsMetricSizeProp] = int(reader.ReadInt32())
	props[queryParallelismProp] = int(reader.ReadInt32())
	props[readFromBackupProp] = reader.ReadBool()
	reader.ReadInt32() // Skip deprecated RebalanceBatchSize
	reader.ReadInt64() // Skip deprecated RebalanceBatchesPrefetchCount
	reader.ReadInt64() // Skip deprecated RebalanceDelay
	props[rebalanceModeProp] = CacheRebalanceMode(reader.ReadInt32())
	props[rebalanceOrderProp] = int(reader.ReadInt32())
	reader.ReadInt64() // Skip deprecated RebalanceThrottle
	reader.ReadInt64() // Skip deprecated RebalanceTimeout
	props[sqlEscapeAllProp] = reader.ReadBool()
	props[sqlIndexMaxInlineSizeProp] = int(reader.ReadInt32())
	if err = conf.setStringProperty(reader, sqlSchemaProp); err != nil {
		return conf, err
	}
	props[writeSyncModeProp] = CacheWriteSynchronizationMode(reader.ReadInt32())
	err = setCollectionProperty(&conf, reader, cacheKeyConfigProp, func(_ int, reader BinaryInputStream) (CacheKeyConfig, error) {
		keyConfig := CacheKeyConfig{}
		var err0 error
		keyConfig.typeName, err0 = unmarshalString(reader)
		if err0 != nil {
			return keyConfig, err0
		}
		keyConfig.affKeyFldName, err0 = unmarshalString(reader)
		if err0 != nil {
			return keyConfig, err0
		}
		return keyConfig, nil
	})
	if err != nil {
		return conf, err
	}
	err = setCollectionProperty(&conf, reader, queryEntitiesProp, unmarshalQueryEntity(ctx, marshaller))
	if err != nil {
		return conf, err
	}
	if protoCtx.SupportsExpiryPolicy() && reader.ReadBool() {
		expPolicy := &ExpiryPolicy{
			creation: millisToDuration(reader.ReadInt64()),
			update:   millisToDuration(reader.ReadInt64()),
			access:   millisToDuration(reader.ReadInt64()),
		}
		props[expirePolicyProp] = expPolicy
	}
	return conf, err
}

func marshalQueryEntity(ctx context.Context, marshaller marshaller, writer BinaryOutputStream, entity *QueryEntity) error {
	marshalString(writer, entity.KeyType())
	marshalString(writer, entity.ValueType())
	marshalEmptyStringAsNull(writer, entity.TableName())
	marshalEmptyStringAsNull(writer, entity.KeyFieldName())
	marshalEmptyStringAsNull(writer, entity.ValueFieldName())

	fields := entity.Fields()
	err := writeSequence(writer, len(fields), func(output BinaryOutputStream, idx int) error {
		field := fields[idx]
		return marshalQueryField(ctx, marshaller, output, &field)
	})
	if err != nil {
		return err
	}
	writer.WriteInt32(int32(len(entity.Aliases())))
	for orig, alias := range entity.Aliases() {
		marshalString(writer, orig)
		marshalString(writer, alias)
	}

	indexes := entity.Indexes()
	err = writeSequence(writer, len(indexes), func(output BinaryOutputStream, idx int) error {
		index := indexes[idx]
		return marshalQueryIndex(output, &index)
	})
	if err != nil {
		return err
	}
	return nil
}

func marshalQueryField(ctx context.Context, marshaller marshaller, writer BinaryOutputStream, field *QueryField) error {
	protoCtx := marshaller.protocolContext()
	withPrecisionScale := protoCtx.SupportsQueryEntityPrecisionAndScale()
	marshalString(writer, field.Name())
	marshalString(writer, field.TypeName())
	writer.WriteBool(field.IsKey())
	writer.WriteBool(field.IsNotNull())
	err := marshaller.marshal(ctx, writer, field.DefaultValue())
	if err != nil {
		return err
	}
	if withPrecisionScale {
		writer.WriteInt32(int32(field.Precision()))
		writer.WriteInt32(int32(field.Scale()))
	}
	return err
}

func marshalQueryIndex(writer BinaryOutputStream, index *QueryIndex) error {
	marshalString(writer, index.Name())
	writer.WriteInt8(int8(index.Type()))
	writer.WriteInt32(int32(index.InlineSize()))

	fields := index.Fields()
	err := writeSequence(writer, len(fields), func(_ BinaryOutputStream, idx int) error {
		field := fields[idx]
		marshalString(writer, field.Name)
		writer.WriteBool(field.Asc)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func unmarshalQueryEntity(ctx context.Context, marshaller marshaller) func(int, BinaryInputStream) (QueryEntity, error) {
	qryFieldUnmarshalProc := unmarshalQueryField(ctx, marshaller)
	return func(_ int, reader BinaryInputStream) (QueryEntity, error) {
		var err error
		entity := QueryEntity{}
		entity.keyType, err = unmarshalString(reader)
		if err != nil {
			return entity, err
		}
		entity.valType, err = unmarshalString(reader)
		if err != nil {
			return entity, err
		}
		entity.tblName, err = unmarshalString(reader)
		if err != nil {
			return entity, err
		}
		entity.keyFldName, err = unmarshalString(reader)
		if err != nil {
			return entity, err
		}
		entity.valFldName, err = unmarshalString(reader)
		if err != nil {
			return entity, err
		}
		entity.fields, err = readSlice(reader, qryFieldUnmarshalProc)
		if err != nil {
			return entity, err
		}
		aliasesSz := reader.ReadInt32()
		entity.aliases = make(map[string]string, aliasesSz)
		for i := 0; i < int(aliasesSz); i++ {
			key, err0 := unmarshalString(reader)
			if err0 != nil {
				return entity, err0
			}
			val, err0 := unmarshalString(reader)
			if err0 != nil {
				return entity, err0
			}
			entity.aliases[key] = val
		}
		entity.indexes, err = readSlice(reader, unmarshalQueryIndex)
		if err != nil {
			return entity, err
		}
		return entity, nil
	}
}

func unmarshalQueryField(ctx context.Context, marshaller marshaller) func(int, BinaryInputStream) (QueryField, error) {
	protoCtx := marshaller.protocolContext()
	withPrecisionScale := protoCtx.SupportsQueryEntityPrecisionAndScale()
	return func(_ int, reader BinaryInputStream) (QueryField, error) {
		var err error
		fld := QueryField{precision: -1, scale: -1}
		fld.name, err = unmarshalString(reader)
		if err != nil {
			return fld, err
		}
		fld.typeName, err = unmarshalString(reader)
		if err != nil {
			return fld, err
		}
		fld.isKey = reader.ReadBool()
		fld.isNotNull = reader.ReadBool()
		fld.dfltVal, err = marshaller.unmarshal(ctx, reader)
		if err != nil {
			return fld, err
		}
		if withPrecisionScale {
			fld.precision = int(reader.ReadInt32())
		}
		if withPrecisionScale {
			fld.scale = int(reader.ReadInt32())
		}
		return fld, nil
	}
}

func unmarshalQueryIndex(_ int, reader BinaryInputStream) (QueryIndex, error) {
	idx := QueryIndex{}
	name, err := unmarshalString(reader)
	if err != nil {
		return idx, err
	}
	idx.name = name
	idx.idxType = IndexType(reader.ReadInt8())
	idx.inlineSz = int(reader.ReadInt32())
	idx.fields, err = readSlice(reader, func(_ int, reader0 BinaryInputStream) (IndexField, error) {
		fldName, err0 := unmarshalString(reader0)
		if err0 != nil {
			return IndexField{}, err0
		}
		return IndexField{Name: fldName, Asc: reader0.ReadBool()}, nil
	})
	if err != nil {
		return idx, err
	}
	return idx, nil
}

func (config *CacheConfiguration) setStringProperty(reader BinaryInputStream, propCode propertyCode) error {
	var err error
	var val string
	if val, err = unmarshalString(reader); err != nil {
		return err
	}
	if len(val) > 0 {
		config.props[propCode] = val
	}
	return nil
}

// Copy creates full copy of the configuration, optional opts replace configuration parameters in a new copy if passed.
func (config *CacheConfiguration) Copy(opts ...CacheConfigurationOption) CacheConfiguration {
	newConfig := CacheConfiguration{
		props: make(map[int16]interface{}),
	}
	for code, prop := range config.props {
		switch val := prop.(type) {
		case []QueryEntity:
			{
				szvVal := len(val)
				if szvVal > 0 {
					cpEntities := make([]QueryEntity, szvVal)
					for i := 0; i < szvVal; i++ {
						cpEntities[i] = val[i].Copy()
					}
					newConfig.props[code] = cpEntities
				}
			}
		case []CacheKeyConfig:
			{
				szVal := len(val)
				if szVal > 0 {
					cpKeyCfg := make([]CacheKeyConfig, szVal)
					copy(cpKeyCfg, val)
					newConfig.props[code] = cpKeyCfg
				}
			}
		default:
			newConfig.props[code] = val
		}
	}
	for _, opt := range opts {
		opt(&newConfig)
	}
	return newConfig
}

// Name returns name of the cache.
func (config *CacheConfiguration) Name() string {
	return getProperty(config, cacheNameProp, "")
}

// Backups returns number of backups.
func (config *CacheConfiguration) Backups() int {
	return getProperty(config, backupsProp, 0)
}

// CacheMode returns cache mode.
func (config *CacheConfiguration) CacheMode() CacheMode {
	return getProperty[CacheMode](config, cacheModeProp, -1)
}

// CacheAtomicityMode returns cache atomicity mode.
func (config *CacheConfiguration) CacheAtomicityMode() CacheAtomicityMode {
	return getProperty[CacheAtomicityMode](config, cacheAtomicityModeProp, -1)
}

// IsCopyOnRead returns whether a copy of the value stored in the on-heap cache should be created for a cache operation return the value.
func (config *CacheConfiguration) IsCopyOnRead() bool {
	return getProperty(config, copyOnReadProp, false)
}

// DataRegionName returns data region name where cache data are stored.
func (config *CacheConfiguration) DataRegionName() string {
	return getProperty(config, dataRegionNameProp, "")
}

// IsEagerTtl returns whether expired cache entries will be eagerly removed from cache.
func (config *CacheConfiguration) IsEagerTtl() bool {
	return getProperty(config, eagerTtlProp, false)
}

// CacheGroupName returns the cache group name. Caches with the same group name share single underlying 'physical' cache (partition set),
// but are logically isolated.
func (config *CacheConfiguration) CacheGroupName() string {
	return getProperty(config, groupNameProp, "")
}

// MaxConcurrentAsyncOperations returns maximum number of allowed concurrent asynchronous operations, 0 means that the number is unlimited.
func (config *CacheConfiguration) MaxConcurrentAsyncOperations() int {
	return getProperty(config, maxAsyncOpsProp, 0)
}

// MaxQueryIteratorsCount returns maximum number of query iterators that can be stored.
func (config *CacheConfiguration) MaxQueryIteratorsCount() int {
	return getProperty(config, maxQueryIteratorsProp, 0)
}

// IsOnHeapCacheEnabled returns whether the on-heap cache is enabled for the off-heap based page memory.
func (config *CacheConfiguration) IsOnHeapCacheEnabled() bool {
	return getProperty(config, onHeapCacheEnabledProp, false)
}

// PartitionLossPolicy returns partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
// some partition leave the cluster.
func (config *CacheConfiguration) PartitionLossPolicy() PartitionLossPolicy {
	return getProperty[PartitionLossPolicy](config, partitionLossPolicyProp, -1)
}

// QueryDetailsMetricsSize returns size of queries detail metrics that will be stored in memory for monitoring purposes.
// If 0 then history will not be collected.
func (config *CacheConfiguration) QueryDetailsMetricsSize() int {
	return getProperty(config, queryDetailsMetricSizeProp, 0)
}

// QueryParallelism returns a hint to query execution engine on desired degree of parallelism within a single node.
func (config *CacheConfiguration) QueryParallelism() int {
	return getProperty(config, queryParallelismProp, 0)
}

// IsReadFromBackup indicates whether data can be read from backup. If false always get data from primary node (never from backup).
func (config *CacheConfiguration) IsReadFromBackup() bool {
	return getProperty(config, readFromBackupProp, false)
}

// RebalanceMode returns rebalance mode for distributed cache.
func (config *CacheConfiguration) RebalanceMode() CacheRebalanceMode {
	return getProperty(config, rebalanceModeProp, SyncRebalanceMode)
}

// RebalanceOrder returns a cache rebalance order. The rebalance order guarantees that rebalancing for this cache will start only when rebalancing for
// all caches with smaller rebalance order will be completed. Default is 0
func (config *CacheConfiguration) RebalanceOrder() int {
	return getProperty(config, rebalanceOrderProp, 0)
}

// IsSqlEscapeAll returns sql escape flag. If true all the SQL table and field names will be escaped with double quotes like
// "tableName"."fieldsName". This enforces case sensitivity for field names and also allows having special characters in table and field names.
func (config *CacheConfiguration) IsSqlEscapeAll() bool {
	return getProperty(config, sqlEscapeAllProp, false)
}

// IsStatsEnabled returns whether collecting statistics is enabled.
func (config *CacheConfiguration) IsStatsEnabled() bool {
	return getProperty(config, statsEnabledProp, false)
}

// SqlIndexMaxInlineSize return maximum inline size for sql indexes. If -1 then maximum possible inline size is used.
func (config *CacheConfiguration) SqlIndexMaxInlineSize() int {
	return getProperty(config, sqlIndexMaxInlineSizeProp, 0)
}

// SqlSchema returns sql schema name for the indexed cache.
func (config *CacheConfiguration) SqlSchema() string {
	return getProperty(config, sqlSchemaProp, "")
}

// WriteSynchronizationMode returns write synchronization mode.
func (config *CacheConfiguration) WriteSynchronizationMode() CacheWriteSynchronizationMode {
	return getProperty[CacheWriteSynchronizationMode](config, writeSyncModeProp, -1)
}

// ExpiryPolicy returns cache expiry policy. See [ExpiryPolicy] for details.
func (config *CacheConfiguration) ExpiryPolicy() *ExpiryPolicy {
	return getProperty[*ExpiryPolicy](config, expirePolicyProp, nil)
}

// KeyConfiguration returns cache key configuration.
func (config *CacheConfiguration) KeyConfiguration() []CacheKeyConfig {
	return getProperty[[]CacheKeyConfig](config, cacheKeyConfigProp, nil)
}

// QueryEntities returns query entities of the cache. If set then the cache is indexed.
func (config *CacheConfiguration) QueryEntities() []QueryEntity {
	return getProperty[[]QueryEntity](config, queryEntitiesProp, nil)
}

func getProperty[T any](config *CacheConfiguration, code propertyCode, defaultValue T) T {
	data, hasKey := config.props[code]
	if !hasKey {
		return defaultValue
	}
	ret, ok := data.(T)
	if !ok {
		return defaultValue
	}
	return ret
}

func setCollectionProperty[T any](config *CacheConfiguration, reader BinaryInputStream, code propertyCode, elemReader func(int, BinaryInputStream) (T, error)) error {
	coll, err := readSlice[T](reader, elemReader)
	if err != nil {
		return err
	}
	config.props[code] = coll
	return nil
}

// WithCacheName returns [CacheConfigurationOption] that sets a cache name. Especially useful with [CacheConfiguration.Copy]
func WithCacheName(name string) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		name = strings.TrimSpace(name)
		if len(name) > 0 {
			config.props[cacheNameProp] = name
		}
	}
}

// WithCacheGroupName returns [CacheConfigurationOption] that sets a cache group name.
func WithCacheGroupName(name string) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		name = strings.TrimSpace(name)
		if len(name) > 0 {
			config.props[groupNameProp] = name
		}
	}
}

// WithBackupsCount returns [CacheConfigurationOption] that sets a number of cache backups.
func WithBackupsCount(cnt int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[backupsProp] = cnt
	}
}

// WithWriteSynchronizationMode returns [CacheConfigurationOption] that sets cache write synchronization mode.
func WithWriteSynchronizationMode(mode CacheWriteSynchronizationMode) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[writeSyncModeProp] = mode
	}
}

// WithCopyOnRead returns [CacheConfigurationOption] that sets copy-on-read flag. See [CacheConfiguration.IsCopyOnRead].
func WithCopyOnRead(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[copyOnReadProp] = enabled
	}
}

// WithReadFromBackup returns [CacheConfigurationOption] that sets read-from-backup flag. See [CacheConfiguration.IsReadFromBackup].
func WithReadFromBackup(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[readFromBackupProp] = enabled
	}
}

// WithDataRegionName returns [CacheConfigurationOption] that sets cache data region name. See [CacheConfiguration.DataRegionName].
func WithDataRegionName(dataRegionName string) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		dataRegionName = strings.TrimSpace(dataRegionName)
		if len(dataRegionName) > 0 {
			config.props[dataRegionNameProp] = dataRegionName
		}
	}
}

// WithOnHeapCacheEnabled returns [CacheConfigurationOption] that sets on-heap caching flag. See [CacheConfiguration.IsOnHeapCacheEnabled].
func WithOnHeapCacheEnabled(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[onHeapCacheEnabledProp] = enabled
	}
}

// WithCacheMode returns [CacheConfigurationOption] that sets cache mode. See [CacheConfiguration.CacheMode].
func WithCacheMode(mode CacheMode) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[cacheModeProp] = mode
	}
}

// WithCacheAtomicityMode returns [CacheConfigurationOption] that sets cache atomicity mode. See [CacheConfiguration.CacheAtomicityMode].
func WithCacheAtomicityMode(mode CacheAtomicityMode) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[cacheAtomicityModeProp] = mode
	}
}

// WithQueryParallelism returns [CacheConfigurationOption] that sets cache query parallelism. See [CacheConfiguration.QueryParallelism].
func WithQueryParallelism(parallelism int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[queryParallelismProp] = parallelism
	}
}

// WithQueryDetailsMetricsSize returns [CacheConfigurationOption] that sets query details metrics size. See [CacheConfiguration.QueryDetailsMetricsSize].
func WithQueryDetailsMetricsSize(size int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[queryDetailsMetricSizeProp] = size
	}
}

// WithSqlSchema returns [CacheConfigurationOption] that sets sql schema of cache.
func WithSqlSchema(schema string) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		schema = strings.TrimSpace(schema)
		if len(schema) > 0 {
			config.props[sqlSchemaProp] = schema
		}
	}
}

// WithSqlIndexMaxInlineSize returns [CacheConfigurationOption] that sets sql index maximal inline size. See [CacheConfiguration.SqlIndexMaxInlineSize].
func WithSqlIndexMaxInlineSize(size int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[sqlIndexMaxInlineSizeProp] = size
	}
}

// WithSqlEscapeAll returns [CacheConfigurationOption] that sets sql escape flag. See [CacheConfiguration.IsSqlEscapeAll].
func WithSqlEscapeAll(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[sqlEscapeAllProp] = enabled
	}
}

// WithRebalanceMode returns [CacheConfigurationOption] that sets a rebalance mode of the cache. See [CacheConfiguration.RebalanceMode].
func WithRebalanceMode(mode CacheRebalanceMode) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[rebalanceModeProp] = mode
	}
}

// WithRebalanceOrder returns [CacheConfigurationOption] that sets a rebalance order of the cache. See [CacheConfiguration.RebalanceOrder].
func WithRebalanceOrder(order int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[rebalanceOrderProp] = order
	}
}

// WithMaxConcurrentAsyncOperations returns [CacheConfigurationOption] that sets maximal number of concurrent async operations.
// See [CacheConfiguration.MaxConcurrentAsyncOperations].
func WithMaxConcurrentAsyncOperations(count int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[maxAsyncOpsProp] = count
	}
}

// WithPartitionLossPolicy returns [CacheConfigurationOption] that sets a partition loss policy for a cache. See [CacheConfiguration.PartitionLossPolicy].
func WithPartitionLossPolicy(policy PartitionLossPolicy) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[partitionLossPolicyProp] = policy
	}
}

// WithMaxQueryIteratorsCount returns [CacheConfigurationOption] that sets maximal number of query iterators. See [CacheConfiguration.MaxQueryIteratorsCount].
func WithMaxQueryIteratorsCount(count int) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[maxQueryIteratorsProp] = count
	}
}

// WithEagerTtl returns [CacheConfigurationOption] that sets an eager ttl flag. See [CacheConfiguration.IsEagerTtl].
func WithEagerTtl(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[eagerTtlProp] = enabled
	}
}

// WithStatsEnabled returns [CacheConfigurationOption] that sets a statistics enabled flag. See [CacheConfiguration.IsStatsEnabled].
func WithStatsEnabled(enabled bool) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[statsEnabledProp] = enabled
	}
}

// WithExpiryPolicy returns [CacheConfigurationOption] that sets an expiry policy of the cache. See [ExpiryPolicy] for details.
func WithExpiryPolicy(creation time.Duration, access time.Duration, update time.Duration) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		config.props[expirePolicyProp] = &ExpiryPolicy{creation: creation, access: access, update: update}
	}
}

// WithCacheKeyConfiguration returns [CacheConfigurationOption] that specifies cache affinity key with given type name and affinity field name.
func WithCacheKeyConfiguration(typeName string, affinityKeyField string) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		var keyConfigs []CacheKeyConfig
		val, found := config.props[cacheKeyConfigProp]
		if !found {
			keyConfigs = make([]CacheKeyConfig, 0)
		} else {
			keyConfigs = val.([]CacheKeyConfig)
		}
		for _, keyConfig := range keyConfigs {
			if keyConfig.AffinityKeyFieldName() == affinityKeyField {
				return
			}
		}
		keyConfigs = append(keyConfigs, CacheKeyConfig{typeName: typeName, affKeyFldName: affinityKeyField})
		config.props[cacheKeyConfigProp] = keyConfigs
	}
}

type QueryEntityOption func(entity *QueryEntity)

// WithQueryEntity returns [CacheConfigurationOption] that specifies [QueryEntity] for the cache, keyType and valueType
// specifies types for key and value, opts set other options for [QueryEntity].
func WithQueryEntity(keyType string, valueType string, opts ...QueryEntityOption) CacheConfigurationOption {
	return func(config *CacheConfiguration) {
		var queryEntities []QueryEntity
		val, found := config.props[queryEntitiesProp]
		if !found {
			queryEntities = make([]QueryEntity, 0)
		} else {
			queryEntities = val.([]QueryEntity)
		}
		for _, ent := range queryEntities {
			if valueType == ent.ValueType() {
				return
			}
		}
		entity := QueryEntity{
			keyType: keyType,
			valType: valueType,
		}
		for _, opt := range opts {
			opt(&entity)
		}
		queryEntities = append(queryEntities, entity)
		config.props[queryEntitiesProp] = queryEntities
	}
}

// WithTableName returns [QueryEntityOption] that sets sql table name.
func WithTableName(name string) QueryEntityOption {
	return func(entity *QueryEntity) {
		name = strings.TrimSpace(name)
		if len(name) > 0 {
			entity.tblName = name
		}
	}
}

// WithKeyFieldName returns [QueryEntityOption] that sets key field name.
func WithKeyFieldName(name string) QueryEntityOption {
	return func(entity *QueryEntity) {
		name = strings.TrimSpace(name)
		if len(name) > 0 {
			entity.keyFldName = name
		}
	}
}

// WithValueFieldName returns [QueryEntityOption] that sets value field name.
func WithValueFieldName(name string) QueryEntityOption {
	return func(entity *QueryEntity) {
		name = strings.TrimSpace(name)
		if len(name) > 0 {
			entity.valFldName = name
		}
	}
}

type QueryFieldOption func(field *QueryField)

// WithQueryField returns [QueryEntityOption] that adds query field to the resulting [QueryEntity].
// name and typeName set a name and a type name of the field respectively, opts set other options.
func WithQueryField(name string, typeName string, opts ...QueryFieldOption) QueryEntityOption {
	return func(entity *QueryEntity) {
		if entity.fields == nil {
			entity.fields = make([]QueryField, 0)
		}
		field := QueryField{
			name:      name,
			typeName:  typeName,
			isKey:     false,
			isNotNull: false,
			precision: -1,
			scale:     -1,
			dfltVal:   nil,
		}
		for _, opt := range opts {
			opt(&field)
		}
		entity.fields = append(entity.fields, field)
	}
}

// WithNotNull returns [QueryFieldOption] that sets this field non-nullable.
func WithNotNull() QueryFieldOption {
	return func(field *QueryField) {
		field.isNotNull = true
	}
}

// WithKey returns [QueryFieldOption] that sets this field as a part of key.
func WithKey() QueryFieldOption {
	return func(field *QueryField) {
		field.isKey = true
	}
}

// WithDefaultValue returns [QueryFieldOption] that sets default value of the field.
func WithDefaultValue(val interface{}) QueryFieldOption {
	return func(field *QueryField) {
		field.dfltVal = val
	}
}

// WithScale returns [QueryFieldOption] that sets scale of the field.
func WithScale(scale int) QueryFieldOption {
	return func(field *QueryField) {
		field.scale = scale
	}
}

// WithPrecision returns [QueryFieldOption] that sets precision of the field.
func WithPrecision(precision int) QueryFieldOption {
	return func(field *QueryField) {
		field.precision = precision
	}
}

// WithFieldAlias returns [QueryEntityOption] that sets alias for the field name.
func WithFieldAlias(origName string, alias string) QueryEntityOption {
	return func(entity *QueryEntity) {
		if entity.aliases == nil {
			entity.aliases = make(map[string]string)
		}
		entity.aliases[origName] = alias
	}
}

type QueryIndexOption func(index *QueryIndex)

// WithIndex returns [QueryEntityOption] that adds an index to [QueryEntity]. name specifies a name
// the index, opts specify other options.
func WithIndex(name string, opts ...QueryIndexOption) QueryEntityOption {
	return func(entity *QueryEntity) {
		if entity.indexes == nil {
			entity.indexes = make([]QueryIndex, 0)
		}
		for _, idx := range entity.indexes {
			if idx.Name() == name {
				return
			}
		}
		idx := QueryIndex{name: name, inlineSz: -1, idxType: Sorted, fields: make([]IndexField, 0)}
		for _, opt := range opts {
			opt(&idx)
		}
		entity.indexes = append(entity.indexes, idx)
	}
}

// WithIndexType returns [QueryIndexOption] that sets index type.
func WithIndexType(indexType IndexType) QueryIndexOption {
	return func(index *QueryIndex) {
		index.idxType = indexType
	}
}

// WithInlineSize returns [QueryIndexOption] that sets index inline size. See [QueryIndex.InlineSize]
func WithInlineSize(inlineSize int) QueryIndexOption {
	return func(index *QueryIndex) {
		index.inlineSz = inlineSize
	}
}

// WithIndexField returns [QueryIndexOption] that sets an index field. See [IndexField].
func WithIndexField(field IndexField) QueryIndexOption {
	return func(index *QueryIndex) {
		for _, f := range index.fields {
			if f.Name == field.Name {
				return
			}
		}
		index.fields = append(index.fields, field)
	}
}
