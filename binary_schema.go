package ignite

import (
	"context"
	"fmt"
	"sync"
)

const (
	fnv1OffsetBasis uint32 = 0x811C9DC5
	fnv1Prime       uint32 = 0x01000193
	maxOffset1             = 1 << 8
	maxOffset2             = 1 << 16
	opGetBinaryType int16  = 3002
	opPutBinaryType int16  = 3003
)

// BinaryType contains binary object type metadata.
type BinaryType interface {
	// TypeId returns type identificator.
	TypeId() int32
	// TypeName returns type name
	TypeName() string
	// AffinityKeyName returns field that is used as affinity key.
	AffinityKeyName() string
	// IsEnum returns true if binary enum, false otherwise.
	IsEnum() bool
	// Fields returns fields of the binary object.
	Fields() []string
}

type binaryMetadata struct {
	typeId      int32
	typeName    string
	affKeyName  string
	isEnum      bool
	fields      map[string]binaryFieldMeta
	fieldsOrder []string
	schemas     map[int32]*binarySchema
}

func newBinaryMetadata(typeId int32, typeName string, affKeyName string) *binaryMetadata {
	return &binaryMetadata{
		typeId:      typeId,
		typeName:    typeName,
		affKeyName:  affKeyName,
		isEnum:      false,
		fields:      make(map[string]binaryFieldMeta),
		fieldsOrder: make([]string, 0),
		schemas:     make(map[int32]*binarySchema),
	}
}

func (meta *binaryMetadata) addField(name string, typeId int32, fieldId int32) {
	meta.fields[name] = binaryFieldMeta{typeId: typeId, fieldId: fieldId}
	meta.fieldsOrder = append(meta.fieldsOrder, name)
}

func (meta *binaryMetadata) addSchema(schema binarySchema) {
	cpy := schema.copy()
	meta.schemas[schema.schemaId] = &cpy
}

func (meta *binaryMetadata) copy() *binaryMetadata {
	cpy := binaryMetadata{
		typeId:      meta.typeId,
		typeName:    meta.typeName,
		affKeyName:  meta.affKeyName,
		isEnum:      meta.isEnum,
		fields:      make(map[string]binaryFieldMeta),
		fieldsOrder: make([]string, len(meta.fieldsOrder)),
		schemas:     make(map[int32]*binarySchema),
	}
	copy(cpy.fieldsOrder, meta.fieldsOrder)
	for k, v := range meta.fields {
		cpy.fields[k] = v
	}
	for k, v := range meta.schemas {
		scCpy := v.copy()
		cpy.schemas[k] = &scCpy
	}
	return &cpy
}

func (meta *binaryMetadata) TypeId() int32 {
	return meta.typeId
}

func (meta *binaryMetadata) TypeName() string {
	return meta.typeName
}

func (meta *binaryMetadata) AffinityKeyName() string {
	return meta.affKeyName
}

func (meta *binaryMetadata) IsEnum() bool {
	return meta.isEnum
}

func (meta *binaryMetadata) Fields() []string {
	ret := make([]string, len(meta.fieldsOrder))
	copy(ret, meta.fieldsOrder)
	return ret
}

type binarySchema struct {
	schemaId int32
	fieldIds []int32
}

func (s *binarySchema) copy() binarySchema {
	cpy := binarySchema{
		schemaId: s.schemaId,
		fieldIds: make([]int32, len(s.fieldIds)),
	}
	copy(cpy.fieldIds, s.fieldIds)
	return cpy
}

type binarySchemaBuilder struct {
	schemaId int32
	data     []int32
}

func newBinarySchemaBuilder() *binarySchemaBuilder {
	schemaId := fnv1OffsetBasis
	return &binarySchemaBuilder{
		schemaId: int32(schemaId),
		data:     make([]int32, 0),
	}
}

func (bld *binarySchemaBuilder) AddField(fieldId int32, offset int32) {
	bld.data = append(bld.data, fieldId)
	bld.data = append(bld.data, offset)
	bld.schemaId = updateSchemaId(bld.schemaId, fieldId)
}

func (bld *binarySchemaBuilder) Build() binarySchema {
	ret := binarySchema{
		schemaId: bld.schemaId,
		fieldIds: make([]int32, 0),
	}
	for idx := 0; idx < len(bld.data); idx += 2 {
		ret.fieldIds = append(ret.fieldIds, bld.data[idx])
	}
	return ret
}

func (bld *binarySchemaBuilder) WriteFooter(writer BinaryOutputStream, compactFooter bool) int {
	fieldCnt := len(bld.data) / 2
	if fieldCnt == 0 {
		return 0
	}
	lastOffset := bld.data[len(bld.data)-1]
	var fldSize int
	if lastOffset < maxOffset1 {
		fldSize = 1
	} else if lastOffset < maxOffset2 {
		fldSize = 2
	} else {
		fldSize = 4
	}
	for idx := 0; idx < len(bld.data); idx += 2 {
		if !compactFooter {
			writer.WriteInt32(bld.data[idx])
		}
		offset := bld.data[idx+1]
		if fldSize == 1 {
			writer.WriteInt8(int8(offset))
		} else if fldSize == 2 {
			writer.WriteInt16(int16(offset))
		} else {
			writer.WriteInt32(offset)
		}
	}
	return fldSize
}

func updateSchemaId(schemaId int32, fieldId int32) int32 {
	schemaId = schemaId ^ (fieldId & 0xFF)
	schemaId = schemaId * int32(fnv1Prime)
	schemaId = schemaId ^ ((fieldId >> 8) & 0xFF)
	schemaId = schemaId * int32(fnv1Prime)
	schemaId = schemaId ^ ((fieldId >> 16) & 0xFF)
	schemaId = schemaId * int32(fnv1Prime)
	schemaId = schemaId ^ ((fieldId >> 24) & 0xFF)
	schemaId = schemaId * int32(fnv1Prime)
	return schemaId
}

type binaryFieldMeta struct {
	typeId  int32
	fieldId int32
}

type binaryMetadataRegistry interface {
	getMetadata(ctx context.Context, typeId int32) (*binaryMetadata, error)
	getSchema(ctx context.Context, typeId int32, schemaId int32) (*binarySchema, error)
	putMetadata(ctx context.Context, typeId int32, meta *binaryMetadata) error
	clearRegistry()
}

type binaryMetadataRegistryImpl struct {
	cache binaryMetadataRegistry
	cli   *Client
}

func newBinaryMetadataRegistry(cli *Client) binaryMetadataRegistry {
	return &binaryMetadataRegistryImpl{
		cache: newBinaryMetadataCache(),
		cli:   cli,
	}
}

func (r *binaryMetadataRegistryImpl) getMetadata(ctx context.Context, typeId int32) (*binaryMetadata, error) {
	meta, _ := r.cache.getMetadata(ctx, typeId)
	if meta == nil {
		var err error
		meta, err = r.requestAndCacheBinaryMeta(ctx, typeId)
		if err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func (r *binaryMetadataRegistryImpl) getSchema(ctx context.Context, typeId int32, schemaId int32) (*binarySchema, error) {
	schema, _ := r.cache.getSchema(ctx, typeId, schemaId)
	if schema == nil {
		_, err := r.requestAndCacheBinaryMeta(ctx, typeId)
		if err != nil {
			return nil, err
		}
		schema, _ = r.cache.getSchema(ctx, typeId, schemaId)
	}
	return schema, nil
}

func (r *binaryMetadataRegistryImpl) putMetadata(ctx context.Context, typeId int32, meta *binaryMetadata) error {
	old, _ := r.cache.getMetadata(ctx, typeId) // Cache cannot return error
	shouldSent, err := mergeMetadata(old, meta)
	if err != nil {
		return err
	}
	if shouldSent {
		if err = r.sendBinaryMeta(ctx, meta); err != nil {
			return err
		}
	}
	return r.cache.putMetadata(ctx, typeId, meta)
}

func (r *binaryMetadataRegistryImpl) clearRegistry() {
	r.cache.clearRegistry()
}

func (r *binaryMetadataRegistryImpl) requestAndCacheBinaryMeta(ctx context.Context, typeId int32) (*binaryMetadata, error) {
	var binaryMeta *binaryMetadata = nil
	var err error = nil
	r.cli.ch.send(ctx, opGetBinaryType, func(output BinaryOutputStream) error {
		output.WriteInt32(typeId)
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		} else if input.ReadBool() {
			binaryMeta, err = unmarshalBinaryMeta(input)
		}
	})
	if binaryMeta != nil {
		_ = r.cache.putMetadata(ctx, typeId, binaryMeta) // Cache cannot return error
	}
	return binaryMeta, err
}

func (r *binaryMetadataRegistryImpl) sendBinaryMeta(ctx context.Context, meta *binaryMetadata) error {
	if meta == nil {
		return nil
	}
	var err error = nil
	if meta.isEnum {
		panic("enums are not supported")
	}
	r.cli.ch.send(ctx, opPutBinaryType, func(output BinaryOutputStream) error {
		output.WriteInt32(meta.typeId)
		marshalString(output, meta.TypeName())
		// ignite requires null to be written if this field is empty.
		marshalEmptyStringAsNull(output, meta.AffinityKeyName())
		err0 := writeSequence(output, len(meta.fieldsOrder), func(_ BinaryOutputStream, idx int) error {
			fName := meta.fieldsOrder[idx]
			fMeta := meta.fields[fName]
			marshalString(output, fName)
			output.WriteInt32(fMeta.typeId)
			output.WriteInt32(fMeta.fieldId)
			return nil
		})
		if err0 != nil {
			return err0
		}
		output.WriteBool(meta.isEnum) // should be always false
		err0 = writeMap(output, meta.schemas, func(_ BinaryOutputStream, _ int32, schema *binarySchema) error {
			output.WriteInt32(schema.schemaId)
			return writeSequence(output, len(schema.fieldIds), func(_ BinaryOutputStream, idx int) error {
				fieldId := schema.fieldIds[idx]
				output.WriteInt32(fieldId)
				return nil
			})
		})
		return err0
	}, func(_ BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	return err
}

func unmarshalBinaryMeta(reader BinaryInputStream) (*binaryMetadata, error) {
	bMeta := &binaryMetadata{
		typeId: reader.ReadInt32(),
		isEnum: false, // Enums are not supported.
	}
	var err error
	if bMeta.typeName, err = unmarshalString(reader); err != nil {
		return nil, err
	}
	if bMeta.affKeyName, err = unmarshalString(reader); err != nil {
		return nil, err
	}
	bMeta.fieldsOrder = make([]string, 0)
	bMeta.fields, err = readMap[string, binaryFieldMeta](reader, func(BinaryInputStream) (string, binaryFieldMeta, error) {
		val := binaryFieldMeta{}
		key, err0 := unmarshalString(reader)
		if err0 != nil {
			return "", val, err0
		}
		val.typeId = reader.ReadInt32()
		val.fieldId = reader.ReadInt32()
		bMeta.fieldsOrder = append(bMeta.fieldsOrder, key)
		return key, val, nil
	})
	if err != nil {
		return nil, err
	}
	if reader.ReadBool() {
		return nil, fmt.Errorf("enum types are not supported: typeId=%d, typeName=%s", bMeta.typeId, bMeta.typeName)
	}
	bMeta.schemas, err = readMap[int32, *binarySchema](reader, func(BinaryInputStream) (int32, *binarySchema, error) {
		key := reader.ReadInt32()
		val := binarySchema{schemaId: key}
		var err0 error
		val.fieldIds, err0 = readSlice[int32](reader, func(int, BinaryInputStream) (int32, error) {
			return reader.ReadInt32(), nil
		})
		return key, &val, err0
	})
	return bMeta, err
}

type binaryMetadataCache struct {
	mu   sync.Mutex
	data map[int32]*binaryMetadata
}

func newBinaryMetadataCache() binaryMetadataRegistry {
	return &binaryMetadataCache{
		data: make(map[int32]*binaryMetadata),
	}
}

func (r *binaryMetadataCache) getMetadata(_ context.Context, typeId int32) (*binaryMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta := r.data[typeId]
	if meta == nil {
		return nil, nil
	}
	return meta.copy(), nil
}

func (r *binaryMetadataCache) getSchema(_ context.Context, typeId int32, schemaId int32) (*binarySchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta := r.data[typeId]
	if meta == nil {
		return nil, nil
	}
	schema, ok := meta.schemas[schemaId]
	if !ok {
		return nil, nil
	}
	scCpy := schema.copy()
	return &scCpy, nil
}

func (r *binaryMetadataCache) putMetadata(_ context.Context, typeId int32, meta *binaryMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed, err := mergeMetadata(r.data[typeId], meta)
	if err != nil {
		return err
	}
	if changed {
		r.data[typeId] = meta
	}
	return nil
}

func (r *binaryMetadataCache) clearRegistry() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.data {
		delete(r.data, id)
	}
}

func mergeMetadata(oldMeta *binaryMetadata, meta *binaryMetadata) (bool, error) {
	if oldMeta == nil {
		return true, nil
	}
	if oldMeta.typeId != meta.typeId {
		return false, fmt.Errorf("type ids do not match: %d vs %d", oldMeta.typeId, meta.typeId)
	}
	if oldMeta.typeName != meta.typeName {
		return false, fmt.Errorf("types have the same typeId %d: %s vs %s", meta.typeId, oldMeta.typeName, meta.typeName)
	}
	if oldMeta.affKeyName != meta.affKeyName {
		return false, fmt.Errorf("different affinity key fields: %s vs %s", oldMeta.affKeyName, meta.affKeyName)
	}
	schemaChanged := false
	// Copy fields from old schema to merged one's.
	mergedFields := make(map[string]binaryFieldMeta, len(oldMeta.fields))
	mergedFieldsOrder := make([]string, 0, len(oldMeta.fieldsOrder))
	for _, field := range oldMeta.fieldsOrder {
		mergedFieldsOrder = append(mergedFieldsOrder, field)
		mergedFields[field] = oldMeta.fields[field]
	}
	// Check new schema, add new fields if found to merged one's.
	for _, field := range meta.fieldsOrder {
		fieldMeta := meta.fields[field]
		oldFieldMeta, hasOldField := oldMeta.fields[field]
		if hasOldField && oldFieldMeta.typeId != fieldMeta.typeId {
			return false, fmt.Errorf("type %s with typeId %d has different type for field %s: %d vs %d",
				meta.typeName, meta.typeId, field, oldFieldMeta.typeId, fieldMeta.typeId)
		}
		if !hasOldField {
			schemaChanged = true
			mergedFields[field] = fieldMeta
			mergedFieldsOrder = append(mergedFieldsOrder, field)
		}
	}
	// Copy old schemas' ids to ids of merged one's.
	mergedSchemas := make(map[int32]*binarySchema, len(oldMeta.schemas))
	for id, schema := range oldMeta.schemas {
		scCpy := schema.copy()
		mergedSchemas[id] = &scCpy
	}
	// Add new schemas' ids to merged one's.
	for id, schema := range meta.schemas {
		_, hasSchema := oldMeta.schemas[id]
		if !hasSchema {
			schemaChanged = true
			scCpy := schema.copy()
			mergedSchemas[id] = &scCpy
		}
	}
	if schemaChanged {
		*meta = binaryMetadata{
			typeId:      meta.typeId,
			typeName:    meta.typeName,
			affKeyName:  meta.affKeyName,
			fields:      mergedFields,
			fieldsOrder: mergedFieldsOrder,
			schemas:     mergedSchemas,
		}
	}
	return schemaChanged, nil
}
