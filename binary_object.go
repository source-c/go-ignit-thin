package ignite

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/source-c/go-ignit-thin/internal"
	"strings"
)

const (
	protoVersion       int8   = 1
	flagsPos                  = 2
	typeIdPos                 = 4
	hashCodePos               = 8
	lenPos                    = 12
	schemaIdPos               = 16
	schemaOffsetPos           = 20
	headerLength              = 24
	flagUserType       uint16 = 0x0001
	flagHasSchema      uint16 = 0x0002
	flagHasRaw         uint16 = 0x0004
	flagOffsetOneByte  uint16 = 0x0008
	flagOffsetTwoBytes uint16 = 0x0010
	flagCompactFooter  uint16 = 0x0020
)

// BinaryObject is a wrapper around byte slice that contains serialized data in Apache Ignite binary format.
type BinaryObject interface {
	// Type returns binary object metadata
	Type(ctx context.Context) (BinaryType, error)
	// Field returns value of field with specified name.
	Field(ctx context.Context, name string) (interface{}, error)
	// Size returns the underlying byte slice size.
	Size() int
	// Data returns the underlying byte slice
	Data() []byte
	// HashCode returns hash code according to ApacheIgnite binary format specification.
	HashCode() int32
}

// FieldGetter is a helper for easy retrieving field data from BinaryObject.
type FieldGetter[T any] struct {
	obj     BinaryObject
	fldName string
}

// NewFieldGetter creates [FieldGetter] for specified [BinaryObject] and field.
func NewFieldGetter[T any](obj BinaryObject, fldName string) *FieldGetter[T] {
	return &FieldGetter[T]{obj, fldName}
}

// Get retrieves field value and set it to specified pointer.
func (getter *FieldGetter[T]) Get(ctx context.Context, in *T) error {
	val, err := getter.obj.Field(ctx, getter.fldName)
	if err != nil {
		return err
	}
	val0, ok := val.(T)
	if !ok {
		return fmt.Errorf("cannot cast %v to %T", val, &in)
	}
	*in = val0
	return nil
}

type BinaryIdMapper interface {
	TypeId(typeName string) int32
	FieldId(typeId int32, fldName string) int32
}

type BinaryBasicIdMapper struct {
}

func (b BinaryBasicIdMapper) TypeId(typeName string) int32 {
	return internal.HashCode(strings.ToLower(typeName))
}

func (b BinaryBasicIdMapper) FieldId(_ int32, fldName string) int32 {
	return internal.HashCode(fldName)
}

type binaryObjectImpl struct {
	typeId int32
	data   []byte
	marsh  marshaller
}

func (b *binaryObjectImpl) Type(ctx context.Context) (BinaryType, error) {
	typeId, err := b.getTypeId()
	if err != nil {
		return nil, err
	}
	meta, err := b.marsh.getMetadata(ctx, typeId)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, fmt.Errorf("no metadata found for typeId=%d", typeId)
	}
	return meta, nil
}

func (b *binaryObjectImpl) Field(ctx context.Context, fldName string) (interface{}, error) {
	flags := b.flags()
	if flags&flagHasSchema != flagHasSchema {
		return nil, nil
	}

	bIdMapper := b.marsh.binaryIdMapper()
	typeId, err := b.getTypeId()
	if err != nil {
		return nil, err
	}
	schemaId := b.schemaId()
	fieldId := bIdMapper.FieldId(typeId, fldName)

	isCompactFooter := flags&flagCompactFooter == flagCompactFooter
	footer, fldSize := b.footer()
	var fldOffsetIdx int
	if isCompactFooter {
		schema, err := b.marsh.getSchema(ctx, typeId, schemaId)
		if err != nil {
			return nil, err
		}
		if schema == nil {
			return nil, fmt.Errorf("schema not fount for typeId=%d and schemaId=%d", typeId, schemaId)
		}
		fieldIdx := -1
		for idx, id := range schema.fieldIds {
			if id == fieldId {
				fieldIdx = idx
			}
		}
		if fieldIdx == -1 {
			return nil, nil
		}
		fldOffsetIdx = fieldIdx * fldSize
	} else {
		fldOffsetIdx = -1
		for pos := 0; pos < len(footer); {
			id := int32(binary.LittleEndian.Uint32(footer[pos : pos+intBytes]))
			if id == fieldId {
				fldOffsetIdx = pos + 4
				break
			}
			pos += 4 + fldSize
		}
		if fldOffsetIdx == -1 {
			return nil, nil
		}
	}
	var fldOffset int
	if fldSize == 1 {
		fldOffset = int(footer[fldOffsetIdx])
	} else if fldSize == 2 {
		fldOffset = int(binary.LittleEndian.Uint16(footer[fldOffsetIdx : fldOffsetIdx+shortBytes]))
	} else {
		fldOffset = int(binary.LittleEndian.Uint32(footer[fldOffsetIdx : fldOffsetIdx+intBytes]))
	}
	return b.marsh.unmarshal(ctx, NewBinaryInputStream(b.data, fldOffset))
}

func (b *binaryObjectImpl) Size() int {
	return len(b.data)
}

func (b *binaryObjectImpl) Data() []byte {
	return b.data
}

func (b *binaryObjectImpl) getTypeId() (int32, error) {
	if len(b.data) < headerLength {
		return 0, fmt.Errorf("invalid binary object, data is corrupted")
	}
	typeId := b.typeId
	if typeId != 0 {
		return typeId, nil
	}
	typeId = b.getRawTypeId()
	if typeId == int32(unregisteredType) {
		clsName, err := unmarshalString(NewBinaryInputStream(b.data, headerLength))
		if err != nil {
			return 0, fmt.Errorf("invalid binary object, data is corrupted: %w", err)
		}
		typeId = b.marsh.binaryIdMapper().TypeId(clsName)
	}
	b.typeId = typeId
	return typeId, nil
}

func (b *binaryObjectImpl) getRawTypeId() int32 {
	if len(b.data) < headerLength {
		panic("impossible condition")
	}
	return int32(binary.LittleEndian.Uint32(b.data[typeIdPos : typeIdPos+intBytes]))
}

func (b *binaryObjectImpl) schemaId() int32 {
	if len(b.data) < headerLength {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(b.data[schemaIdPos : schemaIdPos+intBytes]))
}

func (b *binaryObjectImpl) footer() ([]byte, int) {
	if len(b.data) < headerLength {
		return nil, 0
	}
	flags := b.flags()
	if flags&flagHasSchema != flagHasSchema {
		return nil, 0
	}
	var fldSize int
	if flags&flagOffsetOneByte == flagOffsetOneByte {
		fldSize = 1
	} else if flags&flagOffsetTwoBytes == flagOffsetTwoBytes {
		fldSize = 2
	} else {
		fldSize = 4
	}
	schemaStart := int(binary.LittleEndian.Uint32(b.data[schemaOffsetPos : schemaOffsetPos+intBytes]))
	schemaLen := len(b.data) - schemaStart
	if flags&flagHasRaw == flagHasRaw {
		schemaLen -= 4
	}
	return b.data[schemaStart : schemaStart+schemaLen], fldSize
}

func (b *binaryObjectImpl) flags() uint16 {
	if len(b.data) < headerLength {
		return 0
	}
	return binary.LittleEndian.Uint16(b.data[flagsPos : flagsPos+shortBytes])
}

func (b *binaryObjectImpl) HashCode() int32 {
	if len(b.data) < headerLength {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(b.data[hashCodePos : hashCodePos+intBytes]))
}

type boField struct {
	typeId int32
	value  interface{}
}

type binaryObjectOptions struct {
	typeName     string
	affKeyName   string
	fields       map[string]*boField
	fieldsOrder  []string
	isRegistered bool // should be true by default, only for testing
}

func newBinaryObject(ctx context.Context, marsh marshaller, opts *binaryObjectOptions) (BinaryObject, error) {
	bIdMapper := marsh.binaryIdMapper()
	typeId := bIdMapper.TypeId(opts.typeName)

	schemaBuilder := newBinarySchemaBuilder()

	oldMeta, err := marsh.getMetadata(ctx, typeId)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	if oldMeta != nil {
		if opts.typeName != oldMeta.typeName {
			return nil, fmt.Errorf("types have the same typeId %d: old %s vs %s", typeId, oldMeta.typeName,
				opts.typeName)
		}
		if opts.affKeyName != oldMeta.affKeyName {
			return nil, fmt.Errorf("type %s with typeId %d has different affinity key field: %s vs %s",
				opts.typeName, typeId, oldMeta.affKeyName, opts.affKeyName)
		}
	}
	binaryMeta := newBinaryMetadata(typeId, opts.typeName, opts.affKeyName)

	outStream := NewBinaryOutputStream(headerLength)
	startPos := outStream.Position()
	outStream.SetPosition(headerLength)

	flags := flagUserType
	if marsh.isCompactFooter() {
		flags |= flagCompactFooter
	}

	var offset int
	if !opts.isRegistered {
		marshalString(outStream, opts.typeName)
		offset = outStream.Position()
	} else {
		offset = headerLength
	}
	if len(opts.fields) > 0 {
		flags |= flagHasSchema
		for _, fldName := range opts.fieldsOrder {
			field := opts.fields[fldName]
			fieldId := bIdMapper.FieldId(typeId, fldName)
			// set old field metadata if exists
			var oldFieldMeta *binaryFieldMeta = nil
			if oldMeta != nil {
				if fMeta, ok := oldMeta.fields[fldName]; ok {
					oldFieldMeta = &fMeta
				}
			}
			// set field type if not have been set already
			if field.typeId < 0 {
				if field.value != nil {
					fldTypeId, err := GetTypeId(field.value)
					if err != nil {
						return nil, fmt.Errorf("failed to get type id for field %s: %w", fldName, err)
					}
					field.typeId = int32(fldTypeId)
				} else if oldFieldMeta != nil {
					field.typeId = oldFieldMeta.typeId
				} else {
					field.typeId = int32(BinaryObjectType)
				}
			}
			// check type compliance for oldMeta
			if oldFieldMeta != nil && field.typeId != oldFieldMeta.typeId {
				return nil, fmt.Errorf("type %s with typeId %d has different type for field %s: old %d vs %d",
					opts.typeName, typeId, fldName, oldFieldMeta.typeId, typeId)
			}
			// add new field to meta if oldFieldMeta is nil
			if oldFieldMeta == nil {
				binaryMeta.addField(fldName, field.typeId, fieldId)
			}
			schemaBuilder.AddField(fieldId, int32(outStream.Position()-startPos))
			if err = marsh.marshal(ctx, outStream, field.value); err != nil {
				return nil, fmt.Errorf("failed to marshall field %s: %w", fldName, err)
			}
		}
		offset = outStream.Position() - startPos
		fldSize := schemaBuilder.WriteFooter(outStream, marsh.isCompactFooter())
		if fldSize == 1 {
			flags |= flagOffsetOneByte
		} else if fldSize == 2 {
			flags |= flagOffsetTwoBytes
		}
	}
	// Write header
	schema := schemaBuilder.Build()
	retPos := outStream.Position()
	outStream.SetPosition(startPos)
	outStream.WriteInt8(BinaryObjectType)
	outStream.WriteInt8(protoVersion)
	outStream.WriteUInt16(flags)
	if !opts.isRegistered {
		outStream.WriteInt32(int32(unregisteredType))
	} else {
		outStream.WriteInt32(typeId)
	}
	outStream.SetPosition(startPos + lenPos)
	outStream.WriteInt32(int32(retPos - startPos))
	outStream.WriteInt32(schema.schemaId)
	outStream.WriteInt32(int32(offset))
	// Add schema to registry
	binaryMeta.addSchema(schema)
	err = marsh.putMetadata(ctx, typeId, binaryMeta)
	if err != nil {
		return nil, err
	}
	// Calculate and write hashcode
	hashCode := outStream.HashCode(startPos+headerLength, startPos+offset)
	outStream.SetPosition(startPos + hashCodePos)
	outStream.WriteInt32(hashCode)
	// Restore final stream position
	outStream.SetPosition(retPos)
	return &binaryObjectImpl{
		data:  outStream.Data(),
		marsh: marsh,
	}, nil
}
