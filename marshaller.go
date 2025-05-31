package ignite

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/google/uuid"
	"reflect"
	"time"
	"unsafe"
)

type marshaller interface {
	binaryMetadataRegistry
	marshal(ctx context.Context, writer BinaryOutputStream, payload interface{}) error
	unmarshal(ctx context.Context, reader BinaryInputStream) (interface{}, error)
	protocolContext() *ProtocolContext
	isCompactFooter() bool
	binaryIdMapper() BinaryIdMapper
}

type TypeDesc = int8

const (
	objectType TypeDesc = iota - 1
	unregisteredType
	ByteType
	ShortType
	IntType
	LongType
	FloatType
	DoubleType
	CharType
	BoolType
	StringType
	UuidType
	DateType
	ByteArrayType
	ShortArrayType  //lint:ignore U1000 reserved for future
	IntArrayType    //lint:ignore U1000 reserved for future
	LongArrayType   //lint:ignore U1000 reserved for future
	FloatArrayType  //lint:ignore U1000 reserved for future
	DoubleArrayType //lint:ignore U1000 reserved for future
	CharArrayType   //lint:ignore U1000 reserved for future
	BoolArrayType   //lint:ignore U1000 reserved for future
	StringArrayType
	UuidArrayType
	DateArrayType
	ObjectArrayType
	CollectionType
	MapType
	wrappedObjectType  TypeDesc = 27
	DecimalType        TypeDesc = 30
	DecimalArrayType   TypeDesc = 31
	TimestampType      TypeDesc = 33
	TimestampArrayType TypeDesc = 34
	TimeType           TypeDesc = 36
	TimeArrayType      TypeDesc = 37
	NullType           TypeDesc = 101
	handleType         TypeDesc = 102 //lint:ignore U1000 reserved for future
	BinaryObjectType   TypeDesc = 103
)

const (
	opGetBinaryConfiguration int16 = 3004
)

type marshallerImpl struct {
	cli           *Client
	reg           binaryMetadataRegistry
	compactFooter bool
	idMapper      BinaryIdMapper
}

func (m *marshallerImpl) protocolContext() *ProtocolContext {
	return m.cli.ch.protocolContext()
}

func (m *marshallerImpl) isCompactFooter() bool {
	return m.compactFooter
}

func (m *marshallerImpl) binaryIdMapper() BinaryIdMapper {
	return m.idMapper
}

func (m *marshallerImpl) getMetadata(ctx context.Context, typeId int32) (*binaryMetadata, error) {
	return m.reg.getMetadata(ctx, typeId)
}

func (m *marshallerImpl) getSchema(ctx context.Context, typeId int32, schemaId int32) (*binarySchema, error) {
	return m.reg.getSchema(ctx, typeId, schemaId)
}

func (m *marshallerImpl) putMetadata(ctx context.Context, typeId int32, meta *binaryMetadata) error {
	return m.reg.putMetadata(ctx, typeId, meta)
}

func (m *marshallerImpl) clearRegistry() {
	m.reg.clearRegistry()
}

func (m *marshallerImpl) checkBinaryConfiguration(ctx context.Context) error {
	var err error
	pCtx := m.protocolContext()
	if pCtx != nil && pCtx.SupportsAttributeFeature(BinaryConfigurationFeature) {
		var srvCompactFooter bool
		m.cli.ch.send(ctx, opGetBinaryConfiguration, func(output BinaryOutputStream) error {
			return nil
		}, func(input BinaryInputStream, err0 error) {
			if err0 != nil {
				err = err0
				return
			}
			srvCompactFooter = input.ReadBool()
			input.ReadInt8() // Ignored
		})
		if err == nil {
			m.compactFooter = srvCompactFooter
		}
	}
	return err
}

func (m *marshallerImpl) marshal(ctx context.Context, writer BinaryOutputStream, payload interface{}) error {
	if payload == nil {
		writer.WriteNull()
		return nil
	}

	switch val := payload.(type) {
	case bool:
		{
			writer.WriteInt8(BoolType)
			writer.WriteBool(val)
		}
	case uint8:
		{
			writer.WriteInt8(ByteType)
			writer.WriteUInt8(val)
		}
	case int8:
		{
			writer.WriteInt8(ByteType)
			writer.WriteInt8(val)
		}
	case uint16:
		{
			writer.WriteInt8(CharType)
			writer.WriteUInt16(val)
		}
	case int16:
		{
			writer.WriteInt8(ShortType)
			writer.WriteInt16(val)
		}
	case uint32:
		{
			writer.WriteInt8(IntType)
			writer.WriteUInt32(val)
		}
	case int32:
		{
			writer.WriteInt8(IntType)
			writer.WriteInt32(val)
		}
	case uint:
		{
			writer.WriteInt8(LongType)
			writer.WriteUInt64(uint64(val))
		}
	case int:
		{
			writer.WriteInt8(LongType)
			writer.WriteInt64(int64(val))
		}
	case uint64:
		{
			writer.WriteInt8(LongType)
			writer.WriteUInt64(val)
		}
	case int64:
		{
			writer.WriteInt8(LongType)
			writer.WriteInt64(val)
		}
	case float32:
		{
			writer.WriteInt8(FloatType)
			writer.WriteFloat32(val)
		}
	case float64:
		{
			writer.WriteInt8(DoubleType)
			writer.WriteFloat64(val)
		}
	case []bool:
		{
			writer.WriteInt8(BoolArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteBoolSlice(val)
		}
	case []byte:
		{
			marshalByteArray(writer, val)
		}
	case []int8:
		{
			writer.WriteInt8(ByteArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteInt8Slice(val)
		}
	case []uint16:
		{
			writer.WriteInt8(CharArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteUInt16Slice(val)
		}
	case []int16:
		{
			writer.WriteInt8(ShortArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteInt16Slice(val)
		}
	case []uint32:
		{
			writer.WriteInt8(IntArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteUInt32Slice(val)
		}
	case []int32:
		{
			writer.WriteInt8(IntArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteInt32Slice(val)
		}
	case []uint:
		{
			writer.WriteInt8(LongArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteUIntSlice(val)
		}
	case []int:
		{
			writer.WriteInt8(LongArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteIntSlice(val)
		}
	case []uint64:
		{
			writer.WriteInt8(LongArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteUInt64Slice(val)
		}
	case []int64:
		{
			writer.WriteInt8(LongArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteInt64Slice(val)
		}
	case []float32:
		{
			writer.WriteInt8(FloatArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteFloat32Slice(val)
		}
	case []float64:
		{
			writer.WriteInt8(DoubleArrayType)
			writer.WriteInt32(int32(len(val)))
			writer.WriteFloat64Slice(val)
		}
	case string:
		{
			marshalString(writer, val)
		}
	case []string:
		{
			writer.WriteInt8(StringArrayType)
			err := writeIgniteTypedArray(writer, StringType, val, func(output BinaryOutputStream, el *string) error {
				writeString(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []*string:
		{
			writer.WriteInt8(StringArrayType)
			err := writeIgniteTypedArrayP(writer, StringType, val, func(output BinaryOutputStream, el *string) error {
				writeString(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case uuid.UUID:
		{
			marshalUuid(writer, &val)
		}
	case []uuid.UUID:
		{
			writer.WriteInt8(UuidArrayType)
			err := writeIgniteTypedArray(writer, UuidType, val, func(output BinaryOutputStream, el *uuid.UUID) error {
				writeUuid(output, el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []*uuid.UUID:
		{
			writer.WriteInt8(UuidArrayType)
			err := writeIgniteTypedArrayP(writer, UuidType, val, func(output BinaryOutputStream, el *uuid.UUID) error {
				writeUuid(output, el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case Time:
		{
			writer.WriteInt8(TimeType)
			writeTime(writer, val)
		}
	case []Time:
		{
			writer.WriteInt8(TimeArrayType)
			err := writeIgniteTypedArray(writer, TimeType, val, func(output BinaryOutputStream, el *Time) error {
				writeTime(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []*Time:
		{
			writer.WriteInt8(TimeArrayType)
			err := writeIgniteTypedArrayP(writer, TimeType, val, func(output BinaryOutputStream, el *Time) error {
				writeTime(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case Date:
		{
			writer.WriteInt8(DateType)
			writeDate(writer, val)
		}
	case []Date:
		{
			writer.WriteInt8(DateArrayType)
			err := writeIgniteTypedArray(writer, DateType, val, func(output BinaryOutputStream, el *Date) error {
				writeDate(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []*Date:
		{
			writer.WriteInt8(DateArrayType)
			err := writeIgniteTypedArrayP(writer, DateType, val, func(output BinaryOutputStream, el *Date) error {
				writeDate(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case time.Time:
		{
			writer.WriteInt8(TimestampType)
			writeTimestamp(writer, val)
		}
	case []time.Time:
		{
			writer.WriteInt8(TimestampArrayType)
			err := writeIgniteTypedArray(writer, TimestampType, val, func(output BinaryOutputStream, el *time.Time) error {
				writeTimestamp(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []*time.Time:
		{
			writer.WriteInt8(TimestampArrayType)
			err := writeIgniteTypedArrayP(writer, TimestampType, val, func(output BinaryOutputStream, el *time.Time) error {
				writeTimestamp(output, *el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case *apd.Decimal:
		{
			writer.WriteInt8(DecimalType)
			writeDecimal(writer, val)
		}
	case apd.Decimal:
		{
			writer.WriteInt8(DecimalType)
			writeDecimal(writer, &val)
		}
	case []*apd.Decimal:
		{
			writer.WriteInt8(DecimalArrayType)
			err := writeIgniteTypedArrayP(writer, DecimalType, val, func(output BinaryOutputStream, el *apd.Decimal) error {
				writeDecimal(output, el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []apd.Decimal:
		{
			writer.WriteInt8(DecimalArrayType)
			err := writeIgniteTypedArray(writer, DecimalType, val, func(output BinaryOutputStream, el *apd.Decimal) error {
				writeDecimal(output, el)
				return nil
			})
			if err != nil {
				return err
			}
		}
	case []interface{}:
		{
			writer.WriteInt8(ObjectArrayType)
			if err := m.writeInterfaceSlice(ctx, writer, val); err != nil {
				return err
			}
		}
	case Collection:
		{
			writer.WriteInt8(CollectionType)
			if err := m.writeCollection(ctx, writer, val); err != nil {
				return err
			}
		}
	case Map:
		{
			writer.WriteInt8(MapType)
			if err := m.writeMap(ctx, writer, val); err != nil {
				return err
			}
		}
	case BinaryObject:
		{
			writer.WriteBytes(val.Data())
		}
	default:
		switch reflect.ValueOf(val).Kind() {
		case reflect.Map:
			{
				writer.WriteInt8(MapType)
				if err := m.writeGoMap(ctx, writer, reflect.ValueOf(val)); err != nil {
					return err
				}
			}
		case reflect.Slice:
			{
				writer.WriteInt8(ObjectArrayType)
				if err := m.writeReflectSlice(ctx, writer, reflect.ValueOf(val)); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("type '%T' is not supported", val)
		}
	}
	return nil
}

func GetTypeId(val interface{}) (TypeDesc, error) {
	if val == nil {
		return NullType, nil
	}
	switch t := val.(type) {
	case bool:
		{
			return BoolType, nil
		}
	case uint8, int8:
		{
			return ByteType, nil
		}
	case uint16:
		{
			return CharType, nil
		}
	case int16:
		{
			return ShortType, nil
		}
	case uint32, int32:
		{
			return IntType, nil
		}
	case uint64, int64, uint, int:
		{
			return LongType, nil
		}
	case float32:
		{
			return FloatType, nil
		}
	case float64:
		{
			return DoubleType, nil
		}
	case []byte, []int8:
		{
			return ByteArrayType, nil
		}
	case []int16:
		{
			return ShortArrayType, nil
		}
	case []uint16:
		{
			return CharArrayType, nil
		}
	case []uint32, []int32:
		{
			return IntArrayType, nil
		}
	case []uint64, []int64, []uint, []int:
		{
			return LongArrayType, nil
		}
	case []float32:
		{
			return FloatArrayType, nil
		}
	case []float64:
		{
			return DoubleArrayType, nil
		}
	case string:
		{
			return StringType, nil
		}
	case []string, []*string:
		{
			return StringArrayType, nil
		}
	case uuid.UUID:
		{
			return UuidType, nil
		}
	case []uuid.UUID, []*uuid.UUID:
		{
			return UuidArrayType, nil
		}
	case Time:
		{
			return TimeType, nil
		}
	case []Time, []*Time:
		{
			return TimeArrayType, nil
		}
	case Date:
		{
			return DateType, nil
		}
	case []Date, []*Date:
		{
			return DateArrayType, nil
		}
	case time.Time:
		{
			return TimestampType, nil
		}
	case []time.Time, []*time.Time:
		{
			return TimestampArrayType, nil
		}
	case *apd.Decimal, apd.Decimal:
		{
			return DecimalType, nil
		}
	case []*apd.Decimal, []apd.Decimal:
		{
			return DecimalArrayType, nil
		}
	case BinaryObject:
		{
			return BinaryObjectType, nil
		}
	case []interface{}:
		{
			return ObjectArrayType, nil
		}
	case Map:
		{
			return MapType, nil
		}
	case Collection:
		{
			return CollectionType, nil
		}
	default:
		{
			switch reflect.ValueOf(t).Kind() {
			case reflect.Slice:
				return ObjectArrayType, nil
			case reflect.Map:
				return MapType, nil
			default:
				return -1, fmt.Errorf("type '%T' is not supported", t)
			}
		}
	}

}

func (m *marshallerImpl) unmarshal(ctx context.Context, reader BinaryInputStream) (interface{}, error) {
	err := ensureAvailable(reader, byteBytes)
	if err != nil {
		return nil, err
	}
	payloadType := reader.ReadInt8()
	switch payloadType {
	case NullType:
		{
			return nil, nil
		}
	case BoolType:
		{
			err = ensureAvailable(reader, boolBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadBool(), nil
		}
	case ByteType:
		{
			err = ensureAvailable(reader, byteBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadInt8(), nil
		}
	case ShortType:
		{
			err = ensureAvailable(reader, shortBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadInt16(), nil
		}
	case CharType:
		{
			err = ensureAvailable(reader, charBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadUInt16(), nil
		}
	case IntType:
		{
			err = ensureAvailable(reader, intBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadInt32(), nil
		}
	case LongType:
		{
			err = ensureAvailable(reader, longBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadInt64(), nil
		}
	case FloatType:
		{
			err = ensureAvailable(reader, intBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadFloat32(), nil

		}
	case DoubleType:
		{
			err = ensureAvailable(reader, longBytes)
			if err != nil {
				return nil, err
			}
			return reader.ReadFloat64(), nil
		}
	case ByteArrayType:
		{
			return readByteArray(reader)
		}
	case BoolArrayType, ShortArrayType, CharArrayType, IntArrayType, LongArrayType, FloatArrayType, DoubleArrayType:
		{
			var elemSz int
			switch payloadType {
			case BoolArrayType:
				elemSz = boolBytes
			case ShortArrayType:
				elemSz = shortBytes
			case CharArrayType:
				elemSz = charBytes
			case IntArrayType, FloatArrayType:
				elemSz = intBytes
			case LongArrayType, DoubleArrayType:
				elemSz = longBytes
			default:
				panic("impossible condition")
			}
			err = ensureAvailable(reader, intBytes)
			if err != nil {
				return nil, err
			}
			sz := int(reader.ReadInt32())
			err = ensureAvailable(reader, sz*elemSz)
			if err != nil {
				return nil, err
			}
			switch payloadType {
			case BoolArrayType:
				return reader.ReadBoolSlice(sz), nil
			case ShortArrayType:
				return reader.ReadInt16Slice(sz), nil
			case CharArrayType:
				return reader.ReadUInt16Slice(sz), nil
			case IntArrayType:
				return reader.ReadInt32Slice(sz), nil
			case LongArrayType:
				return reader.ReadInt64Slice(sz), nil
			case FloatArrayType:
				return reader.ReadFloat32Slice(sz), nil
			case DoubleArrayType:
				return reader.ReadFloat64Slice(sz), nil
			default:
				panic("impossible condition")
			}
		}
	case StringType:
		{
			return readString(reader)
		}
	case StringArrayType:
		{
			return readIgniteTypedArray[string](reader, StringType, func(_ int, reader BinaryInputStream) (*string, error) {
				ret, err := readString(reader)
				if err != nil {
					return nil, err
				}
				return &ret, err
			})
		}
	case UuidType:
		{
			return readUuid(reader)
		}
	case UuidArrayType:
		{
			return readIgniteTypedArray[uuid.UUID](reader, UuidType, func(_ int, reader BinaryInputStream) (*uuid.UUID, error) {
				ret, err := readUuid(reader)
				if err != nil {
					return nil, err
				}
				return &ret, err
			})
		}
	case TimeType:
		{
			return readTime(reader)
		}
	case TimeArrayType:
		{
			return readIgniteTypedArray[Time](reader, TimeType, func(_ int, reader BinaryInputStream) (*Time, error) {
				ret, err := readTime(reader)
				if err != nil {
					return nil, err
				}
				return &ret, err
			})
		}
	case DateType:
		{
			return readDate(reader)
		}
	case DateArrayType:
		{
			return readIgniteTypedArray[Date](reader, DateType, func(_ int, reader BinaryInputStream) (*Date, error) {
				ret, err := readDate(reader)
				if err != nil {
					return nil, err
				}
				return &ret, err
			})
		}
	case TimestampType:
		{
			return readTimestamp(reader)
		}
	case TimestampArrayType:
		{
			return readIgniteTypedArray[time.Time](reader, TimestampType, func(_ int, reader BinaryInputStream) (*time.Time, error) {
				ret, err := readTimestamp(reader)
				if err != nil {
					return nil, err
				}
				return &ret, err
			})
		}
	case DecimalType:
		{
			return readDecimal(reader)
		}
	case DecimalArrayType:
		{
			return readIgniteTypedArray[apd.Decimal](reader, DecimalType, func(_ int, reader BinaryInputStream) (*apd.Decimal, error) {
				return readDecimal(reader)
			})
		}
	case ObjectArrayType:
		{
			return m.readIgniteObjectArray(ctx, reader)
		}
	case CollectionType:
		{
			return m.readIgniteCollection(ctx, reader)
		}
	case MapType:
		{
			return m.readIgniteMap(ctx, reader)
		}
	case BinaryObjectType:
		{
			return m.readBinaryObject(reader, false)
		}
	case wrappedObjectType:
		{
			return m.readBinaryObject(reader, true)
		}
	default:
		return nil, fmt.Errorf("type %d is not supported", payloadType)
	}
}

func (m *marshallerImpl) readBinaryObject(reader BinaryInputStream, wrapped bool) (BinaryObject, error) {
	var err error
	var data []byte
	if wrapped {
		err = ensureAvailable(reader, intBytes)
		if err != nil {
			return nil, err
		}
		sz := int(reader.ReadInt32())
		err = ensureAvailable(reader, sz+intBytes)
		if err != nil {
			return nil, err
		}
		data = reader.ReadBytes(sz)
		offset := reader.ReadInt32()
		data = data[offset:]
	} else {
		if err = ensureAvailable(reader, headerLength); err != nil {
			return nil, err
		}
		startPos := reader.Position() - 1 // First byte of header has been already read
		reader.SetPosition(startPos + lenPos)
		sz := int(reader.ReadInt32())
		reader.SetPosition(startPos)
		if err = ensureAvailable(reader, sz); err != nil {
			return nil, err
		}
		data = reader.ReadBytes(sz)
	}
	return &binaryObjectImpl{
		data:  data,
		marsh: m,
	}, nil
}

func (m *marshallerImpl) readIgniteObjectArray(ctx context.Context, reader BinaryInputStream) ([]interface{}, error) {
	if reader.ReadInt32() == int32(unregisteredType) {
		if _, err := unmarshalString(reader); err != nil {
			return nil, err
		}
	}
	return readSlice(reader, func(_ int, reader BinaryInputStream) (interface{}, error) {
		return m.unmarshal(ctx, reader)
	})
}

func (m *marshallerImpl) readIgniteCollection(ctx context.Context, reader BinaryInputStream) (Collection, error) {
	var err error
	ret := Collection{}
	ret.values, err = readSlice(reader, func(idx int, reader0 BinaryInputStream) (interface{}, error) {
		if idx == 0 {
			ret.kind = reader0.ReadInt8()
		}
		return m.unmarshal(ctx, reader0)
	})
	return ret, err
}

func (m *marshallerImpl) readIgniteMap(ctx context.Context, reader BinaryInputStream) (Map, error) {
	var err error
	ret := Map{}
	ret.entries, err = readSlice(reader, func(idx int, reader0 BinaryInputStream) (KeyValue, error) {
		if idx == 0 {
			ret.kind = reader0.ReadInt8()
		}
		kv := KeyValue{}
		var err0 error
		if kv.Key, err0 = m.unmarshal(ctx, reader0); err0 != nil {
			return kv, err0
		}
		if kv.Value, err0 = m.unmarshal(ctx, reader0); err0 != nil {
			return kv, err0
		}
		return kv, err0
	})
	return ret, err
}

func (m *marshallerImpl) writeInterfaceSlice(ctx context.Context, writer BinaryOutputStream, objArr []interface{}) error {
	writer.WriteInt32(int32(objectType))
	return writeSequence(writer, len(objArr), func(output BinaryOutputStream, idx int) error {
		if err := m.marshal(ctx, output, objArr[idx]); err != nil {
			return err
		}
		return nil
	})
}

func (m *marshallerImpl) writeReflectSlice(ctx context.Context, writer BinaryOutputStream, objArr reflect.Value) error {
	writer.WriteInt32(int32(objectType))
	return writeSequence(writer, objArr.Len(), func(output BinaryOutputStream, idx int) error {
		value := objArr.Index(idx).Interface()
		return m.marshal(ctx, writer, value)
	})
}

func (m *marshallerImpl) writeCollection(ctx context.Context, writer BinaryOutputStream, collection Collection) error {
	values := collection.Values()
	return writeSequence(writer, len(values), func(output BinaryOutputStream, idx int) error {
		if idx == 0 {
			output.WriteInt8(collection.Kind())
		}
		if err := m.marshal(ctx, output, values[idx]); err != nil {
			return err
		}
		return nil
	})
}

func (m *marshallerImpl) writeMap(ctx context.Context, writer BinaryOutputStream, collection Map) error {
	entries := collection.Entries()
	return writeSequence(writer, len(entries), func(output BinaryOutputStream, idx int) error {
		if idx == 0 {
			output.WriteInt8(collection.Kind())
		}
		entry := entries[idx]
		if err := m.marshal(ctx, output, entry.Key); err != nil {
			return err
		}
		if err := m.marshal(ctx, output, entry.Value); err != nil {
			return err
		}
		return nil
	})
}

func (m *marshallerImpl) writeGoMap(ctx context.Context, writer BinaryOutputStream, collection reflect.Value) error {
	keys := collection.MapKeys()
	return writeSequence(writer, len(keys), func(output BinaryOutputStream, idx int) error {
		if idx == 0 {
			output.WriteInt8(HashMap)
		}
		key := keys[idx]
		if err := m.marshal(ctx, output, key.Interface()); err != nil {
			return err
		}
		if err := m.marshal(ctx, output, collection.MapIndex(key).Interface()); err != nil {
			return err
		}
		return nil
	})
}

func marshalString(writer BinaryOutputStream, val string) {
	writer.WriteInt8(StringType)
	writeString(writer, val)
}

func writeString(writer BinaryOutputStream, val string) {
	bytes := []byte(val)
	writer.WriteInt32(int32(len(bytes)))
	writer.WriteBytes(bytes)
}

func marshalEmptyStringAsNull(writer BinaryOutputStream, val string) {
	if len(val) == 0 {
		writer.WriteNull()
	} else {
		marshalString(writer, val)
	}
}

func marshalByteArray(writer BinaryOutputStream, val []byte) {
	if val == nil {
		writer.WriteNull()
		return
	}
	writer.WriteInt8(ByteArrayType)
	writer.WriteInt32(int32(len(val)))
	writer.WriteBytes(val)
}

func marshalUuid(writer BinaryOutputStream, val *uuid.UUID) {
	if val == nil {
		writer.WriteNull()
	} else {
		writer.WriteInt8(UuidType)
		writer.WriteUInt64(binary.BigEndian.Uint64(val[:8]))
		writer.WriteUInt64(binary.BigEndian.Uint64(val[8:]))
	}
}

func writeUuid(writer BinaryOutputStream, val *uuid.UUID) {
	writer.WriteUInt64(binary.BigEndian.Uint64(val[:8]))
	writer.WriteUInt64(binary.BigEndian.Uint64(val[8:]))
}

func writeDate(writer BinaryOutputStream, val Date) {
	writer.WriteInt64(int64(val))
}

func writeTime(writer BinaryOutputStream, val Time) {
	writer.WriteInt64(int64(val))
}

func writeTimestamp(writer BinaryOutputStream, val time.Time) {
	millis := val.Unix() * 1000
	nanos := val.Nanosecond()
	millis += int64(nanos / int(time.Millisecond))
	nanos %= int(time.Millisecond)
	writer.WriteInt64(millis)
	writer.WriteInt32(int32(nanos))
}

func writeDecimal(writer BinaryOutputStream, val *apd.Decimal) {
	writer.WriteInt32(-val.Exponent)
	coeff := val.Coeff.Bytes()
	if len(coeff) == 0 || coeff[0] > 0x7F {
		tmp := make([]byte, len(coeff)+1)
		copy(tmp[1:], coeff)
		coeff = tmp
	}
	if val.Negative {
		coeff[0] |= 0x80
	}
	writer.WriteInt32(int32(len(coeff)))
	writer.WriteBytes(coeff)
}

func checkNotNull(reader BinaryInputStream, expected TypeDesc) (bool, error) {
	var err error
	if err = ensureAvailable(reader, byteBytes); err != nil {
		return false, err
	}
	t := reader.ReadInt8()
	switch t {
	case NullType:
		{
			return false, nil
		}
	case expected:
		{
			return true, nil
		}
	default:
		{
			return false, fmt.Errorf("unexpected type %d in stream", t)
		}
	}
}

func readByteArray(reader BinaryInputStream) ([]byte, error) {
	if err := ensureAvailable(reader, intBytes); err != nil {
		return nil, err
	}
	bytesSz := int(reader.ReadInt32())
	if err := ensureAvailable(reader, bytesSz); err != nil {
		return nil, err
	}
	return reader.ReadBytes(bytesSz), nil
}

func unmarshalByteArray(reader BinaryInputStream) ([]byte, error) {
	isNotNull, err := checkNotNull(reader, ByteArrayType)
	if err != nil || !isNotNull {
		return nil, err
	}
	return readByteArray(reader)
}

func readString(reader BinaryInputStream) (string, error) {
	if err := ensureAvailable(reader, intBytes); err != nil {
		return "", err
	}
	strSz := int(reader.ReadInt32())
	if err := ensureAvailable(reader, strSz); err != nil {
		return "", err
	}
	return bytesToString(reader.ReadBytes(strSz)), nil
}

func unmarshalString(reader BinaryInputStream) (string, error) {
	isNotNull, err := checkNotNull(reader, StringType)
	if err != nil || !isNotNull {
		return "", err
	}
	return readString(reader)
}

func readUuid(reader BinaryInputStream) (uuid.UUID, error) {
	if err := ensureAvailable(reader, 2*longBytes); err != nil {
		return uuid.Nil, err
	}
	ret := uuid.UUID{}
	binary.BigEndian.PutUint64(ret[:8], reader.ReadUInt64())
	binary.BigEndian.PutUint64(ret[8:], reader.ReadUInt64())
	return ret, nil
}

func unmarshalUuid(reader BinaryInputStream) (uuid.UUID, error) {
	isNotNull, err := checkNotNull(reader, UuidType)
	if err != nil || !isNotNull {
		return uuid.Nil, err
	}
	return readUuid(reader)
}

func readTime(reader BinaryInputStream) (Time, error) {
	if err := ensureAvailable(reader, longBytes); err != nil {
		return 0, err
	}
	return Time(reader.ReadInt64()), nil
}

func readDate(reader BinaryInputStream) (Date, error) {
	if err := ensureAvailable(reader, longBytes); err != nil {
		return 0, err
	}
	return Date(reader.ReadInt64()), nil
}

func readTimestamp(reader BinaryInputStream) (time.Time, error) {
	if err := ensureAvailable(reader, intBytes+longBytes); err != nil {
		return time.Time{}, err
	}
	millis := reader.ReadInt64()
	nanos := reader.ReadInt32() + int32((millis%1000)*int64(time.Millisecond))
	return time.Unix(millis/1000, int64(nanos)), nil
}

func readDecimal(reader BinaryInputStream) (*apd.Decimal, error) {
	if err := ensureAvailable(reader, intBytes); err != nil {
		return nil, err
	}
	exp := -reader.ReadInt32()
	coefSz := int(reader.ReadInt32())
	if coefSz == 0 {
		return nil, errors.New("invalid coef value: empty")
	}
	coefData := reader.ReadBytes(coefSz)
	negative := coefData[0]&0x80 == 0x80
	if negative {
		coefData[0] &= 0x7F
	}
	coef := new(apd.BigInt)
	coef.SetBytes(coefData)
	if negative {
		coef.Neg(coef)
	}
	return apd.NewWithBigInt(coef, exp), nil
}

func ensureAvailable(reader BinaryInputStream, nBytes int) error {
	if reader.Available() < nBytes {
		return fmt.Errorf("invalid binary stream")
	}
	return nil
}

func bytesToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func readIgniteTypedArray[T any](reader BinaryInputStream, elType TypeDesc, elemReader func(idx int, reader BinaryInputStream) (*T, error)) ([]*T, error) {
	return readSlice(reader, func(idx int, input BinaryInputStream) (*T, error) {
		isNotNull, err := checkNotNull(input, elType)
		if err != nil || !isNotNull {
			return nil, err
		}
		return elemReader(idx, input)
	})
}

func writeIgniteTypedArrayP[T any](writer BinaryOutputStream, elType TypeDesc, arr []*T, elemWriter func(writer BinaryOutputStream, el *T) error) error {
	return writeSequence(writer, len(arr), func(output BinaryOutputStream, idx int) error {
		el := arr[idx]
		if el == nil {
			writer.WriteNull()
		} else {
			writer.WriteInt8(elType)
			return elemWriter(writer, el)
		}
		return nil
	})
}

func writeIgniteTypedArray[T any](writer BinaryOutputStream, elType TypeDesc, arr []T, elemWriter func(writer BinaryOutputStream, el *T) error) error {
	return writeSequence(writer, len(arr), func(output BinaryOutputStream, idx int) error {
		el := arr[idx]
		writer.WriteInt8(elType)
		return elemWriter(writer, &el)
	})
}
