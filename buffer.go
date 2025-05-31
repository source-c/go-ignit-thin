package ignite

import (
	"encoding/binary"
	"fmt"
	"github.com/source-c/go-ignit-thin/internal"
	"math"
	"unsafe"
)

const (
	boolBytes  = 1
	byteBytes  = 1
	shortBytes = 2
	charBytes  = 2
	intBytes   = 4
	longBytes  = 8
)

type BinaryOutputStream interface {
	Data() []byte
	Position() int
	Available() int
	SetPosition(pos int)
	WriteNull()
	WriteBool(v bool)
	WriteUInt8(v uint8)
	WriteInt8(v int8)
	WriteUInt16(v uint16)
	WriteInt16(v int16)
	WriteUInt32(v uint32)
	WriteInt32(v int32)
	WriteUInt64(v uint64)
	WriteInt64(v int64)
	WriteFloat32(v float32)
	WriteFloat64(v float64)
	WriteBytes(v []byte)
	WriteInt8Slice(val []int8)
	WriteBoolSlice(val []bool)
	WriteUInt16Slice(v []uint16)
	WriteInt16Slice(v []int16)
	WriteUInt32Slice(v []uint32)
	WriteInt32Slice(v []int32)
	WriteUIntSlice(v []uint)
	WriteIntSlice(v []int)
	WriteUInt64Slice(v []uint64)
	WriteInt64Slice(v []int64)
	WriteFloat32Slice(val []float32)
	WriteFloat64Slice(val []float64)
	HashCode(start int, end int) int32
}

type BinaryInputStream interface {
	Position() int
	Available() int
	SetPosition(pos int)
	ReadBool() bool
	ReadUInt8() uint8
	ReadInt8() int8
	ReadUInt16() uint16
	ReadInt16() int16
	ReadUInt32() uint32
	ReadInt32() int32
	ReadUInt64() uint64
	ReadInt64() int64
	ReadFloat32() float32
	ReadFloat64() float64
	ReadBytes(size int) []byte
	ReadBoolSlice(size int) []bool
	ReadInt8Slice(size int) []int8
	ReadUInt16Slice(size int) []uint16
	ReadInt16Slice(size int) []int16
	ReadUInt32Slice(size int) []uint32
	ReadInt32Slice(size int) []int32
	ReadUInt64Slice(size int) []uint64
	ReadInt64Slice(size int) []int64
	ReadFloat32Slice(size int) []float32
	ReadFloat64Slice(size int) []float64
	IsNull() bool
}

type binaryOutputStreamImpl struct {
	buffer   []byte
	position int
}

type binaryInputStreamImpl struct {
	buffer   []byte
	offset   int
	position int
}

var isLittleEndian = getNativeEndian()

// returns true if little, false if big
func getNativeEndian() bool {
	return binary.LittleEndian.Uint16([]byte{0x12, 0x34}) == uint16(0x3412)
}

func NewBinaryOutputStream(length int) BinaryOutputStream {
	return &binaryOutputStreamImpl{
		buffer: make([]byte, length),
	}
}

func NewBinaryInputStream(buffer []byte, offset int) BinaryInputStream {
	return &binaryInputStreamImpl{
		buffer:   buffer,
		offset:   offset,
		position: offset,
	}
}

func (bw *binaryOutputStreamImpl) Data() []byte {
	return bw.buffer[:bw.position]
}

func (bw *binaryOutputStreamImpl) Available() int {
	return len(bw.buffer) - bw.position
}

func (bw *binaryOutputStreamImpl) Position() int {
	return bw.position
}

func (bw *binaryOutputStreamImpl) SetPosition(pos int) {
	bw.position = pos
}

func (bw *binaryOutputStreamImpl) ensureAvailable(size int) {
	if math.MaxInt32-bw.position < size {
		panic(fmt.Sprintf("Buffer length overflow: position=%d, required size=%d", bw.position, size))
	}
	if bw.Available() < size {
		temp := make([]byte, bw.position+size)
		copy(temp, bw.buffer)
		bw.buffer = temp
	}
}

func (bw *binaryOutputStreamImpl) WriteNull() {
	bw.WriteInt8(NullType)
}

func (bw *binaryOutputStreamImpl) WriteBool(v bool) {
	bw.ensureAvailable(boolBytes)
	bw.writeBool(v)
}

func (bw *binaryOutputStreamImpl) writeBool(v bool) {
	if v {
		bw.buffer[bw.position] = 1
	} else {
		bw.buffer[bw.position] = 0
	}
	bw.position += boolBytes
}

func (bw *binaryOutputStreamImpl) WriteUInt8(v uint8) {
	bw.ensureAvailable(byteBytes)
	bw.writeByte(v)
}

func (bw *binaryOutputStreamImpl) WriteInt8(v int8) {
	bw.ensureAvailable(byteBytes)
	bw.writeByte(uint8(v))
}

func (bw *binaryOutputStreamImpl) writeByte(v byte) {
	bw.buffer[bw.position] = v
	bw.position += byteBytes
}

func (bw *binaryOutputStreamImpl) WriteInt16(v int16) {
	bw.ensureAvailable(shortBytes)
	bw.writeShort(uint16(v))
}

func (bw *binaryOutputStreamImpl) WriteUInt16(v uint16) {
	bw.ensureAvailable(shortBytes)
	bw.writeShort(v)
}

func (bw *binaryOutputStreamImpl) writeShort(v uint16) {
	binary.LittleEndian.PutUint16(bw.buffer[bw.position:], v)
	bw.position += shortBytes
}

func (bw *binaryOutputStreamImpl) WriteInt32(v int32) {
	bw.ensureAvailable(intBytes)
	bw.writeInt(uint32(v))
}

func (bw *binaryOutputStreamImpl) WriteUInt32(v uint32) {
	bw.ensureAvailable(intBytes)
	bw.writeInt(v)
}

func (bw *binaryOutputStreamImpl) writeInt(v uint32) {
	binary.LittleEndian.PutUint32(bw.buffer[bw.position:], v)
	bw.position += intBytes
}

func (bw *binaryOutputStreamImpl) WriteUInt64(v uint64) {
	bw.ensureAvailable(longBytes)
	bw.writeLong(v)
}

func (bw *binaryOutputStreamImpl) WriteInt64(v int64) {
	bw.ensureAvailable(longBytes)
	bw.writeLong(uint64(v))
}

func (bw *binaryOutputStreamImpl) writeLong(v uint64) {
	binary.LittleEndian.PutUint64(bw.buffer[bw.position:], v)
	bw.position += longBytes
}

func (bw *binaryOutputStreamImpl) WriteFloat32(v float32) {
	bw.WriteUInt32(math.Float32bits(v))
}

func (bw *binaryOutputStreamImpl) WriteFloat64(v float64) {
	bw.WriteUInt64(math.Float64bits(v))
}

func (bw *binaryOutputStreamImpl) WriteBytes(v []byte) {
	length := len(v)
	bw.ensureAvailable(length)
	copy(bw.buffer[bw.position:], v)
	bw.position += length
}

func (bw *binaryOutputStreamImpl) WriteBoolSlice(val []bool) {
	writePrimitiveSliceFast(bw, val, boolBytes)
}

func (bw *binaryOutputStreamImpl) WriteInt8Slice(val []int8) {
	writePrimitiveSliceFast(bw, val, byteBytes)
}

func (bw *binaryOutputStreamImpl) WriteUInt16Slice(val []uint16) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, charBytes)
	} else {
		for _, v := range val {
			bw.WriteUInt16(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteInt16Slice(val []int16) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, shortBytes)
	} else {
		for _, v := range val {
			bw.WriteInt16(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteUInt32Slice(val []uint32) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, intBytes)
	} else {
		for _, v := range val {
			bw.WriteUInt32(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteInt32Slice(val []int32) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, intBytes)
	} else {
		for _, v := range val {
			bw.WriteInt32(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteUIntSlice(val []uint) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, longBytes)
	} else {
		for _, v := range val {
			bw.WriteUInt64(uint64(v))
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteIntSlice(val []int) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, longBytes)
	} else {
		for _, v := range val {
			bw.WriteInt64(int64(v))
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteUInt64Slice(val []uint64) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, longBytes)
	} else {
		for _, v := range val {
			bw.WriteUInt64(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteInt64Slice(val []int64) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, longBytes)
	} else {
		for _, v := range val {
			bw.WriteInt64(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteFloat32Slice(val []float32) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, intBytes)
	} else {
		for _, v := range val {
			bw.WriteFloat32(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) WriteFloat64Slice(val []float64) {
	if isLittleEndian {
		writePrimitiveSliceFast(bw, val, longBytes)
	} else {
		for _, v := range val {
			bw.WriteFloat64(v)
		}
	}
}

func (bw *binaryOutputStreamImpl) HashCode(start int, end int) int32 {
	bufLen := len(bw.buffer)
	if start > bufLen || start < 0 || end > bufLen || end < 0 || start > end {
		panic(fmt.Sprintf("invalid start=%d, end=%d", start, end))
	}
	return internal.SliceHashCode(bw.buffer[start:end])
}

func (br *binaryInputStreamImpl) Available() int {
	return len(br.buffer) - br.position
}

func (br *binaryInputStreamImpl) Position() int {
	return br.position
}

func (br *binaryInputStreamImpl) SetPosition(pos int) {
	if pos < 0 {
		panic(fmt.Sprintf("negatige position passed: %d", pos))
	}
	if len(br.buffer) < pos {
		panic(fmt.Sprintf("position %d is out of range, buffer length: %d", pos, len(br.buffer)))
	}
	br.position = pos
}

func (br *binaryInputStreamImpl) ReadBool() bool {
	ret := false
	if br.buffer[br.position] == 1 {
		ret = true
	}
	br.position += boolBytes
	return ret
}

func (br *binaryInputStreamImpl) ReadUInt8() uint8 {
	ret := br.buffer[br.position]
	br.position += byteBytes
	return ret
}

func (br *binaryInputStreamImpl) ReadInt8() int8 {
	return int8(br.ReadUInt8())
}

func (br *binaryInputStreamImpl) ReadUInt16() uint16 {
	r := binary.LittleEndian.Uint16(br.buffer[br.position:])
	br.position += shortBytes
	return r
}

func (br *binaryInputStreamImpl) ReadInt16() int16 {
	return int16(br.ReadUInt16())
}

func (br *binaryInputStreamImpl) ReadUInt32() uint32 {
	r := binary.LittleEndian.Uint32(br.buffer[br.position:])
	br.position += intBytes
	return r
}

func (br *binaryInputStreamImpl) ReadInt32() int32 {
	return int32(br.ReadUInt32())
}

func (br *binaryInputStreamImpl) ReadUInt64() uint64 {
	r := binary.LittleEndian.Uint64(br.buffer[br.position:])
	br.position += longBytes
	return r
}

func (br *binaryInputStreamImpl) IsNull() bool {
	if br.buffer[br.position] == byte(NullType) {
		br.position += byteBytes
		return true
	}
	return false
}

func (br *binaryInputStreamImpl) ReadInt64() int64 {
	return int64(br.ReadUInt64())
}

func (br *binaryInputStreamImpl) ReadFloat32() float32 {
	return math.Float32frombits(br.ReadUInt32())
}

func (br *binaryInputStreamImpl) ReadFloat64() float64 {
	return math.Float64frombits(br.ReadUInt64())
}

func (br *binaryInputStreamImpl) ReadBytes(size int) []byte {
	ret := make([]byte, size)
	br.position += copy(ret, br.buffer[br.position:])
	return ret
}

func (br *binaryInputStreamImpl) ReadBoolSlice(size int) []bool {
	return readPrimitiveSliceFast[bool](br, byteBytes, size)
}

func (br *binaryInputStreamImpl) ReadInt8Slice(size int) []int8 {
	return readPrimitiveSliceFast[int8](br, byteBytes, size)
}

func (br *binaryInputStreamImpl) ReadUInt16Slice(size int) []uint16 {
	if isLittleEndian {
		return readPrimitiveSliceFast[uint16](br, charBytes, size)
	} else {
		ret := make([]uint16, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadUInt16()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadInt16Slice(size int) []int16 {
	if isLittleEndian {
		return readPrimitiveSliceFast[int16](br, charBytes, size)
	} else {
		ret := make([]int16, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadInt16()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadUInt32Slice(size int) []uint32 {
	if isLittleEndian {
		return readPrimitiveSliceFast[uint32](br, intBytes, size)
	} else {
		ret := make([]uint32, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadUInt32()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadInt32Slice(size int) []int32 {
	if isLittleEndian {
		return readPrimitiveSliceFast[int32](br, intBytes, size)
	} else {
		ret := make([]int32, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadInt32()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadUInt64Slice(size int) []uint64 {
	if isLittleEndian {
		return readPrimitiveSliceFast[uint64](br, longBytes, size)
	} else {
		ret := make([]uint64, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadUInt64()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadInt64Slice(size int) []int64 {
	if isLittleEndian {
		return readPrimitiveSliceFast[int64](br, longBytes, size)
	} else {
		ret := make([]int64, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadInt64()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadFloat32Slice(size int) []float32 {
	if isLittleEndian {
		return readPrimitiveSliceFast[float32](br, intBytes, size)
	} else {
		ret := make([]float32, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadFloat32()
		}
		return ret
	}
}

func (br *binaryInputStreamImpl) ReadFloat64Slice(size int) []float64 {
	if isLittleEndian {
		return readPrimitiveSliceFast[float64](br, longBytes, size)
	} else {
		ret := make([]float64, size)
		for i := 0; i < size; i++ {
			ret[i] = br.ReadFloat64()
		}
		return ret
	}
}

type primitives interface {
	~bool | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func writePrimitiveSliceFast[T primitives](bw *binaryOutputStreamImpl, val []T, elemSz int) {
	length := len(val) * elemSz
	bw.ensureAvailable(length)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(&val[0])), length)
	copy(bw.buffer[bw.position:bw.position+length], raw)
	bw.position += length
}

func readPrimitiveSliceFast[T primitives](br *binaryInputStreamImpl, elemSz int, size int) []T {
	length := size * elemSz
	ret := make([]T, size)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(&ret[0])), length)
	copy(raw, br.buffer[br.position:br.position+length])
	br.position += length
	return ret
}

func readSlice[T any](reader BinaryInputStream, elemReader func(int, BinaryInputStream) (T, error)) ([]T, error) {
	sz := int(reader.ReadInt32())
	coll := make([]T, sz)
	err := readSequence(reader, sz, func(idx int, reader BinaryInputStream) error {
		el, err := elemReader(idx, reader)
		if err != nil {
			return err
		}
		coll[idx] = el
		return nil
	})
	return coll, err
}

func readSequence(reader BinaryInputStream, length int, elemReader func(idx int, reader BinaryInputStream) error) error {
	for i := 0; i < length; i++ {
		if err := elemReader(i, reader); err != nil {
			return err
		}
	}
	return nil
}

func writeSequence(writer BinaryOutputStream, length int, valueWriter func(output BinaryOutputStream, idx int) error) error {
	writer.WriteInt32(int32(length))
	for idx := 0; idx < length; idx++ {
		err := valueWriter(writer, idx)
		if err != nil {
			return err
		}
	}
	return nil
}

func readMap[K comparable, V any](reader BinaryInputStream, kvReader func(BinaryInputStream) (K, V, error)) (map[K]V, error) {
	length := int(reader.ReadInt32())
	outMap := make(map[K]V)
	err := readSequence(reader, length, func(_ int, reader BinaryInputStream) error {
		k, v, err := kvReader(reader)
		if err != nil {
			return err
		}
		outMap[k] = v
		return nil
	})
	return outMap, err
}

func writeMap[K comparable, V any](writer BinaryOutputStream, inMap map[K]V, kvWriter func(BinaryOutputStream, K, V) error) error {
	writer.WriteInt32(int32(len(inMap)))
	for k, v := range inMap {
		err := kvWriter(writer, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
