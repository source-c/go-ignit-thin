package ignite

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

const testArrSize = 1 << 11

func TestReadWritePrimitiveSlices(t *testing.T) {
	fixtures := []struct {
		name    string
		arrTest func()
	}{
		{
			"BoolSlice",
			func() {
				exp := createRandPrimitiveArray(func() bool { return rand.Intn(2) != 0 })
				writer := NewBinaryOutputStream(0)
				writer.WriteBoolSlice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadBoolSlice(len(exp)))
			},
		},
		{
			"ByteSlice",
			func() {
				exp := createRandPrimitiveArray(func() byte { return byte(rand.Intn(1<<8 - 1)) })
				writer := NewBinaryOutputStream(0)
				writer.WriteBytes(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadBytes(len(exp)))
			},
		},
		{
			"SignedByteSlice",
			func() {
				exp := createRandPrimitiveArray(func() int8 { return int8(rand.Intn(1<<8 - 1)) })
				writer := NewBinaryOutputStream(0)
				writer.WriteInt8Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadInt8Slice(len(exp)))
			},
		},
		{
			"UInt16Slice",
			func() {
				exp := createRandPrimitiveArray(func() uint16 { return uint16(rand.Intn(1<<8*2 - 1)) })
				writer := NewBinaryOutputStream(0)
				writer.WriteUInt16Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadUInt16Slice(len(exp)))
			},
		},
		{
			"Int16Slice",
			func() {
				exp := createRandPrimitiveArray(func() int16 { return int16(rand.Intn(1<<8*2 - 1)) })
				writer := NewBinaryOutputStream(0)
				writer.WriteInt16Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadInt16Slice(len(exp)))
			},
		},
		{
			"UInt32Slice",
			func() {
				exp := createRandPrimitiveArray(func() uint32 { return uint32(rand.Uint32()) })
				writer := NewBinaryOutputStream(0)
				writer.WriteUInt32Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadUInt32Slice(len(exp)))
			},
		},
		{
			"Int32Slice",
			func() {
				exp := createRandPrimitiveArray(func() int32 { return int32(rand.Uint32()) })
				writer := NewBinaryOutputStream(0)
				writer.WriteInt32Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadInt32Slice(len(exp)))
			},
		},
		{
			"UIntSlice",
			func() {
				exp := createRandPrimitiveArray(func() uint { return uint(rand.Uint64()) })
				writer := NewBinaryOutputStream(0)
				writer.WriteUIntSlice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, convertSignArrayUInt(exp), reader.ReadUInt64Slice(len(exp)))
			},
		},
		{
			"IntSlice",
			func() {
				exp := createRandPrimitiveArray(func() int { return int(rand.Int63()) })
				writer := NewBinaryOutputStream(0)
				writer.WriteIntSlice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, convertSignArrayInt(exp), reader.ReadInt64Slice(len(exp)))
			},
		},
		{
			"UInt64Slice",
			func() {
				exp := createRandPrimitiveArray(func() uint64 { return rand.Uint64() })
				writer := NewBinaryOutputStream(0)
				writer.WriteUInt64Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadUInt64Slice(len(exp)))
			},
		},
		{
			"Int64Slice",
			func() {
				exp := createRandPrimitiveArray(func() int64 { return rand.Int63() })
				writer := NewBinaryOutputStream(0)
				writer.WriteInt64Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadInt64Slice(len(exp)))
			},
		},
		{
			"Float32Slice",
			func() {
				exp := createRandPrimitiveArray(func() float32 { return rand.Float32() })
				writer := NewBinaryOutputStream(0)
				writer.WriteFloat32Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadFloat32Slice(len(exp)))
			},
		},
		{
			"Float64Slice",
			func() {
				exp := createRandPrimitiveArray(func() float64 { return rand.Float64() })
				writer := NewBinaryOutputStream(0)
				writer.WriteFloat64Slice(exp)
				reader := NewBinaryInputStream(writer.Data(), 0)
				require.Equal(t, exp, reader.ReadFloat64Slice(len(exp)))
			},
		},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name+"/platform-endian", func(t *testing.T) {
			fixture.arrTest()
		})

		t.Run(fixture.name+"/little-endian", func(t *testing.T) {
			isLittleEndian = true
			defer func() {
				isLittleEndian = getNativeEndian()
			}()
			fixture.arrTest()
		})

		t.Run(fixture.name+"/big-endian", func(t *testing.T) {
			isLittleEndian = false
			defer func() {
				isLittleEndian = getNativeEndian()
			}()
			fixture.arrTest()
		})
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			fixture.arrTest()
		})
	}
}

func createRandPrimitiveArray[T primitives](randFactory func() T) []T {
	ret := make([]T, testArrSize)
	for i := 0; i < len(ret); i++ {
		ret[i] = randFactory()
	}
	return ret
}
