package ignite

import (
	"github.com/stretchr/testify/require"
	"github.com/source-c/go-ignit-thin/internal/bitset"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestParseVersion(t *testing.T) {
	invalid := []string{
		" 1. 7. 0 ",
		"1.7.0.ver",
		"a.c.d",
		"asdasd",
		"1.2",
		"1.2.3.4",
	}
	for _, ver := range invalid {
		_, ok := ParseVersion(ver)
		require.False(t, ok)
	}
	ver, ok := ParseVersion("1.7.0")
	require.True(t, ok)
	require.Equal(t, ver, ProtocolVersion{1, 7, 0})
}

func TestCompareVersion(t *testing.T) {
	var fixtures []ProtocolVersion
	var k, i, j int16
	for k = 0; k < 3; k++ {
		for i = 0; i < 10; i++ {
			for j = 0; j < 10; j++ {
				fixtures = append(fixtures, ProtocolVersion{k, i, j})
			}
		}
	}
	test := make([]ProtocolVersion, len(fixtures))
	copy(test, fixtures)
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(len(test), func(i, j int) {
		test[i], test[j] = test[j], test[i]
	})
	sort.Slice(test, func(i, j int) bool {
		return test[i].Compare(test[j]) < 0
	})
	require.Equal(t, fixtures, test)
}

func TestProtocolFeatures(t *testing.T) {
	ctx := NewProtocolContext(ProtocolVersion{1, 6, 0}, UserAttributesFeature)
	require.Equal(t, ProtocolVersion{1, 6, 0}, ctx.Version())
	require.Nil(t, ctx.features)
	for f := _minFeature; f <= _maxFeature; f++ {
		require.False(t, ctx.SupportsAttributeFeature(f))
	}
	require.False(t, ctx.SupportsBitmapFeatures())

	var features []AttributeFeature
	for f := _minFeature; f <= _maxFeature; f++ {
		features = append(features, f)
	}
	ctx = NewProtocolContext(ProtocolVersion{1, 7, 0}, features...)
	require.True(t, ctx.SupportsBitmapFeatures())
	require.Equal(t, ProtocolVersion{1, 7, 0}, ctx.Version())
	for f := _minFeature; f <= _maxFeature; f++ {
		require.True(t, ctx.SupportsAttributeFeature(f))
	}
	ctx.updateAttributeFeatures(bitset.New())
	for f := _minFeature; f <= _maxFeature; f++ {
		require.False(t, ctx.SupportsAttributeFeature(f))
	}
}
