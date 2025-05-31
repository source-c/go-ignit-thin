package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashCode(t *testing.T) {
	assert.Equal(t, int32(1544803905), HashCode("default"))
	assert.Equal(t, int32(1482644790), HashCode("myCache"))
	assert.Equal(t, int32(-1671596858), HashCode("ABCDEFGHIJKL"))
	assert.Equal(t, int32(-1172872323), HashCode("Cache1234567890"))
	assert.Equal(t, int32(1449161031), HashCode("Cache-123"))
	assert.Equal(t, int32(-1094917604), HashCode("Hello, 世界"))
	assert.Equal(t, int32(1520184206), HashCode("禁止抽烟!🚭"))
}
