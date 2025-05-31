package bitset

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestClear(t *testing.T) {
	for sz := uint(1); sz <= 1<<10; sz++ {
		bs := New()
		for i := uint(0); i < sz; i++ {
			if rand.Int()%2 == 0 {
				bs.Set(i)
				assert.Truef(t, bs.Test(i), "failed for sz=%d, idx=%d", sz, i)
			}
		}

		for i := uint(0); i < sz; i++ {
			bs.Clear(i)
			assert.Falsef(t, bs.Test(i), "failed for sz=%d, idx=%d", sz, i)
		}

		assert.Truef(t, len(bs.Bytes()) == 0, "failed for sz=%d", sz)
	}
}

func TestEquals(t *testing.T) {
	for sz := uint(1); sz <= 1<<10; sz++ {
		bs := New()
		bs1 := New()
		for i := uint(0); i < sz; i++ {
			if i%2 == 0 {
				bs.Set(i)
				assert.Truef(t, bs.Test(i), "failed for sz=%d, idx=%d", sz, i)
			} else {
				bs1.Set(i)
				assert.Truef(t, bs1.Test(i), "failed for sz=%d, idx=%d", sz, i)
			}
		}

		assert.False(t, bs.Equals(bs1))

		bs1 = New()
		for i := uint(0); i < sz; i++ {
			bs1.Clear(i)
			assert.Falsef(t, bs1.Test(i), "failed for sz=%d, idx=%d", sz, i)
		}

		assert.False(t, bs.Equals(bs1))
		assert.False(t, bs.Equals(New()))
		assert.True(t, bs1.Equals(New()))
	}
}

func TestSerialization(t *testing.T) {
	for sz := uint(0); sz <= 1<<10; sz++ {
		bs1 := New()
		for i := uint(0); i < sz; i++ {
			if rand.Int()%2 == 0 {
				bs1.Set(i)
			}
		}
		serData := bs1.Bytes()
		serData = append(serData, 0)
		bs2 := FromBytes(serData)

		assert.True(t, bs1.Equals(bs2))
		assert.True(t, bs2.Equals(bs1))

		for i := uint(0); i < sz; i++ {
			assert.Equal(t, bs1.Test(i), bs2.Test(i))
		}
	}
}

func TestEmpty(t *testing.T) {
	bs := New()
	assert.True(t, bs.Equals(FromBytes([]byte{})))
	assert.True(t, len(bs.Bytes()) == 0)
	assert.False(t, bs.Test(uint(rand.Uint64())))
}
