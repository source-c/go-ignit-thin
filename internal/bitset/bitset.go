package bitset

import "encoding/binary"

const (
	addressBitsPerWord uint = 6
)

type BitSet struct {
	words      []uint64
	wordsInUse uint
}

func New() *BitSet {
	return &BitSet{
		words: make([]uint64, 1),
	}
}

func (bs *BitSet) Test(idx uint) bool {
	wordIdx := wordIndex(idx)
	if wordIdx >= bs.wordsInUse {
		return false
	}
	if wordIdx > 0 {
		idx = idx % wordIdx
	}
	return bs.words[wordIdx]&(uint64(1)<<idx) != 0
}

func (bs *BitSet) Set(idx uint) {
	wordIdx := wordIndex(idx)
	bs.expandTo(wordIdx)
	if wordIdx > 0 {
		idx = idx % wordIdx
	}
	bs.words[wordIdx] |= uint64(1) << idx
}

func (bs *BitSet) Clear(idx uint) {
	wordIdx := wordIndex(idx)
	if wordIdx >= bs.wordsInUse {
		return
	}
	if wordIdx > 0 {
		idx = idx % wordIdx
	}
	bs.words[wordIdx] &= ^(uint64(1) << idx)
	bs.recalculateWordsInUse()
}

func (bs *BitSet) Equals(other *BitSet) bool {
	if bs.wordsInUse != other.wordsInUse {
		return false
	}
	for i := uint(0); i < bs.wordsInUse; i++ {
		if bs.words[i] != other.words[i] {
			return false
		}
	}
	return true
}

func FromBytes(data []byte) *BitSet {
	if len(data) == 0 {
		return New()
	}
	if x := len(data) % 8; x > 0 {
		for i := x; i < 8; i++ {
			data = append(data, 0)
		}
	}

	length := len(data) >> 3
	words := make([]uint64, length)

	for i := 0; i < length; i++ {
		words[i] = binary.LittleEndian.Uint64(data[8*i:])
	}

	bs := BitSet{
		words:      words,
		wordsInUse: uint(length),
	}
	bs.recalculateWordsInUse()
	return &bs
}

func (bs *BitSet) Bytes() []byte {
	// Simplify serialization, don't care about excessive bytes.
	n := bs.wordsInUse
	if n == 0 {
		return []byte{}
	}
	res := make([]byte, 8*n)
	for i := uint(0); i < n; i++ {
		binary.LittleEndian.PutUint64(res[8*i:], bs.words[i])
	}
	return res
}

func (bs *BitSet) expandTo(wordIdx uint) {
	wordsRequired := wordIdx + 1
	if bs.wordsInUse < wordsRequired {
		bs.ensureCapacity(wordsRequired)
		bs.wordsInUse = wordsRequired
	}
}

func (bs *BitSet) ensureCapacity(wordsRequired uint) {
	if len(bs.words) < int(wordsRequired) {
		tmp := make([]uint64, maxUint(wordsRequired, uint(len(bs.words))*2))
		copy(tmp, bs.words)
		bs.words = tmp
	}
}

func (bs *BitSet) recalculateWordsInUse() {
	var i int
	for i = int(bs.wordsInUse) - 1; i >= 0; i-- {
		if bs.words[i] != 0 {
			break
		}
	}
	bs.wordsInUse = uint(i) + 1
}

func wordIndex(bitIndex uint) uint {
	return bitIndex >> addressBitsPerWord
}

func maxUint(x uint, y uint) uint {
	if x > y {
		return x
	} else {
		return y
	}
}
