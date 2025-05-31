package internal

import "unicode/utf16"

const (
	replaceChar  = '\uFFFD'
	maxCodePoint = '\U0010FFFF'
	maxUint16    = 0xFFFF
)

func HashCode(name string) int32 {
	if len(name) == 0 {
		return 0
	}
	var hash int32 = 0
	for _, r := range name {
		if r <= maxUint16 {
			hash = 31*hash + r
		} else if r > maxUint16 && r <= maxCodePoint {
			r1, r2 := utf16.EncodeRune(r)
			hash = 31*hash + r1
			hash = 31*hash + r2
		} else {
			hash = 31*hash + replaceChar
		}
	}
	return hash
}

func SliceHashCode(data []byte) int32 {
	var hash int32 = 1
	for _, b := range data {
		hash = 31*hash + int32(b)
	}
	return hash
}
