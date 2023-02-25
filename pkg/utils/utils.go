package utils

import (
	"encoding/binary"
	"math"
)

func Float32ToBytes(f float32) [4]byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], math.Float32bits(f))
	return buf
}

func Float32FromBytes(buf [4]byte) float32 {
	bits := binary.BigEndian.Uint32(buf[:])
	return math.Float32frombits(bits)
}

func Int32ToBytes(v int32) [4]byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	return buf
}

func Int32FromBytes(buf [4]byte) int32 {
	bits := binary.BigEndian.Uint32(buf[:])
	return int32(bits)
}

func SliceTo4ByteArray(s []byte) [4]byte {
	var buf [4]byte
	copy(buf[:], s[:])
	return buf
}

func Max[T int](a, b T) T {
	if a > b {
		return a
	}
	return b
}
