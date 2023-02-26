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

func UInt32ToBytes(v uint32) [4]byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return buf
}

func UInt32FromBytes(buf [4]byte) uint32 {
	bits := binary.BigEndian.Uint32(buf[:])
	return bits
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

func UInt64ToBytes(v uint64) [8]byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf
}

func UInt64FromBytes(buf [8]byte) uint64 {
	bits := binary.BigEndian.Uint64(buf[:])
	return bits
}

func SliceTo4ByteArray(s []byte) [4]byte {
	var buf [4]byte
	copy(buf[:], s[:])
	return buf
}

func SliceTo8ByteArray(s []byte) [8]byte {
	var buf [8]byte
	copy(buf[:], s[:])
	return buf
}

func Max[T int | int8](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func Min[T int | int8](a, b T) T {
	if a < b {
		return a
	}
	return b
}
