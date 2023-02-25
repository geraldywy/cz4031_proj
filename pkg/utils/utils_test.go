package utils

import (
	"testing"
)

func TestFloat32Conversion(t *testing.T) {
	tests := []struct {
		name  string
		input float32
	}{
		{
			name:  "Positive float32",
			input: 11.023,
		},
		{
			name:  "Zero float32",
			input: 0,
		},
		{
			name:  "Negative float32",
			input: -2.34,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Float32FromBytes(Float32ToBytes(tt.input)); got != tt.input {
				t.Errorf("Float32FromBytes() = %v, want %v", got, tt.input)
			}
		})
	}
}

func TestInt32Conversion(t *testing.T) {
	tests := []struct {
		name  string
		input int32
	}{
		{
			name:  "Positive int32",
			input: 11002,
		},
		{
			name:  "Zero int32",
			input: 0,
		},
		{
			name:  "Negative int32",
			input: -2002,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Int32FromBytes(Int32ToBytes(tt.input)); got != tt.input {
				t.Errorf("Float32FromBytes() = %v, want %v", got, tt.input)
			}
		})
	}
}
