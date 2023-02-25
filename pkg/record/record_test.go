package record

import (
	"reflect"
	"testing"
)

func TestRecordConversion(t *testing.T) {
	tests := []struct {
		name   string
		record *recordImpl
	}{
		{
			name: "Basic case",
			record: &recordImpl{
				tconst:        "tt0000027",
				averageRating: 5.6,
				numVotes:      925,
			},
		},
		{
			name: "Basic case 2",
			record: &recordImpl{
				tconst:        "tt7701848",
				averageRating: 6.8,
				numVotes:      5,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.record.Serialize()
			newRecord := NewRecordFromBytes(buf)
			if !reflect.DeepEqual(newRecord, tt.record) {
				t.Fatalf("Record not the same after serizaling and deserializing, got: %+v, want: %+v", newRecord, tt.record)
			}
		})
	}
}
