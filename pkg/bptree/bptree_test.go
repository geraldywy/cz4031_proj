package bptree

import (
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"reflect"
	"sort"
	"testing"
)

func Test_bptree_InsertAndSearch(t *testing.T) {
	type fields struct {
		m     uint8
		store storage.Storage
	}
	tests := []struct {
		name    string
		fields  fields
		records [][]record.Record
		wantErr bool
	}{
		{
			name: "basic insert (one block) + read back",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000001", 4.3, 2)},
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
			},
			wantErr: false,
		},
		{
			name: "basic insert (multiple blocks) + read back",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000001", 4.3, 2)},
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000004", 4.3, 11)},
				{record.NewRecord("tt0000005", 4.3, 2000)},
				{record.NewRecord("tt0000006", 4.3, 500)},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0000008", 4.3, 554)},
				{record.NewRecord("tt0000009", 4.3, 553)},
				{record.NewRecord("tt0000010", 4.3, 551)},
				{record.NewRecord("tt0000011", 4.3, 12)},
				{record.NewRecord("tt0000011", 4.3, 15)},
			},
			wantErr: false,
		},
		{
			name: "basic insert (multiple blocks) + read back, different order",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000011", 4.3, 15)},
				{record.NewRecord("tt0000005", 4.3, 2000)},
				{record.NewRecord("tt0000006", 4.3, 500)},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0000008", 4.3, 554)},
				{record.NewRecord("tt0000001", 4.3, 2)},
				{record.NewRecord("tt0000009", 4.3, 553)},
				{record.NewRecord("tt0000004", 4.3, 11)},
				{record.NewRecord("tt0000010", 4.3, 551)},
				{record.NewRecord("tt0000011", 4.3, 12)},
			},
			wantErr: false,
		},
		{
			name: "basic insert (multiple blocks, duplicate keys) + read back",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000011", 4.3, 15)},
				{record.NewRecord("tt0000005", 4.3, 2000)},
				{
					record.NewRecord("tt0000006", 4.3, 500),
					record.NewRecord("tt0000018", 4.3, 500),
					record.NewRecord("tt0000012", 4.3, 500),
					record.NewRecord("tt0000016", 4.3, 500),
				},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0000008", 4.3, 554)},
				{record.NewRecord("tt0000001", 4.3, 2)},
				{
					record.NewRecord("tt0000009", 4.3, 553),
					record.NewRecord("tt0002016", 4.3, 553),
					record.NewRecord("tt0001016", 4.3, 553),
				},
				{record.NewRecord("tt0000004", 4.3, 11)},
				{record.NewRecord("tt0000010", 4.3, 551)},
				{record.NewRecord("tt0000011", 4.3, 12)},
			},
			wantErr: false,
		},
		{
			name: "basic insert (multiple blocks, duplicate keys) + read back, different order",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{
					record.NewRecord("tt0000006", 4.3, 500),
					record.NewRecord("tt0000018", 4.3, 500),
					record.NewRecord("tt0000012", 4.3, 500),
					record.NewRecord("tt0000016", 4.3, 500),
				},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000011", 4.3, 15), record.NewRecord("tt0200011", 4.3, 15)},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0000008", 4.3, 554)},
				{record.NewRecord("tt0000010", 4.3, 551), record.NewRecord("tt2000010", 4.3, 551)},
				{record.NewRecord("tt0000011", 4.3, 12)},
				{record.NewRecord("tt0000005", 4.3, 2000), record.NewRecord("tt0040005", 4.3, 2000)},
				{record.NewRecord("tt0000001", 4.3, 2)},
				{
					record.NewRecord("tt0000009", 4.3, 553),
					record.NewRecord("tt0002016", 4.3, 553),
					record.NewRecord("tt0001016", 4.3, 553),
				},
				{record.NewRecord("tt0000004", 4.3, 11)},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup insert records into store first
			insertionOrder := make([]uint32, 0)
			ptrsToInsert := make(map[uint32][]*storage_ptr.StoragePointer)
			for _, recs := range tt.records {
				k := uint32(recs[0].NumVotes())
				insertionOrder = append(insertionOrder, k)
				for _, rec := range recs {
					ptr, err := tt.fields.store.Insert(rec)
					if err != nil {
						t.Fatalf("Failed to setup test precondition, insertion of records into store, err: %v", err)
					}
					ptrsToInsert[k] = append(ptrsToInsert[k], ptr)
				}
			}

			b := NewBPTree(tt.fields.m, tt.fields.store)
			for _, k := range insertionOrder {
				for _, ptr := range ptrsToInsert[k] {
					if err := b.Insert(k, ptr); (err != nil) != tt.wantErr {
						t.Fatalf("Insert() error = %v, wantErr %v", err, tt.wantErr)
					}
				}
			}

			// read back
			for _, recs := range tt.records {
				k := uint32(recs[0].NumVotes())
				records, _, err := b.Search(k)
				if err != nil {
					t.Fatalf("readback err = %v, key: %d", err, k)
				}
				if len(recs) != len(records) {
					t.Fatalf("different length readback err, got size: %d, want size: %d", len(records), len(recs))
				}
				sort.Slice(recs, func(i, j int) bool {
					return recs[i].TConst() < recs[j].TConst()
				})
				sort.Slice(records, func(i, j int) bool {
					return records[i].TConst() < records[j].TConst()
				})

				if !reflect.DeepEqual(records, recs) {
					t.Fatalf("mismatch readback err, got: %d, want: %d", records, recs)
				}
			}
		})
	}
}
