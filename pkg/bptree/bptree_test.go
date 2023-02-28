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

func Test_bptree_InsertAndSearchRange(t *testing.T) {
	type fields struct {
		m     uint8
		store storage.Storage
	}
	tests := []struct {
		name        string
		fields      fields
		records     [][]record.Record
		from        uint32
		to          uint32
		wantErr     bool
		wantRecords []record.Record
	}{
		{
			name: "one block read range",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000001", 4.3, 2)},
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
			},
			from:    2,
			to:      5,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000001", 4.3, 2),
				record.NewRecord("tt0000002", 3.2, 5),
			},
		},
		{
			name: "read range multiple blocks",
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
			from:    200,
			to:      555,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000006", 4.3, 500),
				record.NewRecord("tt0000007", 4.3, 550),
				record.NewRecord("tt0000008", 4.3, 554),
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0000010", 4.3, 551),
			},
		},
		{
			name: "read range multiple blocks, different order",
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
			from:    200,
			to:      555,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000006", 4.3, 500),
				record.NewRecord("tt0000007", 4.3, 550),
				record.NewRecord("tt0000008", 4.3, 554),
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0000010", 4.3, 551),
			},
		},
		{
			name: "multiple blocks, duplicate keys, simple read range",
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
			from:    552,
			to:      553,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0002016", 4.3, 553),
				record.NewRecord("tt0001016", 4.3, 553),
			},
		},
		{
			name: "multiple blocks, duplicate keys, read range",
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
				{record.NewRecord("tt0000111", 4.3, 12)},
			},
			from:    10,
			to:      555,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000011", 4.3, 15),
				record.NewRecord("tt0000006", 4.3, 500),
				record.NewRecord("tt0000018", 4.3, 500),
				record.NewRecord("tt0000012", 4.3, 500),
				record.NewRecord("tt0000016", 4.3, 500),
				record.NewRecord("tt0000007", 4.3, 550),
				record.NewRecord("tt0000008", 4.3, 554),
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0002016", 4.3, 553),
				record.NewRecord("tt0001016", 4.3, 553),
				record.NewRecord("tt0000004", 4.3, 11),
				record.NewRecord("tt0000010", 4.3, 551),
				record.NewRecord("tt0000111", 4.3, 12),
			},
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
				{
					record.NewRecord("tt0000011", 4.3, 15),
					record.NewRecord("tt0200011", 4.3, 15),
				},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0000008", 4.3, 554)},
				{
					record.NewRecord("tt0000010", 4.3, 551),
					record.NewRecord("tt2000010", 4.3, 551),
				},
				{record.NewRecord("tt0000111", 4.3, 12)},
				{record.NewRecord("tt0000005", 4.3, 2000), record.NewRecord("tt0040005", 4.3, 2000)},
				{record.NewRecord("tt0000001", 4.3, 2)},
				{
					record.NewRecord("tt0000009", 4.3, 553),
					record.NewRecord("tt0002016", 4.3, 553),
					record.NewRecord("tt0001016", 4.3, 553),
				},
				{record.NewRecord("tt0000004", 4.3, 11)},
			},
			from:    10,
			to:      555,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000011", 4.3, 15),
				record.NewRecord("tt0000006", 4.3, 500),
				record.NewRecord("tt0000018", 4.3, 500),
				record.NewRecord("tt0000012", 4.3, 500),
				record.NewRecord("tt0000016", 4.3, 500),
				record.NewRecord("tt0000007", 4.3, 550),
				record.NewRecord("tt0000008", 4.3, 554),
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0002016", 4.3, 553),
				record.NewRecord("tt0001016", 4.3, 553),
				record.NewRecord("tt0000004", 4.3, 11),
				record.NewRecord("tt0000010", 4.3, 551),
				record.NewRecord("tt0000111", 4.3, 12),
				record.NewRecord("tt0200011", 4.3, 15),
				record.NewRecord("tt2000010", 4.3, 551),
			},
		},
		{
			name: "multiple blocks, duplicate keys, read all",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000111", 4.3, 15)},
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
			from:    0,
			to:      100000,
			wantErr: false,
			wantRecords: []record.Record{
				record.NewRecord("tt0000002", 3.2, 5),
				record.NewRecord("tt0000003", 2.2, 1),
				record.NewRecord("tt0000111", 4.3, 15),
				record.NewRecord("tt0000005", 4.3, 2000),
				record.NewRecord("tt0000006", 4.3, 500),
				record.NewRecord("tt0000018", 4.3, 500),
				record.NewRecord("tt0000012", 4.3, 500),
				record.NewRecord("tt0000016", 4.3, 500),
				record.NewRecord("tt0000007", 4.3, 550),
				record.NewRecord("tt0000008", 4.3, 554),
				record.NewRecord("tt0000001", 4.3, 2),
				record.NewRecord("tt0000009", 4.3, 553),
				record.NewRecord("tt0002016", 4.3, 553),
				record.NewRecord("tt0001016", 4.3, 553),
				record.NewRecord("tt0000004", 4.3, 11),
				record.NewRecord("tt0000010", 4.3, 551),
				record.NewRecord("tt0000011", 4.3, 12),
			},
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

			// read range
			records, err := b.SearchRange(tt.from, tt.to)
			if err != nil {
				t.Fatalf("read range err: %v", err)
			}
			sort.Slice(tt.wantRecords, func(i, j int) bool {
				return tt.wantRecords[i].TConst() < tt.wantRecords[j].TConst()
			})
			sort.Slice(records, func(i, j int) bool {
				return records[i].TConst() < records[j].TConst()
			})

			if !reflect.DeepEqual(records, tt.wantRecords) {
				t.Fatalf("mismatch readback err, got: %d, want: %d", records, tt.wantRecords)
			}
		})
	}
}

func Test_bptree_DeleteAll(t *testing.T) {
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
			name: "one block simple delete",
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
			name: "delete multiple blocks",
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
			name: "delete multiple blocks, different order",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000111", 4.3, 15)},
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
			name: "multiple blocks, duplicate keys, delete all",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0000002", 3.2, 5)},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{record.NewRecord("tt0000111", 4.3, 15)},
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
			name: "delete multiple blocks, duplicate keys",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt1000002", 3.2, 5)},
				{record.NewRecord("tt2000003", 2.2, 1)},
				{record.NewRecord("tt3000111", 4.3, 15)},
				{record.NewRecord("tt4000005", 4.3, 2000)},
				{
					record.NewRecord("tt1000006", 4.3, 500),
					record.NewRecord("tt2000018", 4.3, 500),
					record.NewRecord("tt3000012", 4.3, 500),
					record.NewRecord("tt4000016", 4.3, 500),
				},
				{record.NewRecord("tt1000007", 4.3, 550)},
				{record.NewRecord("tt1000008", 4.3, 554)},
				{record.NewRecord("tt1000001", 4.3, 2)},
				{
					record.NewRecord("tt2000009", 4.3, 553),
					record.NewRecord("tt3002016", 4.3, 553),
					record.NewRecord("tt4001016", 4.3, 553),
				},
				{record.NewRecord("tt2000004", 4.3, 11)},
				{record.NewRecord("tt3000010", 4.3, 551)},
				{record.NewRecord("tt4000111", 4.3, 12)},
			},
			wantErr: false,
		},
		{
			name: "delete (multiple blocks, duplicate keys), different order",
			fields: fields{
				m:     3,
				store: storage.NewStorage(1024, 200),
			},
			records: [][]record.Record{
				{record.NewRecord("tt0020002", 3.2, 5)},
				{
					record.NewRecord("tt2000006", 4.3, 500),
					record.NewRecord("tt0000018", 4.3, 500),
					record.NewRecord("tt0000012", 4.3, 500),
					record.NewRecord("tt0000016", 4.3, 500),
				},
				{record.NewRecord("tt0000003", 2.2, 1)},
				{
					record.NewRecord("tt0010011", 4.3, 15),
					record.NewRecord("tt0200011", 4.3, 15),
				},
				{record.NewRecord("tt0000007", 4.3, 550)},
				{record.NewRecord("tt0020008", 4.3, 554)},
				{
					record.NewRecord("tt0000010", 4.3, 551),
					record.NewRecord("tt2000010", 4.3, 551),
				},
				{record.NewRecord("tt0000111", 4.3, 12)},
				{record.NewRecord("tt0300005", 4.3, 2000), record.NewRecord("tt0040005", 4.3, 2000)},
				{record.NewRecord("tt0400001", 4.3, 2)},
				{
					record.NewRecord("tt3000009", 4.3, 553),
					record.NewRecord("tt0002016", 4.3, 553),
					record.NewRecord("tt0001016", 4.3, 553),
				},
				{record.NewRecord("tt0040004", 4.3, 11)},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			insertionOrder := make([]uint32, 0)
			for _, recs := range tt.records {
				k := uint32(recs[0].NumVotes())
				insertionOrder = append(insertionOrder, k)
			}

			// let every key be the deletion key once
			for _, delKey := range insertionOrder {
				// redo store
				tt.fields.store = storage.NewStorage(1024, 200)
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
				expectedResult := make([]record.Record, 0)
				for _, recs := range tt.records {
					k := uint32(recs[0].NumVotes())
					if k != delKey {
						expectedResult = append(expectedResult, recs...)
					}
				}
				if err := b.DeleteAll(delKey); err != nil {
					t.Fatalf("delete all, key: %d err: %v", delKey, err)
				}

				// read back all keys
				records, err := b.SearchRange(0, 100000)
				if err != nil {
					t.Fatalf("read all err: %v", err)
				}
				sort.Slice(expectedResult, func(i, j int) bool {
					return expectedResult[i].TConst() < expectedResult[j].TConst()
				})
				sort.Slice(records, func(i, j int) bool {
					return records[i].TConst() < records[j].TConst()
				})
				if !reflect.DeepEqual(records, expectedResult) {
					t.Fatalf("mismatch readback err for deletion key: %d, got: %d, want: %d", delKey, records, expectedResult)
				}
			}

		})
	}
}
