package storage

import (
	"github.com/geraldywy/cz4031_proj1/pkg/record"
	"github.com/geraldywy/cz4031_proj1/pkg/storage_ptr"
	"reflect"
	"testing"
)

func Test_storageImpl_ReadRecord(t *testing.T) {
	type fields struct {
		store []block
	}
	tests := []struct {
		name    string
		fields  fields
		ptr     *storage_ptr.StoragePointer
		want    record.Record
		wantErr bool
	}{
		{
			name: "Simple read within a block",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{29, 19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					nil,
					nil,
					nil,
				},
			},
			ptr: &storage_ptr.StoragePointer{
				BlockPtr:  2,
				RecordPtr: 1,
			},
			want:    record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			wantErr: false,
		},
		{
			name: "Read across blocks",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{9, 19, 116, 116, 48, 48, 48, 48, 48},
					[]byte{19, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					nil,
					nil,
				},
			},
			ptr: &storage_ptr.StoragePointer{
				BlockPtr:  2,
				RecordPtr: 1,
			},
			want:    record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storageImpl{
				store: tt.fields.store,
			}
			buf, err := s.Read(tt.ptr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := record.NewRecordFromBytes(buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadRecord() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storageImpl_InsertRecord(t *testing.T) {
	type fields struct {
		store       []block
		spaceUsed   int
		maxCapacity int
		blockSize   int
	}
	type args struct {
		record record.Record
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *storage_ptr.StoragePointer
		wantErr bool
	}{
		{
			name: "Basic insert within a block",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{20, 19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				spaceUsed:   100,
				maxCapacity: 1000,
				blockSize:   50,
			},
			args: args{
				record: record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0}),
			},
			want: &storage_ptr.StoragePointer{
				BlockPtr:  2,
				RecordPtr: 20,
			},
			wantErr: false,
		},
		{
			name: "Insert across blocks",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{28, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157,
						0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				spaceUsed:   100,
				maxCapacity: 1000,
				blockSize:   30,
			},
			args: args{
				record: record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0}),
			},
			want: &storage_ptr.StoragePointer{
				BlockPtr:  2,
				RecordPtr: 28,
			},
			wantErr: false,
		},
		{
			name: "Insert at new block",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{28, 0, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				spaceUsed:   500,
				maxCapacity: 1000,
				blockSize:   28,
			},
			args: args{
				record: record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			},
			want: &storage_ptr.StoragePointer{
				BlockPtr:  3,
				RecordPtr: 1,
			},
			wantErr: false,
		},
		{
			name: "Insert at the start",
			fields: fields{
				store:       []block{},
				spaceUsed:   0,
				maxCapacity: 1000,
				blockSize:   21,
			},
			args: args{
				record: record.NewRecordFromBytes([]byte{19, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			},
			want: &storage_ptr.StoragePointer{
				BlockPtr:  0,
				RecordPtr: 1,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storageImpl{
				store:       tt.fields.store,
				spaceUsed:   tt.fields.spaceUsed,
				maxCapacity: tt.fields.maxCapacity,
				blockSize:   tt.fields.blockSize,
			}
			got, err := s.Insert(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got == nil && tt.want != nil) || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InsertRecord() got = %v, want %v", got, tt.want)
			}
			buf, err := s.Read(got)
			if err != nil {
				t.Errorf("Failed to read back record inserted, got err %v", err)
			}
			rec := record.NewRecordFromBytes(buf)
			if !reflect.DeepEqual(rec, tt.args.record) {
				t.Errorf("Failed to read back record inserted, got = %v, want %v", rec, tt.args.record)
			}
		})
	}
}

func Test_storageImpl_DeleteRecord(t *testing.T) {
	type fields struct {
		store       []block
		spaceUsed   int
		maxCapacity int
		blockSize   int
	}
	type args struct {
		ptr *storage_ptr.StoragePointer
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantStore []block
	}{
		{
			name: "delete only record",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{19, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0},
				},
				spaceUsed:   19,
				maxCapacity: 30,
				blockSize:   30,
			},
			args: args{
				ptr: &storage_ptr.StoragePointer{
					BlockPtr:  2,
					RecordPtr: 1,
				},
			},
			wantErr: false,
			wantStore: []block{
				nil,
				nil,
			},
		},
		{
			name: "delete one record within a block",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{37, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 2, 1},
				},
				spaceUsed:   37,
				maxCapacity: 500,
				blockSize:   47,
			},
			args: args{
				ptr: &storage_ptr.StoragePointer{
					BlockPtr:  2,
					RecordPtr: 1,
				},
			},
			wantErr: false,
			wantStore: []block{
				nil,
				nil,
				[]byte{19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 2, 1},
			},
		},
		{
			name: "delete one record, update across blocks",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{23, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 18, 116, 116, 48},
					[]byte{15, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0},
				},
				spaceUsed:   38,
				maxCapacity: 100,
				blockSize:   28,
			},
			args: args{
				ptr: &storage_ptr.StoragePointer{
					BlockPtr:  2,
					RecordPtr: 1,
				},
			},
			wantErr: false,
			wantStore: []block{
				nil,
				nil,
				[]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 116, 116, 48},
				[]byte{15, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0},
			},
		},
		{
			name: "delete one record stored across blocks",
			fields: fields{
				store: []block{
					nil,
					nil,
					[]byte{23, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 18, 116, 116, 48},
					[]byte{15, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157},
				},
				spaceUsed:   38,
				maxCapacity: 100,
				blockSize:   28,
			},
			args: args{
				ptr: &storage_ptr.StoragePointer{
					BlockPtr:  2,
					RecordPtr: 24,
				},
			},
			wantErr: false,
			wantStore: []block{
				nil,
				nil,
				[]byte{19, 18, 116, 116, 48, 48, 48, 48, 48, 50, 55, 64, 179, 51, 51, 0, 0, 3, 157, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storageImpl{
				store:       tt.fields.store,
				spaceUsed:   tt.fields.spaceUsed,
				maxCapacity: tt.fields.maxCapacity,
				blockSize:   tt.fields.blockSize,
			}
			if err := s.Delete(tt.args.ptr); (err != nil) != tt.wantErr {
				t.Errorf("DeleteRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.store, tt.wantStore) {
				t.Errorf("DeleteRecord() store assertion failed got: %v, want %v", s.store, tt.wantStore)
			}
		})
	}
}

func TestBPTNodeConversion(t *testing.T) {
	tests := []struct {
		name string
		node *BPTNode
		m    uint8
	}{
		{
			name: "Basic case",
			node: &BPTNode{
				NumKeys: 1,
				Keys:    []uint32{4, 0, 0},
				ChildPtrs: []*storage_ptr.StoragePointer{
					{
						BlockPtr:  0,
						RecordPtr: 89,
					},
					nil, nil, nil,
				},
				Parent:     nil,
				IsLeafNode: true,
			},
			m: 3,
		},
		{
			name: "Basic case 2",
			node: &BPTNode{
				NumKeys: 1,
				Keys:    []uint32{4, 0, 0, 0, 0},
				ChildPtrs: []*storage_ptr.StoragePointer{
					{
						BlockPtr:  0,
						RecordPtr: 189,
					},
					{
						BlockPtr:  2,
						RecordPtr: 205,
					}, nil, nil, nil, nil,
				},
				Parent: &storage_ptr.StoragePointer{
					BlockPtr:  0,
					RecordPtr: 10,
				},
				IsLeafNode: false,
			},
			m: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			M = tt.m
			buf := tt.node.Serialize()
			newRecord := NewBPTNodeFromBytes(buf)
			if !reflect.DeepEqual(newRecord, tt.node) {
				t.Fatalf("BPT Node not the same after serizaling and deserializing, got: %+v, want: %+v", newRecord, tt.node)
			}
		})
	}
}
