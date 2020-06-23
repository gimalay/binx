package binx

import (
	"testing"

	bolt "github.com/coreos/bbolt"
	"github.com/stretchr/testify/assert"
)

func bt(m Indexable) []byte {
	b, _ := m.MarshalBinary()
	return b
}

func Test_store_Put_WithIndex(t *testing.T) {
	tests := []struct {
		name     string
		argument []Indexable
		expected bucket
	}{
		{
			name:     "Put storable entry with index",
			argument: []Indexable{&indexable{ID: id1, IndexedField: value1}},
			expected: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
				},
				indexBucketName: bucket{
					value1: bucket{id1: []byte{}},
				},
				masterIndexBucketName: bucket{id1: bucket{indexBucketName: []byte(value1)}},
			},
		},
		{
			name: "Put storable entry with updated index",
			argument: []Indexable{
				&indexable{ID: id1, IndexedField: value1},
				&indexable{ID: id1, IndexedField: value2},
			},
			expected: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value2}),
				},

				indexBucketName: bucket{
					value2: bucket{id1: []byte{}},
					value1: bucket{},
				},
				masterIndexBucketName: bucket{id1: bucket{indexBucketName: []byte(value2)}},
			},
		},
		{
			"Put storable 2 entries",
			[]Indexable{
				&indexable{ID: id1, IndexedField: value1},
				&indexable{ID: id2, IndexedField: value2},
			},
			bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
				},

				indexBucketName: bucket{
					value1: bucket{id1: []byte{}},
					value2: bucket{id2: []byte{}},
				},
				masterIndexBucketName: bucket{
					id1: bucket{indexBucketName: []byte(value1)},
					id2: bucket{indexBucketName: []byte(value2)},
				},
			},
		},
		{
			"Put storable 2 entries and update one index",
			[]Indexable{
				&indexable{ID: id1, IndexedField: value1},
				&indexable{ID: id2, IndexedField: value2},
				&indexable{ID: id1, IndexedField: value3},
			},
			bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value3}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
				},

				indexBucketName: bucket{
					value1: bucket{},
					value2: bucket{id2: []byte{}},
					value3: bucket{id1: []byte{}},
				},
				masterIndexBucketName: bucket{
					id1: bucket{indexBucketName: []byte(value3)},
					id2: bucket{indexBucketName: []byte(value2)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, teardown := prep(t, bucket{bucketName: bucket{}, indexBucketName: bucket{}, masterIndexBucketName: bucket{}})
			defer teardown()

			err := db.Update(func(tx *bolt.Tx) error {
				for _, v := range tt.argument {
					err := (&Tx{tx}).Put(v)
					if err != nil {
						return err
					}
				}
				return nil
			})

			assert.Nil(t, err)
			state := bucket{}
			err = db.View(readBuckets(&state))
			assert.Nil(t, err)

			assert.Equal(t, tt.expected, state)
		})
	}
}
