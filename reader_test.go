package binx

import (
	"testing"

	bolt "github.com/coreos/bbolt"
	"github.com/stretchr/testify/assert"
)

func Test_store_Get(t *testing.T) {
	tests := []struct {
		name          string
		existing      bucket
		keyToGet      []byte
		argument      Queryable
		expected      Queryable
		expectedError string
	}{
		{
			name: "Get indexable entry id1",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
				},
			},
			keyToGet: []byte(id1),
			argument: &indexable{},
			expected: &indexable{ID: id1, IndexedField: value1},
		},
		{
			name: "Get indexable entry with id2",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
				},
			},
			keyToGet: []byte(id2),
			argument: &indexable{},
			expected: &indexable{ID: id2, IndexedField: value2},
		},
		{
			name:          "key is empty",
			existing:      bucket{bucketName: bucket{}},
			keyToGet:      []byte{},
			argument:      &indexable{},
			expectedError: errEmptyKey,
		},
		{
			name: "nil value",
			existing: bucket{bucketName: bucket{
				id1: bt(&indexable{ID: id1, IndexedField: value1}),
			}},
			keyToGet:      []byte(id1),
			argument:      nil,
			expectedError: errNilPointer,
		},
		{
			name:          "key not found",
			existing:      bucket{bucketName: bucket{}},
			keyToGet:      []byte(id1),
			argument:      &indexable{},
			expectedError: ErrNotFound.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(tx *bolt.Tx) error {
				r := &Tx{tx}
				return r.Get(tt.argument, tt.keyToGet)
			})

			if err != nil {
				assert.Equal(t, tt.expectedError, err.Error())
				return
			}

			assert.Nil(t, err)
			assert.Equal(t, tt.expected, tt.argument)
		})
	}
}

func Test_store_ListRange(t *testing.T) {
	tests := []struct {
		name     string
		existing bucket
		argument Queryable
		from, to Index
		expected Queryable
	}{
		{
			name: "range not specified",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
					value3: bucket{
						id3: []byte{},
					},
				},
			},
			from:     nil,
			to:       nil,
			argument: &indexableSlice{},
			expected: &indexableSlice{
				indexable{ID: id1, IndexedField: value1},
				indexable{ID: id2, IndexedField: value2},
				indexable{ID: id3, IndexedField: value3},
			},
		},
		{
			name: "range from specified",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
					value3: bucket{
						id3: []byte{},
					},
				},
			},
			from:     index(value2),
			to:       nil,
			argument: &indexableSlice{},
			expected: &indexableSlice{
				indexable{ID: id2, IndexedField: value2},
				indexable{ID: id3, IndexedField: value3},
			},
		},
		{
			name: "range to specified",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
					value3: bucket{
						id3: []byte{},
					},
				},
			},
			from:     nil,
			to:       index(value2),
			argument: &indexableSlice{},
			expected: &indexableSlice{
				indexable{ID: id1, IndexedField: value1},
				indexable{ID: id2, IndexedField: value2},
			},
		},
		{
			name: "range to skip 1 specified",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
					value3: bucket{
						id3: []byte{},
					},
				},
			},
			from:     nil,
			to:       index(value2),
			argument: &page{&indexableSlice{}, 1, 0},
			expected: &page{&indexableSlice{
				indexable{ID: id2, IndexedField: value2},
			}, 0, 0},
		},
		{
			name: "range to and limit specified",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
					value3: bucket{
						id3: []byte{},
					},
				},
			},
			from:     nil,
			to:       index(value2),
			argument: &page{&indexableSlice{}, 0, 1},
			expected: &page{&indexableSlice{
				indexable{ID: id1, IndexedField: value1},
			}, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(tx *bolt.Tx) error {
				r := &Tx{tx}
				from := tt.from
				to := tt.to

				if from != nil && to != nil {
					return r.Scan(tt.argument, []Bound{lowerBound{from}, upperBound{to}})
				} else if from != nil {
					return r.Scan(tt.argument, []Bound{lowerBound{from}})
				} else if to != nil {
					return r.Scan(tt.argument, []Bound{upperBound{to}})
				}
				return r.Scan(tt.argument, []Bound{})

			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, tt.argument)
		})
	}
}

func Test_store_ListBy(t *testing.T) {

	tests := []struct {
		name     string
		existing bucket
		argument Queryable
		expected Queryable
	}{
		{
			name: "list by index",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value2}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
						id3: []byte{},
					},
				},
			},
			argument: &indexableSlice{},
			expected: &indexableSlice{
				indexable{ID: id1, IndexedField: value1},
				indexable{ID: id2, IndexedField: value2},
				indexable{ID: id3, IndexedField: value2},
			},
		}, {
			name: "list by index skip and limit",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value2}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
						id3: []byte{},
					},
				},
			},
			argument: &page{Limit: 1, Skip: 1, Queryable: &indexableSlice{}},
			expected: &page{
				&indexableSlice{indexable{ID: id2, IndexedField: value2}}, 0, 0,
			},
		},
		{
			name: "list by index limit",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
				},
			},
			argument: &page{Limit: 1, Skip: 0, Queryable: &indexableSlice{}},
			expected: &page{
				&indexableSlice{indexable{ID: id1, IndexedField: value1}}, 0, 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(tx *bolt.Tx) error {
				r := &Tx{tx}
				return r.Scan(tt.argument, []Bound{
					by{index("")},
				})
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, tt.argument)
		})
	}
}

func Test_store_ListWhere(t *testing.T) {

	tests := []struct {
		name        string
		existing    bucket
		index       string
		limit, skip int
		expected    indexableSlice
	}{
		{
			name: "list where index",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
				indexBucketName: bucket{
					value1: bucket{
						id1: []byte{},
					},
					value2: bucket{
						id2: []byte{},
					},
				},
			},
			limit: 1,
			skip:  0,
			index: value2,
			expected: indexableSlice{
				indexable{ID: id2, IndexedField: value2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			sl := indexableSlice{}

			err := s.View(func(tx *bolt.Tx) error {
				r := &Tx{tx}
				return r.Scan(&sl, []Bound{where{index(tt.index)}})
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, sl)
		})
	}
}

func Test_store_List(t *testing.T) {

	tests := []struct {
		name     string
		existing bucket
		argument Queryable
		expected Queryable
	}{
		{
			name: "List 3 indexable",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
			},
			argument: &indexableSlice{},
			expected: &indexableSlice{
				indexable{ID: id1, IndexedField: value1},
				indexable{ID: id2, IndexedField: value2},
				indexable{ID: id3, IndexedField: value3},
			},
		},
		{
			name: "Limit 1, skip 1 ",
			existing: bucket{
				bucketName: bucket{
					id1: bt(&indexable{ID: id1, IndexedField: value1}),
					id2: bt(&indexable{ID: id2, IndexedField: value2}),
					id3: bt(&indexable{ID: id3, IndexedField: value3}),
				},
			},
			argument: &page{&indexableSlice{}, 1, 1},
			expected: &page{
				&indexableSlice{indexable{ID: id2, IndexedField: value2}}, 0, 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(tx *bolt.Tx) error {
				r := &Tx{tx}
				return r.Scan(tt.argument, []Bound{})
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, tt.argument)

		})
	}
}
