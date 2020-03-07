package binx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_store_Last(t *testing.T) {
	tests := []struct {
		name          string
		existing      bucket
		argument      *storable
		expected      storable
		expectedError string
	}{
		{
			name: "Get last storable",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
				},
			},
			argument: &storable{},
			expected: storable{ID: id2, IndexedField: value2},
		},
		{
			name: "nil value",
			existing: bucket{bucketName: bucket{
				id1: bytes(&storable{ID: id1, IndexedField: value1}),
			}},
			argument:      nil,
			expectedError: errNilPointer,
		},
		{
			name:          "key not found",
			existing:      bucket{bucketName: bucket{}},
			argument:      &storable{},
			expectedError: ErrNotFound.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(r Reader) error {
				return r.Last(tt.argument)
			})

			if err != nil {
				assert.Equal(t, tt.expectedError, err.Error())
				return
			}

			assert.Nil(t, err)
			assert.Equal(t, tt.expected, *tt.argument)
		})
	}
}

func Test_store_Get(t *testing.T) {
	tests := []struct {
		name          string
		existing      bucket
		keyToGet      []byte
		argument      *storable
		expected      storable
		expectedError string
	}{
		{
			name: "Get storable entry id1",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
				},
			},
			keyToGet: []byte(id1),
			argument: &storable{},
			expected: storable{ID: id1, IndexedField: value1},
		},
		{
			name: "Get storable entry with id2",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
				},
			},
			keyToGet: []byte(id2),
			argument: &storable{},
			expected: storable{ID: id2, IndexedField: value2},
		},
		{
			name:          "key is empty",
			existing:      bucket{bucketName: bucket{}},
			keyToGet:      []byte{},
			argument:      &storable{},
			expectedError: errEmptyKey,
		},
		{
			name: "nil value",
			existing: bucket{bucketName: bucket{
				id1: bytes(&storable{ID: id1, IndexedField: value1}),
			}},
			keyToGet:      []byte(id1),
			argument:      nil,
			expectedError: errNilPointer,
		},
		{
			name:          "key not found",
			existing:      bucket{bucketName: bucket{}},
			keyToGet:      []byte(id1),
			argument:      &storable{},
			expectedError: ErrNotFound.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			err := s.View(func(r Reader) error {
				return r.Get(tt.argument, tt.keyToGet)
			})

			if err != nil {
				assert.Equal(t, tt.expectedError, err.Error())
				return
			}

			assert.Nil(t, err)
			assert.Equal(t, tt.expected, *tt.argument)
		})
	}
}

func Test_store_ListBy(t *testing.T) {

	tests := []struct {
		name        string
		existing    bucket
		index       string
		limit, skip int
		expected    storableSlice
	}{
		{
			name: "list by index",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value2}),
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
			limit: 0,
			skip:  0,
			expected: storableSlice{
				storable{ID: id1, IndexedField: value1},
				storable{ID: id2, IndexedField: value2},
				storable{ID: id3, IndexedField: value2},
			},
		}, {
			name: "list by index skip and limit",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value2}),
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
			limit: 1,
			skip:  1,
			expected: storableSlice{
				storable{ID: id2, IndexedField: value2},
			},
		},
		{
			name: "list by index limit",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value3}),
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
			expected: storableSlice{
				storable{ID: id1, IndexedField: value1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			sl := storableSlice{}

			err := s.View(func(r Reader) error {
				return r.List(&sl, By(storableIndex{}), Limit(tt.limit), Skip(tt.skip))
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, sl)
		})
	}
}

func Test_store_ListWhere(t *testing.T) {

	tests := []struct {
		name        string
		existing    bucket
		index       string
		limit, skip int
		expected    storableSlice
	}{
		{
			name: "list where index",
			existing: bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value3}),
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
			expected: storableSlice{
				storable{ID: id2, IndexedField: value2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			sl := storableSlice{}

			err := s.View(func(r Reader) error {
				return r.List(&sl, Where(storableIndex{IndexedField: tt.index}), Limit(tt.limit), Skip(tt.skip))
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, sl)
		})
	}
}

func Test_store_List(t *testing.T) {

	tests := []struct {
		name        string
		existing    bucket
		limit, skip int
		expected    storableSlice
	}{
		{
			"List 3 storable",
			bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value3}),
				},
			}, 0, 0,
			storableSlice{
				storable{ID: id1, IndexedField: value1},
				storable{ID: id2, IndexedField: value2},
				storable{ID: id3, IndexedField: value3},
			},
		},
		{
			"Limit 1, skip 1 ",
			bucket{
				bucketName: bucket{
					id1: bytes(&storable{ID: id1, IndexedField: value1}),
					id2: bytes(&storable{ID: id2, IndexedField: value2}),
					id3: bytes(&storable{ID: id3, IndexedField: value3}),
				},
			}, 1, 1,
			storableSlice{
				storable{ID: id2, IndexedField: value2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := prep(t, tt.existing)
			defer teardown()

			sl := storableSlice{}

			err := s.View(func(r Reader) error {
				return r.List(&sl, Limit(tt.limit), Skip(tt.skip))
			})
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, sl)

		})
	}
}
