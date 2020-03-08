package binx

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	bolt "github.com/coreos/bbolt"
)

const (
	dbPath                = "/tmp/testdb"
	bucketName            = "bucketName"
	indexBucketName       = "indexBucketName"
	masterIndexBucketName = "masterIndexBucketName"
	id1                   = "id1"
	id2                   = "id2"
	id3                   = "id3"
	value1                = "value1"
	value2                = "value2"
	value3                = "value3"
)

type (
	storable struct {
		ID           string
		IndexedField string
	}

	indexable struct {
		ID           string
		IndexedField string
	}

	storableIndex storable

	storableSlice          []storable
	storableWithIndexSlice []indexable

	bucket map[string]interface{}
)

func (e *storableWithIndexSlice) AppendBinary(data []byte) error {
	*e = append(*e, indexable{})
	return (*e)[len(*e)-1].UnmarshalBinary(data)
}
func (e storableWithIndexSlice) BucketKey() []byte { return []byte(bucketName) }

func (e *storableSlice) AppendBinary(data []byte) error {
	*e = append(*e, storable{})
	return (*e)[len(*e)-1].UnmarshalBinary(data)
}
func (e storableSlice) BucketKey() []byte { return []byte(bucketName) }

func (e *indexable) Key() []byte                              { return []byte(e.ID) }
func (e *indexable) BucketKey() []byte                        { return []byte(bucketName) }
func (e *indexable) MarshalBinary() ([]byte, error)           { return json.Marshal(e) }
func (e *indexable) UnmarshalBinary(jdata []byte) (err error) { return json.Unmarshal(jdata, e) }
func (e *indexable) MasterIndexBucketKey() []byte             { return []byte(masterIndexBucketName) }
func (e *indexable) Indexes() []Index {
	return []Index{
		Index((*storableIndex)(e)),
	}
}

func (e *storableIndex) BucketKey() []byte { return []byte(indexBucketName) }
func (e *storableIndex) Key() []byte       { return []byte(e.IndexedField) }

func (e *storable) Key() []byte                              { return []byte(e.ID) }
func (e *storable) BucketKey() []byte                        { return []byte(bucketName) }
func (e *storable) MarshalBinary() ([]byte, error)           { return json.Marshal(e) }
func (e *storable) UnmarshalBinary(jdata []byte) (err error) { return json.Unmarshal(jdata, e) }

func prep(t *testing.T, initWith bucket) (s *db, teardown func()) {
	_ = os.Remove(dbPath)
	b, err := bolt.Open(dbPath, 0600, &bolt.Options{})
	assert.Nil(t, err)

	err = b.Update(func(tx *bolt.Tx) error {
		for k, v := range initWith {
			ib, err := tx.CreateBucketIfNotExists([]byte(k))
			if err != nil {
				return err
			}
			if m, ok := v.(bucket); ok {
				err = prepBucket(tx, ib, m)
			} else {
				panic("unexpected type")
			}

		}
		return err
	})
	assert.Nil(t, err)

	return &db{
			DB: b,
		}, func() {
			err = s.Close()
			assert.Nil(t, err)
		}
}

func prepBucket(tx *bolt.Tx, bkt *bolt.Bucket, b bucket) (err error) {
	for k, v := range b {
		if im, ok := v.(bucket); ok {
			ib, err := bkt.CreateBucketIfNotExists([]byte(k))
			if err != nil {
				return err
			}
			err = prepBucket(tx, ib, im)
			if err != nil {
				return err
			}
		} else if a, ok := v.([]byte); ok {
			err := bkt.Put([]byte(k), a)
			if err != nil {
				return err
			}
		} else {
			panic("unexpected type")
		}
	}
	return nil
}

func readBuckets(s *bucket) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		return tx.ForEach(func(key []byte, bkt *bolt.Bucket) error {
			b, err := bucketState(bkt)
			if err != nil {
				return err
			}
			(*s)[string(key)] = b
			return nil
		})
	}
}

func bucketState(bkt *bolt.Bucket) (b bucket, err error) {
	b = bucket{}

	err = bkt.ForEach(func(k, v []byte) (err error) {
		if v == nil && bkt.Bucket(k) != nil {
			ib, err := bucketState(bkt.Bucket(k))
			if err != nil {
				return err
			}
			b[string(k)] = ib
		} else {
			b[string(k)] = v
		}
		return nil
	})

	return b, err
}
