package binx

import (
	"encoding"

	bolt "github.com/coreos/bbolt"
)

type (
	db struct {
		*bolt.DB
	}
)

type (
	Bucket interface {
		BucketKey() []byte
	}
	Index interface {
		Bucket
		Key() []byte
	}
	Storable interface {
		Index
		encoding.BinaryMarshaler
	}

	Indexable interface {
		Storable
		MasterIndexBucketKey() []byte
		Indexes() []Index
	}

	Queryable interface {
		Bucket
		encoding.BinaryUnmarshaler
	}
	QueryableSlice interface {
		Bucket
		AppendBinary(data []byte) error
	}

	DB interface {
		Writer

		Close() error
		Update(func(db Writer) error) error
		View(func(db Reader) error) error
	}

	Reader interface {
		List(q QueryableSlice, limit, skip int) error
		ListBy(q QueryableSlice, index Bucket, limit, skip int) error
		ListWhere(q QueryableSlice, index Index, limit, skip int) error
		Get(q Queryable, key []byte) error
		Last(q Queryable) error
		First(q Queryable) error
	}

	Writer interface {
		Reader
		Put(Indexable) error
	}
)

func Open(fileName string, structure []Indexable) (DB, error) {
	buckets := [][]byte{}
	for _, b := range structure {

		buckets = append(buckets, b.BucketKey())
		buckets = append(buckets, b.MasterIndexBucketKey())
		for _, i := range b.Indexes() {
			buckets = append(buckets, i.BucketKey())
		}
	}

	bdb, err := bolt.Open(fileName, 0600, nil)
	s := &db{bdb}
	if err != nil {
		return nil, err
	}

	err = s.DB.Update(func(tx *bolt.Tx) (err error) {

		for _, v := range buckets {
			if _, err = tx.CreateBucketIfNotExists(v); err != nil {
				return err
			}
		}

		return err
	})

	return DB(s), err
}

func (s *db) Close() error {
	return s.DB.Close()
}

func (s *db) Update(fn func(w Writer) error) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return fn(Writer(&writer{tx, &reader{tx}}))
	})
}

func (s *db) View(fn func(w Reader) error) error {
	return s.DB.View(func(tx *bolt.Tx) error {
		return fn(Reader(&reader{tx}))
	})
}

func (s *db) First(q Queryable) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.First(q)
		return err
	})
	return err
}
func (s *db) Last(q Queryable) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.Last(q)
		return err
	})
	return err
}

func (s *db) Get(q Queryable, key []byte) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.Get(q, key)
		return err
	})
	return err
}

func (s *db) List(q QueryableSlice, limit, skip int) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.List(q, limit, skip)
		return err
	})
	return err
}

func (s *db) ListBy(q QueryableSlice, index Bucket, limit, skip int) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.ListBy(q, index, limit, skip)
		return err
	})

	return err
}

func (s *db) ListWhere(q QueryableSlice, index Index, limit, skip int) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.ListWhere(q, index, limit, skip)
		return err
	})

	return err
}

func (s *db) Put(st Indexable) (err error) {
	return s.DB.Update(func(tx *bolt.Tx) error {
		w := &writer{tx, &reader{tx}}
		return w.Put(st)
	})
}
