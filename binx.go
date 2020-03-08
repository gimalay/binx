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
		Close() error
		Update(func(Reader, Writer) error) error
		View(func(Reader) error) error
	}

	Reader interface {
		Get(q Queryable, key []byte) error
		List(qs QueryableSlice) error
		Query(qs QueryableSlice, qr Query) error
		Last(q Queryable) error
		First(q Queryable) error
	}

	Writer interface {
		Put(Indexable) error
	}

	Query interface {
		validate() error
	}
)

func (q Page) validate() error  { return nil }
func (q By) validate() error    { return nil }
func (q Range) validate() error { return nil }
func (q Where) validate() error { return nil }

type (
	Page struct {
		Skip  int
		Limit int
	}
	By struct {
		Index Index
		Skip  int
		Limit int
	}
	Range struct {
		From  Index
		To    Index
		Skip  int
		Limit int
	}
	Where struct {
		Value Index
		Skip  int
		Limit int
	}
)

type query struct {
	from  Index
	to    Index
	where Index
	by    Index
	skip  int
	limit int
}

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

func (s *db) Update(fn func(r Reader, w Writer) error) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return fn(&reader{tx}, &writer{tx})
	})
}

func (s *db) View(fn func(w Reader) error) error {
	return s.DB.View(func(tx *bolt.Tx) error {
		return fn(&reader{tx})
	})
}
