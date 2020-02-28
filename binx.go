package binx

import (
	"encoding"
	"errors"

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
		Get(q Queryable, key []byte) error
		List(qs QueryableSlice, cnd ...Condition) error
		Last(q Queryable, cnd ...Condition) error
		First(q Queryable, cnd ...Condition) error
	}

	Writer interface {
		Reader
		Put(Indexable) error
	}

	Condition interface {
		apply(query) (query, error)
	}
)

type (
	where struct {
		Index
	}

	by struct {
		Index
	}

	limit struct {
		int
	}

	skip struct {
		int
	}

	query struct {
		Where Index
		By    Index
		Skip  int
		Limit int
	}
)

func Where(idx Index) Condition {
	return where{idx}
}
func By(idx Index) Condition {
	return by{idx}
}
func Limit(n int) Condition {
	return limit{n}
}
func Skip(n int) Condition {
	return skip{n}
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

func (s *db) First(q Queryable, cnd ...Condition) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.First(q, cnd...)
		return err
	})
	return err
}
func (s *db) Last(q Queryable, cnd ...Condition) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.Last(q, cnd...)
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

func (s *db) List(qs QueryableSlice, cnd ...Condition) (err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		r := &reader{tx}
		err = r.List(qs, cnd...)
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

func (cn where) apply(q query) (query, error) {
	if q.Where != nil {
		return q, errors.New("only one \"where\" condition supported at the moment")
	}
	q.Where = cn
	return q, nil
}
func (cn limit) apply(q query) (query, error) {
	if q.Limit != 0 {
		return q, errors.New("only one \"limit\" condition supported at the moment")
	}
	q.Limit = cn.int
	return q, nil
}
func (cn skip) apply(q query) (query, error) {
	if q.Skip != 0 {
		return q, errors.New("only one \"skip\" condition supported at the moment")
	}
	q.Skip = cn.int
	return q, nil
}
func (cn by) apply(q query) (query, error) {
	if q.By != nil {
		return q, errors.New("only one \"by\" condition supported at the moment")
	}
	q.By = cn
	return q, nil
}
