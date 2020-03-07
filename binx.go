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
		Close() error
		Update(func(Reader, Writer) error) error
		View(func(Reader) error) error
	}

	Reader interface {
		Get(q Queryable, key []byte) error
		List(qs QueryableSlice, cnd ...Condition) error
		Last(q Queryable, cnd ...Condition) error
		First(q Queryable, cnd ...Condition) error
	}

	Writer interface {
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
		where Index
		by    Index
		skip  int
		limit int
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

func (s *db) Update(fn func(r Reader, w Writer) error) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return fn(&reader{tx}, &writer{tx})
	})
}

func (s *db) View(fn func(w Reader) error) error {
	return s.DB.View(func(tx *bolt.Tx) error {
		return fn(Reader(&reader{tx}))
	})
}

func (cn where) apply(q query) (query, error) {
	if q.where != nil {
		return q, errors.New("only one \"where\" condition supported at the moment")
	}
	q.where = cn
	return q, nil
}
func (cn limit) apply(q query) (query, error) {
	if q.limit != 0 {
		return q, errors.New("only one \"limit\" condition supported at the moment")
	}
	q.limit = cn.int
	return q, nil
}
func (cn skip) apply(q query) (query, error) {
	if q.skip != 0 {
		return q, errors.New("only one \"skip\" condition supported at the moment")
	}
	q.skip = cn.int
	return q, nil
}
func (cn by) apply(q query) (query, error) {
	if q.by != nil {
		return q, errors.New("only one \"by\" condition supported at the moment")
	}
	q.by = cn
	return q, nil
}
