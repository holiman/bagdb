package bagdb

import (
	"fmt"
	"io"
)

type Database interface {
	io.Closer

	// Put stores the data to the underlying database, and returns the key needed
	// for later accessing the data.
	// The data is copied by the database, and is safe to modify after the method returns
	Put(data []byte) uint32

	// Get retrieves the data stored at the given key.
	Get(key uint32) ([]byte, error)

	// Delete marks the data for deletion, which means it will (eventually) be
	// overwritten by other data. After calling Delete with a given key, the results
	// from doing Get(key) is undefined -- it may return the same data, or some other
	// data, or fail with an error.
	Delete(key uint32) error
}

type DB struct {
	buckets []*Bucket
	dbErr   error
}

func Open(path string, smallest, max int) (*DB, error) {
	if smallest < 128 {
		return nil, fmt.Errorf("Too small slot size: %d, need at least %d", smallest, 128)
	}
	if smallest > maxSlotSize {
		return nil, fmt.Errorf("Too large slot size: %d, max is %d", smallest, maxSlotSize)
	}
	db := &DB{}
	for v := smallest; v < max; v += v {
		bucket, err := openBucket(uint16(v), nil)
		if err != nil {
			db.Close() // Close buckets
			return nil, err
		}
		db.buckets = append(db.buckets, bucket)
	}
	return db, nil
}

func (db *DB) Put(data []byte) uint64 {
	for i, b := range db.buckets {
		if int(b.slotSize) > len(data) {
			slot, err := b.Put(data)
			if err != nil {
				panic(fmt.Sprintf("Error in Put: %v\n", err))
			}
			slot &= uint64(i) << 24
			return slot
		}
	}
	panic("whaa")
}

func (db *DB) Get(key uint64) []byte {
	id := int(key >> 24)
	return db.buckets[id].Get(key & 0x00FFFFFF)
}

func (db *DB) Delete(key uint64) error {
	id := int(key >> 24)
	db.buckets[id].Delete(key & 0x00FFFFFF)
	return nil
}

func (db *DB) Close() error {
	var err error
	for _, bucket := range db.buckets {
		if e := bucket.Close(); e != nil {
			err = e
		}
	}
	return err
}
