// bagdb: Simple datastorage
// Copyright 2021 bagdb authors
// SPDX-License-Identifier: BSD-3-Clause

package bagdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
)

// itemHeaderSize is 4 bytes: each piece of data is stored as
// [ uint16: size |  <data> ]
const (
	itemHeaderSize = 4
	maxSlotSize    = 0xffffffff
)

var ErrClosed = errors.New("bucket closed")

// A Bucket represents a collection of similarly-sized items. The bucket uses
// a number of slots, where each slot is of the exact same size.
type Bucket struct {
	id       string
	slotSize uint16       // Size of the slots, up to 65K
	tail     uint64       // First free slot
	gaps     []uint64     // A slice of indices to slots that are free to use.
	gapsMu   sync.Mutex   // Mutex for operating on the gaps slice
	f        *os.File     // The file backing the data
	fileMu   sync.RWMutex // Mutex for file operations (rw versus Close)
	closed   bool
}

// openBucket opens a (new or existing) bucket with the given slot size.
// If the bucket already exists, it's opened and read, which populates the
// internal gap-list.
// The onData callback is optional, and can be nil.
func openBucket(slotSize uint16, onData onBucketDataFn) (*Bucket, error) {
	id := fmt.Sprintf("bkt_%08d.bag", slotSize)
	f, err := os.OpenFile(id, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	var nSlots = uint64(0)
	if stat, err := f.Stat(); err != nil {
		return nil, err
	} else {
		size := stat.Size()
		nSlots = uint64((size + int64(slotSize) - 1) / int64(slotSize))
	}
	bucket := &Bucket{
		id:       id,
		slotSize: slotSize,
		tail:     nSlots,
		f:        f,
	}
	// Iterate once, this causes the gaps to be reconstructed
	bucket.Iterate(onData)
	return bucket, nil
}

func (bucket *Bucket) Close() error {
	bucket.fileMu.Lock()
	defer bucket.fileMu.Unlock()
	if bucket.closed {
		return nil
	}
	bucket.closed = true
	// Before closing the file, we overwrite all gaps with
	// blank space in the headers. Later on, when opening, we can reconstruct the
	// gaps by skimming through the slots and checking the headers.
	//
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	hdr := make([]byte, 4)
	var err error
	for _, gap := range bucket.gaps {
		if _, e := bucket.f.WriteAt(hdr, int64(gap)*int64(bucket.slotSize)); e != nil {
			err = e
		}
	}
	bucket.gaps = bucket.gaps[:0]
	bucket.f.Close()
	return err
}

// Update overwrites the existing data at the given slot. This operation is more
// efficient than Delete + Put, since it does not require managing slot availability
// but instead just overwrites in-place.
func (bucket *Bucket) Update(data []byte, slot uint64) error {
	// Write data: header + blob
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if err := bucket.writeFile(hdr, data, slot); err != nil {
		return err
	}
	return nil
}

// Put writes the given data and returns a slot identifier. The caller may
// modify the data after this method returns.
func (bucket *Bucket) Put(data []byte) (uint64, error) {
	// Validations
	if have, max := uint16(len(data)+itemHeaderSize), bucket.slotSize; have > max {
		panic(fmt.Errorf("data too large for this bucket, got %d > %d", have, max))
	}
	// Find a free slot
	slot := bucket.getSlot()
	// Write data: header + blob
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if err := bucket.writeFile(hdr, data, slot); err != nil {
		return 0, err
	}
	return slot, nil
}

// Delete marks the data at the given slot of deletion. The caller must ensure
// to NOT call this method multiple times for a given element, since doing so
// may cause (future) corruption.
// If a slot is deleted twice, the element will be in the GC-list twice, and may
// cause the next two writes to both write into the same slot.
// Delete does not touch the disk. When the bucket is Close():d, any remaining
// gaps will be marked as such in the backing file.
func (bucket *Bucket) Delete(slot uint64) {
	// Mark gap
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	bucket.gaps = append(bucket.gaps, slot)
}

// Get returns the data at the given slot. If the slot has been deleted, the returndata
// this method is undefined: it may return the original data, or some newer data
// which has been written into the slot after Delete was called.
func (bucket *Bucket) Get(slot uint64) []byte {
	data, _ := bucket.readFile(slot)
	if len(data) < itemHeaderSize {
		panic(fmt.Sprintf("too short, need %d bytes, got %d", itemHeaderSize, len(data)))
	}
	blobLen := binary.BigEndian.Uint32(data)
	if blobLen+uint32(itemHeaderSize) > uint32(len(data)) {
		panic(fmt.Sprintf("too short, need %d bytes, got %d", blobLen+itemHeaderSize, len(data)))
	}
	return data[itemHeaderSize : itemHeaderSize+blobLen]
}

func (bucket *Bucket) readFile(slot uint64) ([]byte, error) {
	buf := make([]byte, bucket.slotSize)
	// We're read-locking this to prevent the file from being closed while we're
	// reading from it
	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return nil, ErrClosed
	}
	n, _ := bucket.f.ReadAt(buf, int64(slot)*int64(bucket.slotSize))
	return buf[:n], nil
}

func (bucket *Bucket) writeFile(hdr, data []byte, slot uint64) error {
	// We're read-locking this to prevent the file from being closed while we're
	// writing to it
	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return ErrClosed
	}
	if _, err := bucket.f.WriteAt(hdr, int64(slot)*int64(bucket.slotSize)); err != nil {
		return err
	}
	if _, err := bucket.f.WriteAt(data, int64(slot)*int64(bucket.slotSize)+int64(len(hdr))); err != nil {
		return err
	}
	return nil
}

func (bucket *Bucket) getSlot() uint64 {
	var slot uint64
	// Locate a free slot
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	if nGaps := len(bucket.gaps); nGaps > 0 {
		slot = bucket.gaps[nGaps-1]
		bucket.gaps = bucket.gaps[:nGaps-1]
		return slot
	}
	// No gaps available: Expand the tail
	slot = bucket.tail
	bucket.tail++
	return slot
}

// onBucketDataFn is used to iterate the entire dataset in the bucket.
// After the method returns, the content of 'data' will be modified by
// the iterator, so it needs to be copied if it is to be used later.
type onBucketDataFn func(slot uint64, data []byte)

func (bucket *Bucket) Iterate(onData onBucketDataFn) {

	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return
	}

	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	buf := make([]byte, bucket.slotSize)
	// sort the known gaps, so we can skip them easily
	sort.Slice(bucket.gaps, func(i, j int) bool {
		return bucket.gaps[i] < bucket.gaps[j]
	})
	var (
		nextGap = uint64(0xffffffffffffffff)
		gapIdx  = 0
	)

	if len(bucket.gaps) > 0 {
		nextGap = bucket.gaps[0]
	}
	var newGaps []uint64
	for slot := uint64(0); slot < bucket.tail; slot++ {
		if slot == nextGap {
			// We've reached a gap. Skip it
			gapIdx++
			if gapIdx < len(bucket.gaps) {
				nextGap = bucket.gaps[gapIdx]
			} else {
				nextGap = 0xffffffffffffffff
			}
			continue
		}
		n, _ := bucket.f.ReadAt(buf, int64(slot)*int64(bucket.slotSize))
		if n < itemHeaderSize {
			panic(fmt.Sprintf("too short, need %d bytes, got %d", itemHeaderSize, n))
		}
		blobLen := binary.BigEndian.Uint32(buf)
		if blobLen == 0 {
			// Here's an item which has been deleted, but not marked as a gap.
			// Mark it now
			newGaps = append(newGaps, slot)
			continue
		}
		if onData == nil {
			// onData can be nil, it's used on 'Open' to reconstruct the gaps
			continue
		}
		if blobLen+uint32(itemHeaderSize) > uint32(n) {
			panic(fmt.Sprintf("too short, need %d bytes, got %d", blobLen+itemHeaderSize, n))
		}
		onData(slot, buf[itemHeaderSize:itemHeaderSize+blobLen])
	}
	bucket.gaps = append(bucket.gaps, newGaps...)
}
