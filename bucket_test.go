// bagdb: Simple datastorage
// Copyright 2021 bagdb authors
// SPDX-License-Identifier: BSD-3-Clause

package bagdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func getBlob(fill byte, size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = fill
	}
	return buf
}

func checkBlob(fill byte, blob []byte, size int) error {
	if len(blob) != size {
		return fmt.Errorf("wrong size: got %d, want %d", len(blob), size)
	}
	for i := range blob {
		if blob[i] != fill {
			return fmt.Errorf("wrong data, byte %d: got %x want %x", i, blob[i], fill)
		}
	}
	return nil
}

func TestBasics(t *testing.T) {
	b, cleanup := setup(t)
	defer cleanup()
	aa, _ := b.Put(getBlob(0x0a, 150))
	bb, _ := b.Put(getBlob(0x0b, 151))
	cc, _ := b.Put(getBlob(0x0c, 152))
	dd, err := b.Put(getBlob(0x0d, 153))

	if err != nil {
		t.Fatal(err)
	}
	get := func(slot uint64) []byte {
		t.Helper()
		data, err := b.Get(slot)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if err := checkBlob(0x0a, get(aa), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0b, get(bb), 151); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0c, get(cc), 152); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0d, get(dd), 153); err != nil {
		t.Fatal(err)
	}
	// Same checks, but during iteration
	b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
	})
	// Delete item and place a new one there
	b.Delete(bb)
	// Iteration should skip over deleted items
	b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
		if slot == bb {
			t.Fatalf("Expected not to iterate %d", bb)
		}
	})
	ee, _ := b.Put(getBlob(0x0e, 154))
	if err := checkBlob(0x0e, get(ee), 154); err != nil {
		t.Fatal(err)
	}
	// Update in place
	if err := b.Update(getBlob(0x0f, 35), ee); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0f, get(ee), 35); err != nil {
		t.Fatal(err)
	}
	b.Delete(aa)
	b.Delete(ee)
	b.Delete(cc)
	b.Delete(dd)
	// Iteration should be a no-op
	b.Iterate(func(slot uint64, data []byte) {
		t.Fatalf("Expected no iteration")
	})
}

func writeBucketFile(name string, size int, slotData []byte) error {
	var bucketData = make([]byte, len(slotData)*size)
	// Fill all the items
	for i, byt := range slotData {
		if byt == 0 {
			continue
		}
		data := getBlob(byt, size-itemHeaderSize)
		// write header
		binary.BigEndian.PutUint32(bucketData[i*size:], uint32(size-itemHeaderSize))
		// write data
		copy(bucketData[i*size+itemHeaderSize:], data)
	}
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	f.Write(bucketData)
	return nil
}

func checkIdentical(fileA, fileB string) error {
	var (
		dataA []byte
		dataB []byte
		err   error
	)
	dataA, err = ioutil.ReadFile(fileA)
	if err != nil {
		return fmt.Errorf("failed to open %v: %v", fileA, err)
	}
	dataB, err = ioutil.ReadFile(fileB)
	if err != nil {
		return fmt.Errorf("failed to open %v: %v", fileB, err)
	}
	if !bytes.Equal(dataA, dataB) {
		return fmt.Errorf("data differs: \n%x\n%x", dataA, dataB)
	}
	return nil
}

func setup(t *testing.T) (*Bucket, func()) {
	t.Helper()
	bName := fmt.Sprintf("%v.bucket", t.Name())
	a, err := openBucketAs(bName, 200, nil)
	if err != nil {
		t.Fatal(err)
	}
	return a, func() {
		a.Close()
		os.Remove(bName)
	}
}

// TestOversized
// - Test writing oversized data into a bucket
// - Test writing exactly-sized data into a bucket
func TestOversized(t *testing.T) {
	a, cleanup := setup(t)
	defer cleanup()

	for s := 190; s < 205; s++ {
		data := getBlob('x', s)
		slot, err := a.Put(data)
		if err != nil {
			if slot != 0 {
				t.Fatalf("Exp slot 0 on error, got %d", slot)
			}
			if have := s + itemHeaderSize; have <= int(a.slotSize) {
				t.Fatalf("expected to store %d bytes of data, got error", have)
			}
		} else {
			if have := s + itemHeaderSize; have > int(a.slotSize) {
				t.Fatalf("expected error storing %d bytes of data", have)
			}
		}
	}
}

// TestErrOnClose
// - Tests reading, writing, deleting from a closed bucket
func TestErrOnClose(t *testing.T) {
	a, cleanup := setup(t)
	defer cleanup()
	// Write something and delete it again, to have a gap
	if have, want := a.tail, uint64(0); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.tail, uint64(1); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.tail, uint64(2); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	a.Delete(0)

	a.Close()
	a.Close() // Double-close should be a no-op
	if _, err := a.Put(make([]byte, 3)); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected error for Put on closed bucket, got %v", err)
	}
	if _, err := a.Get(0); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected error for Get on closed bucket, got %v", err)
	}
	// Only expectation here is not to panic, basically
	a.Delete(0)
	a.Delete(1)
	a.Delete(1000)
}

func TestBadInput(t *testing.T) {
	a, cleanup := setup(t)
	defer cleanup()

	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))

	if _, err := a.Get(uint64(0x000000FFFFFFFFFF)); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if _, err := a.Get(uint64(0xFFFFFFFFFFFFFFFF)); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if err := a.Delete(0x000FFFF); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if _, err := a.Put(nil); !errors.Is(err, ErrEmptyData) {
		t.Fatalf("expected %v", ErrEmptyData)
	}
	if _, err := a.Put(make([]byte, 0)); !errors.Is(err, ErrEmptyData) {
		t.Fatalf("expected %v", ErrEmptyData)
	}
}

func TestCompaction(t *testing.T) {
	var (
		a   *Bucket
		b   *Bucket
		err error
	)
	if err = writeBucketFile("a", 10, []byte{1, 0, 3, 0, 5, 0, 6, 0, 4, 0, 2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("a")
	if err = writeBucketFile("b", 10, []byte{1, 2, 3, 4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("b")
	// The order and content that we expect for the onData
	expOnData := []byte{1, 2, 3, 4, 5, 6}
	var haveOnData []byte
	onData := func(slot uint64, data []byte) {
		haveOnData = append(haveOnData, data[0])
	}
	/// Now open them as buckets
	a, err = openBucketAs("a", 10, onData)
	if err != nil {
		t.Fatal(err)
	}
	b, err = openBucketAs("b", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	b.Close()
	// Check the content of the files
	if err := checkIdentical("a", "b"); err != nil {
		t.Fatal(err)
	}
	// And the content of the onData callback
	if !bytes.Equal(expOnData, haveOnData) {
		t.Fatalf("onData wrong, expected \n%x\ngot\n%x\n", expOnData, haveOnData)
	}
}

// TODO tests
// - Test Put / Delete in parallel
// - Test that simultaneous filewrites to different parts of the file don't cause problems
