// bagdb: Simple datastorage
// Copyright 2021 bagdb authors
// SPDX-License-Identifier: BSD-3-Clause

package bagdb

import (
	"bytes"
	"encoding/binary"
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

// TODO tests
// - Test on a bucket with a few holes, that the holes are detected as gaps during open
// - Test writing oversized data into a bucket
// - Test writing exactly-sized data into a bucket
// - Test that Close properly writes the holes
// - Test Put / Delete in parallel
// - Test that simultaneous filewrites to different parts of the file don't cause problems
// - Test that deletions properly truncate the file
func TestBucket(t *testing.T) {
	b, err := openBucket(200, nil)
	defer b.Close()
	if err != nil {
		t.Fatal(err)
	}
	aa, _ := b.Put(getBlob(0x0a, 150))
	fmt.Printf("Placed the data into slot: %d\n", aa)
	bb, _ := b.Put(getBlob(0x0b, 150))
	fmt.Printf("Placed the data into slot: %d\n", bb)
	cc, _ := b.Put(getBlob(0x0c, 150))
	fmt.Printf("Placed the data into slot: %d\n", cc)
	dd, err := b.Put(getBlob(0x0d, 150))
	fmt.Printf("Placed the data into slot: %d\n", dd)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Slot: %x\n", dd)
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
	if err := checkBlob(0x0b, get(bb), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0c, get(cc), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0d, get(dd), 150); err != nil {
		t.Fatal(err)
	}

	b.Delete(bb)
	b.Delete(cc)
	b.Delete(aa)
	b.Delete(dd)

	b.Iterate(func(slot uint64, data []byte) {
		fmt.Printf("Slot %d appears to contain %d bytes of %x\n", slot, len(data), data[0])
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
	b, err = openBucketAs("b", 10, nil)
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
