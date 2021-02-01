package bagdb

import (
	"fmt"
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

func TestBucket(t *testing.T) {
	b, err := openBucket(200, nil)
	defer b.Close()
	if err != nil {
		t.Fatal(err)
	}
	aa, _ := b.Put(getBlob(0x0a, 150))
	fmt.Printf("Placed the data into slot: %x\n", aa)
	bb, _ := b.Put(getBlob(0x0b, 150))
	fmt.Printf("Placed the data into slot: %x\n", bb)
	cc, _ := b.Put(getBlob(0x0c, 150))
	fmt.Printf("Placed the data into slot: %x\n", cc)
	dd, err := b.Put(getBlob(0x0d, 150))
	fmt.Printf("Placed the data into slot: %x\n", dd)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Slot: %x\n", dd)
	if err := checkBlob(0x0a, b.Get(aa), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0b, b.Get(bb), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0c, b.Get(cc), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0d, b.Get(dd), 150); err != nil {
		t.Fatal(err)
	}

	b.Delete(bb)
	b.Delete(cc)
	b.Delete(aa)
	//b.Delete(1)

	b.Iterate(func(slot uint64, data []byte) {
		fmt.Printf("Slot %d appears to contain %d bytes of %x\n", slot, len(data), data[0])
	})
}
