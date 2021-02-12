package main

import (
	"context"
	"fmt"
	"github.com/holiman/bagdb"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func fuzzDeletePutGet(ctx context.Context) {
	db, _ := bagdb.Open(".", 10, 300, nil)
	var wg sync.WaitGroup

	adds := make(chan uint64, 2048)

	var addCount uint64
	var delCount uint64
	var t0 = time.Now()
	// Deleter
	deleteFn := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case key := <-adds:
				if err := db.Delete(key); err != nil {
					panic(err)
				}
				atomic.AddUint64(&delCount, 1)
			}
		}
	}
	wg.Add(5)
	go deleteFn()
	go deleteFn()
	go deleteFn()
	go deleteFn()
	go deleteFn()

	// Setter
	setFn := func() {
		defer wg.Done()
		size := 10 + byte(rand.Int())
		val := make([]byte, int(size))
		val[0] = size
		val[len(val)-1] = size
		for {
			key := db.Put(val)
			select {
			case <-ctx.Done():
				return
			case adds <- key:
				atomic.AddUint64(&addCount, 1)

			}
		}
	}
	wg.Add(2)
	go setFn()
	go setFn()
	// reporter
	reportFn := func() {
		defer wg.Done()

		for {
			a := atomic.LoadUint64(&addCount)
			d := atomic.LoadUint64(&delCount)
			fmt.Printf("Added %d entries, deleted %d entries, in %v\n", a, d, time.Since(t0))
			time.Sleep(1 * time.Second)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
	wg.Add(1)
	go reportFn()
	/*
		// Getter
		wg.Add(1)
		getFn := func() {
			defer wg.Done()
			var (
				val    = make([]byte, 1024)
				hits   = uint64(0)
				misses = uint64(0)
			)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					r := byte(rand.Int())
					key := fmt.Sprintf("thekey%d", r)

					for j := 0; j < len(val); j++ {
						val[j] = r
					}
					if got, err := cache.Get(key); err == nil && !bytes.Equal(got, val) {
						errStr := fmt.Sprintf("got %s ->\n %x\n expected:\n %x\n ", key, got, val)
						panic(errStr)
					} else {
						if err == nil {
							hits++
						} else {
							misses++
						}
					}
					if total := hits + misses; total%1000000 == 0 {
						percentage := float64(100) * float64(hits) / float64(total)
						fmt.Printf("Hits %d (%.2f%%) misses %d \n", hits, percentage, misses)
					}
				}
			}
		}

	*/

	wg.Wait()
	db.Close()
	fmt.Printf("Added %d entries, deleted %d entries, in %v\n", addCount, delCount, time.Since(t0))
}
func main() {

	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Press ctrl-c to exit")
	go fuzzDeletePutGet(ctx)

	<-sigs
	fmt.Println("Exiting...")
	cancel()

}
