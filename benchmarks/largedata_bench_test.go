package benchmarks

import (
	"bytes"
	"context"
	"fmt"
	"github.com/source-c/go-ignit-thin"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"math/rand"
	"testing"
	"time"
)

const kb = 1024

func Benchmark_PutGet_Bytearray(b *testing.B) {
	sizes := []int{1, 4, 8, 16}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("Benchmark Put/Get bytearray %dk", sz), func(b *testing.B) {
			benchBinaryPutGet(b, sz*kb)
		})
	}
}

func Benchmark_PutGet_Bytearray_Large(b *testing.B) {
	sizes := []int{64, 128}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("Benchmark Put/Get bytearray large %dk", sz), func(b *testing.B) {
			benchBinaryPutGet(b, sz*kb)
		})
	}
}

func benchBinaryPutGet(b *testing.B, payloadSz int) {
	cliCreate := func() (*ignite.Client, *ignite.Cache) {
		cli, err := ignite.Start(context.Background(), ignite.WithAddresses(IgniteHosts()...))
		if err != nil {
			b.Fatalf("failed to connect to cluster")
		}
		cache, err := cli.GetOrCreateCache(context.Background(), "bench")
		if err != nil {
			b.Fatalf("failed to create cache")
		}
		return cli, cache
	}

	CacheBenchmarker(b, cliCreate, nil, func(b *testing.B, c *ignite.Cache) {
		payload := testing2.MakeByteArrayPayload(payloadSz)
		b.RunParallel(func(pb *testing.PB) {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				key := rnd.Int63n(1 << 20)
				err := c.Put(context.Background(), key, payload)
				if err != nil {
					b.Error("failed to put value", err)
					continue
				}
				val, err := c.Get(context.Background(), key)
				if err != nil {
					b.Error("failed to get value", err)
					continue
				}
				if !bytes.Equal(payload, val.([]byte)) {
					panic("value not equals to payload")
				}
			}
		})
	})
}
