//go:build testing

package benchmarks

import (
	"context"
	"github.com/source-c/go-ignit-thin"
	"os"
	"strconv"
	"strings"
	"testing"
)

const (
	EnvWarmupCount   = "WARMUPS"
	EnvIgniteHosts   = "IGNITE_HOSTS"
	defaultWarmupCnt = 3
)

func CacheBenchmarker(b *testing.B, cliCreate func() (*ignite.Client, *ignite.Cache), fixture func(c *ignite.Cache), f func(b *testing.B, c *ignite.Cache)) {
	warmups := warmupCount()
	if warmups > 0 {
		b.Logf("Warmups: %d", warmups)
	}
	runner := func(b *testing.B, smart bool) {
		cli, cache := cliCreate()
		defer func() {
			if err := cli.Close(context.Background()); err != nil {
				b.Log("Test warning, client not shutdown", err)
			}
		}()
		if fixture != nil {
			fixture(cache)
		}
		b.ResetTimer()
		f(b, cache)
	}
	warmJvmUp := func() {
		client, cache := cliCreate()
		defer func() {
			_ = client.Close(context.Background())
		}()
		for i := 0; i < warmups; i++ {
			f(b, cache)
		}
	}
	warmJvmUp()
	runner(b, true)
}

func warmupCount() int {
	if s := getEnv(EnvWarmupCount); len(s) > 0 {
		if i, err := strconv.ParseInt(s, 10, 32); err != nil {
			panic(err)
		} else {
			return int(i)
		}
	}
	return defaultWarmupCnt
}

func IgniteHosts() []string {
	if s := getEnv(EnvIgniteHosts); len(s) > 0 {
		return strings.Split(s, ";")
	}
	return []string{"localhost"}
}

func getEnv(name string) string {
	if s := os.Getenv(name); len(s) > 0 {
		s = strings.TrimSpace(s)
		return s
	}
	return ""
}
