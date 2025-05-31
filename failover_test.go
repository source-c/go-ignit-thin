package ignite

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	testing2 "github.com/source-c/go-ignit-thin/internal/testing"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type FailoverTestSuite struct {
	testing2.IgniteTestSuite
}

func TestFailoverTestSuite(t *testing.T) {
	suite.Run(t, new(FailoverTestSuite))
}

func (suite *FailoverTestSuite) TearDownSuite() {
	suite.KillAllGrids()
}

func (suite *FailoverTestSuite) AfterTest() {
	suite.KillAllGrids()
}

func (suite *FailoverTestSuite) TestFailover() {
	_, err := suite.StartIgnite()
	if err != nil {
		suite.T().Fatal("failed to start first grid", err)
	}
	_, err = suite.StartIgnite()
	if err != nil {
		suite.T().Fatal("failed to start second grid", err)
	}

	cli, err := StartTestClient(context.Background(), WithAddresses(defaultAddress+":10800", defaultAddress+":10801"), WithShuffleAddresses(false))
	if err != nil {
		suite.T().Fatal("Failed to start client", err)
		return
	}
	defer func() {
		_ = cli.Close(context.Background())
	}()
	cache, err := cli.GetOrCreateCache(context.Background(), "test")
	if err != nil {
		suite.T().Fatal("Failed to create cache", err)
	}
	var errCnt atomic.Int64
	var successCnt atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		LOOP:
			for {
				select {
				case <-stopCh:
					return
				default:
					key := rnd.Int63n(1 << 15)
					err0 := cache.Put(context.Background(), fmt.Sprintf("key-%d", key), "test")
					if err0 != nil {
						suite.T().Log("put failed", err0)
						errCnt.Add(1)
					} else {
						successCnt.Add(1)
					}
					continue LOOP
				}
			}
		}()
	}
	testing2.WaitForCondition(func() bool {
		return successCnt.Load() >= 1000
	}, 3*time.Second)
	_ = suite.KillIgnite(0)
	currOk := successCnt.Load()
	testing2.WaitForCondition(func() bool {
		return successCnt.Load() > 2*currOk
	}, 3*time.Second)
	close(stopCh)
	wg.Wait()
	suite.T().Logf("Total errors %d, total ok %d", errCnt.Load(), successCnt.Load())
	suite.Assert().True(errCnt.Load() == 0, fmt.Sprintf("Total errors %d, total ok %d", errCnt.Load(), successCnt.Load()))
}
