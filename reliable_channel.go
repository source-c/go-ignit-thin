package ignite

import (
	"context"
	"errors"
	"fmt"
	"github.com/source-c/go-ignit-thin/logger"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type reliableChannel struct {
	attemptsLimit int
	currCh        atomic.Value
	cfg           *clientConfiguration
	mux           sync.Mutex
	closed        atomic.Bool
	log           *logger.Logger
}

func (r *reliableChannel) send(ctx context.Context, opCode int16, requestWriter func(output BinaryOutputStream) error, responseReader func(input BinaryInputStream, err error)) {
	if r.closed.Load() {
		responseReader(nil, createClientConnectionError("channel is closed", nil))
		return
	}
	connectFailed := false
	attemptsCnt := 0
	for {
		currCh, err := r.currentChannel(ctx)
		if err != nil {
			r.log.Errorf("connection failed: %s", err)
			responseReader(NewBinaryInputStream(nil, 0), err)
			return
		}
		attemptsLimit := r.attemptsLimit
		attemptsCnt++
		currCh.send(ctx, opCode, requestWriter, func(input BinaryInputStream, err error) {
			var connErr *ClientConnectionError
			if errors.As(err, &connErr) {
				connectFailed = true
			}
			if !connectFailed || attemptsCnt == attemptsLimit {
				responseReader(input, err)
			}
		})
		if !connectFailed || attemptsCnt == attemptsLimit {
			return
		}
		r.log.Debug(func() string {
			return fmt.Sprintf("retrying operation, retries left: %d", attemptsLimit-attemptsCnt)
		})
	}
}

func (r *reliableChannel) currentChannel(ctx context.Context) (*tcpChannel, error) {
	if r.closed.Load() {
		return nil, errors.New("channel is closed")
	}
	for {
		currCh := r.currCh.Load()
		if currCh != nil && !currCh.(*tcpChannel).isClosed() {
			return currCh.(*tcpChannel), nil
		}
		if err := r.initConnection(ctx); err != nil {
			return nil, err
		}
	}
}

func (r *reliableChannel) protocolContext() *ProtocolContext {
	currCh := r.currCh.Load()
	if currCh != nil {
		return currCh.(*tcpChannel).protocolContext()
	}
	return nil
}

func (r *reliableChannel) close(ctx context.Context) {
	if r.closed.CompareAndSwap(false, true) {
		r.mux.Lock()
		defer r.mux.Unlock()
		r.log.Infof("closing connection to %s", r.currCh.Load())
		currCh := r.currCh.Load()
		if currCh != nil {
			currCh.(*tcpChannel).close(ctx)
		}
		r.closed.Store(true)
	}
}

func (r *reliableChannel) isClosed() bool {
	return r.closed.Load()
}

func (r *reliableChannel) initConnection(ctx context.Context) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	oldCh := r.currCh.Load()
	if oldCh != nil && !oldCh.(*tcpChannel).isClosed() {
		return nil
	}
	addresses, err := r.cfg.addressesSupplier(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain addresses: %w", err)
	}
	if len(addresses) == 0 {
		return errors.New("addresses are empty")
	}
	if len(addresses) > 1 && r.cfg.shuffleAddresses {
		rand.New(rand.NewSource(time.Now().UnixNano()))
		rand.Shuffle(len(addresses), func(i, j int) {
			addresses[i], addresses[j] = addresses[j], addresses[i]
		})
	}
	var cliConnErr *ClientConnectionError
	for i := 0; i < len(addresses); i++ {
		if oldCh != nil && oldCh.(*tcpChannel).addr == addresses[i] {
			if i == len(addresses)-1 {
				break
			} else {
				continue
			}
		}
		r.log.Debug(func() string {
			return fmt.Sprintf("trying to init connection to %s", addresses[i])
		})
		var ch *tcpChannel
		ch, err = createTcpChannel(ctx, addresses[i], r.cfg)
		if err == nil {
			if r.cfg.retryLimit > 0 && r.cfg.retryLimit < len(addresses) {
				r.attemptsLimit = r.cfg.retryLimit
			} else {
				r.attemptsLimit = len(addresses)
			}
			r.currCh.Store(ch)
			r.log.Debug(func() string {
				return fmt.Sprintf("successfully connected to %s", ch)
			})
			return nil
		} else if i == len(addresses)-1 || !errors.As(err, &cliConnErr) {
			break
		}
	}
	if err != nil && errors.As(err, &cliConnErr) {
		return err
	} else if isHandshakeError(err) {
		return err
	} else {
		return &ClientConnectionError{ClientError{
			Message: fmt.Sprintf("connection failed to channels [%s]", strings.Join(addresses, ", ")),
		}, err}
	}
}

func isHandshakeError(err error) bool {
	var cliAuthErr *ClientAuthenticationError
	var protoErr *ClientProtocolError
	return err != nil && (errors.As(err, &cliAuthErr) || errors.As(err, &protoErr))
}

func createReliableChannel(ctx context.Context, cfg *clientConfiguration) (channel, error) {
	if cfg.addressesSupplier == nil {
		return nil, errors.New("address supplier is nil")
	}
	ret := &reliableChannel{
		cfg: cfg,
		log: cfg.logger,
	}
	if err := ret.initConnection(ctx); err != nil {
		ret.log.Errorf("connection failed: %s", err)
		return nil, err
	}
	return ret, nil
}
