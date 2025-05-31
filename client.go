package ignite

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/source-c/go-ignit-thin/internal"
	"github.com/source-c/go-ignit-thin/logger"
	"net"
	"strconv"
	"strings"
	"time"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=ErrorCode -output=errorcode_string.go
type ErrorCode uint

const (
	Success               ErrorCode = 0
	Failed                ErrorCode = 1
	InvalidOpCode         ErrorCode = 2
	InvalidNodeState      ErrorCode = 10
	FunctionalityDisabled ErrorCode = 100
	CacheDoesNotExists    ErrorCode = 1000
	CacheExists           ErrorCode = 1001
	CacheConfigInvalid    ErrorCode = 1002
	TooManyCursors        ErrorCode = 1010
	ResourceDoesNotExists ErrorCode = 1011
	SecurityViolation     ErrorCode = 1012
	TxLimitExceeded       ErrorCode = 1020
	TxNotFound            ErrorCode = 1021
	TooManyComputeTasks   ErrorCode = 1030
	AuthFailed            ErrorCode = 2000
)

type ClientError struct {
	Message string
}

type ClientConnectionError struct {
	ClientError
	err error
}

type ClientProtocolError struct {
	ClientError
}

type ClientAuthenticationError struct {
	ClientError
}

type ClientServerError struct {
	ClientError
	Code ErrorCode
}

func (err *ClientError) Error() string {
	return err.Message
}

func (err *ClientProtocolError) Error() string {
	return err.Message
}

func (err *ClientAuthenticationError) Error() string {
	return err.Message
}

func (err *ClientServerError) Error() string {
	return fmt.Sprintf("%s: %s", err.Code, err.Message)
}

func (err *ClientConnectionError) Error() string {
	msg := err.Message
	if len(msg) == 0 {
		msg = "connection failed"
	}
	if err.err != nil {
		return fmt.Sprintf("%s: %s", msg, err.err)
	}
	return msg
}

// Client is a handle representing connections to an Apache Ignite cluster. It is safe for concurrent use
// by multiple goroutines.
type Client struct {
	cfg   *clientConfiguration
	ch    channel
	marsh marshaller
}

const (
	defaultPort                        = 10800
	opCacheGetNames              int16 = 1050
	opCacheCreateWithName        int16 = 1051
	opCacheGetOrCreateWithName   int16 = 1052
	opCacheCreateWithConfig      int16 = 1053
	opCacheGetOrCreateWithConfig int16 = 1054
	opCacheDestroy               int16 = 1056
)

// CacheNames returns slice of cache names currently presents in Apache Ignite cluster.
func (cli *Client) CacheNames(ctx context.Context) ([]string, error) {
	var err error = nil
	var names []string
	cli.ch.send(ctx, opCacheGetNames, func(output BinaryOutputStream) error {
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		sz := int(input.ReadInt32())
		names = make([]string, sz)
		for i := 0; i < sz; i++ {
			var name string
			name, err0 = unmarshalString(input)
			if err0 != nil {
				err = err0
				return
			}
			names[i] = name
		}
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

// CreateCache creates cache with default configuration with specified name, returns [Cache] instance or error if failed.
func (cli *Client) CreateCache(ctx context.Context, name string) (*Cache, error) {
	name = strings.TrimSpace(name)
	if len(name) == 0 {
		return nil, errors.New("cache name is empty")
	}
	var err error
	cli.ch.send(ctx, opCacheCreateWithName, func(output BinaryOutputStream) error {
		marshalString(output, name)
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	if err != nil {
		return nil, err
	}

	return cli.newCache(name), nil
}

// CreateCacheWithConfiguration creates with specified [CacheConfiguration], returns [Cache] instance or error if failed.
func (cli *Client) CreateCacheWithConfiguration(ctx context.Context, config CacheConfiguration) (*Cache, error) {
	var err error
	cli.ch.send(ctx, opCacheCreateWithConfig, func(output BinaryOutputStream) error {
		if err0 := config.marshal(ctx, cli.marsh, output); err0 != nil {
			return err0
		}
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	if err != nil {
		return nil, err
	}
	return cli.newCache(config.Name()), nil
}

// GetOrCreateCache returns already started cache or creates new one with specified name, returns [Cache] instance or error if failed.
func (cli *Client) GetOrCreateCache(ctx context.Context, name string) (*Cache, error) {
	name = strings.TrimSpace(name)
	if len(name) == 0 {
		return nil, errors.New("cache name is empty")
	}
	var err error
	cli.ch.send(ctx, opCacheGetOrCreateWithName, func(output BinaryOutputStream) error {
		if err0 := cli.marsh.marshal(ctx, output, name); err0 != nil {
			return err0
		}
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	if err != nil {
		return nil, err
	}
	return cli.newCache(name), nil
}

// GetOrCreateCacheWithConfiguration returns already started cache or creates new one with specified [CacheConfiguration], returns [Cache] instance or error if failed.
func (cli *Client) GetOrCreateCacheWithConfiguration(ctx context.Context, config CacheConfiguration) (*Cache, error) {
	var err error
	cli.ch.send(ctx, opCacheGetOrCreateWithConfig, func(output BinaryOutputStream) error {
		if err0 := config.marshal(ctx, cli.marsh, output); err0 != nil {
			return err0
		}
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	if err != nil {
		return nil, err
	}

	return cli.newCache(config.Name()), nil
}

// DestroyCache destroys cache with specified name.
func (cli *Client) DestroyCache(ctx context.Context, name string) error {
	var err error
	cli.ch.send(ctx, opCacheDestroy, func(output BinaryOutputStream) error {
		output.WriteInt32(internal.HashCode(name))
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		}
	})
	return err
}

func (cli *Client) newCache(name string) *Cache {
	return &Cache{
		cli:  cli,
		name: name,
		id:   internal.HashCode(name),
	}
}

// WithAffinityKeyName sets affinity key field for binary object.
func WithAffinityKeyName(affKeyName string) func(opt *binaryObjectOptions) {
	return func(opt *binaryObjectOptions) {
		affKeyName = strings.TrimSpace(affKeyName)
		if len(affKeyName) == 0 {
			return
		}
		opt.affKeyName = affKeyName
	}
}

// WithField sets field with specified name and value for binary object.
func WithField(name string, value interface{}) func(opt *binaryObjectOptions) {
	return func(opt *binaryObjectOptions) {
		name = strings.TrimSpace(name)
		if len(name) == 0 {
			return
		}
		_, ok := opt.fields[name]
		if !ok {
			opt.fieldsOrder = append(opt.fieldsOrder, name)
			opt.fields[name] = &boField{
				typeId: -1,
				value:  value,
			}
		}
	}
}

// WithNullField sets field with specified name, type and nil value for binary object.
func WithNullField(name string, typeId TypeDesc) func(opt *binaryObjectOptions) {
	return func(opt *binaryObjectOptions) {
		name = strings.TrimSpace(name)
		if len(name) == 0 {
			return
		}
		_, ok := opt.fields[name]
		if !ok {
			opt.fieldsOrder = append(opt.fieldsOrder, name)
			opt.fields[name] = &boField{
				typeId: int32(typeId),
				value:  nil,
			}
		}
	}
}

// CreateBinaryObject creates BinaryObject with specified type name and options.
func (cli *Client) CreateBinaryObject(ctx context.Context, typeName string, opts ...func(*binaryObjectOptions)) (BinaryObject, error) {
	typeName = strings.TrimSpace(typeName)
	if len(typeName) == 0 {
		return nil, fmt.Errorf("type name is empty")
	}
	boOpts := &binaryObjectOptions{
		typeName:     typeName,
		fields:       make(map[string]*boField),
		fieldsOrder:  make([]string, 0),
		isRegistered: true, // should be true by default, false only for testing
	}
	for _, opt := range opts {
		opt(boOpts)
	}
	return newBinaryObject(ctx, cli.marsh, boOpts)
}

// Version returns current connection protocol version.
func (cli *Client) Version() (string, error) {
	ver := cli.ch.protocolContext().Version()
	return ver.String(), nil
}

// Close closes client, waits until all underlying chores are done.
func (cli *Client) Close(ctx context.Context) error {
	cli.ch.close(ctx)
	return nil
}

type clientConfiguration struct {
	addressesSupplier      func(ctx context.Context) ([]string, error)
	shuffleAddresses       bool
	user                   string
	password               string
	attrs                  map[string]string
	tlsConfigSupplier      func() (*tls.Config, error)
	requestTimeout         time.Duration
	retryLimit             int
	logger                 *logger.Logger
	protocolContext        *ProtocolContext
	enableAutoBinaryConfig bool
	compactFooter          bool
	binaryIdMapper         BinaryIdMapper
}

type ClientConfigurationOption func(config *clientConfiguration) error

// WithAddressSupplier returns [ClientConfigurationOption] that sets address supplier. The supplier must return slice of addresses of
// ignite nodes or error if failed.
//
// WARNING: Adding result of [WithAddresses] after this will override this [ClientConfigurationOption] and vice versa.
func WithAddressSupplier(supplier func(ctx context.Context) ([]string, error)) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if supplier == nil {
			return errors.New("nil address supplier")
		}
		config.addressesSupplier = supplier
		return nil
	}
}

// WithShuffleAddresses returns [ClientConfigurationOption] that sets whether addresses of ignite nodes will be shuffled after
// obtaining from addresses supplier. See also: [WithAddressSupplier]
func WithShuffleAddresses(shuffle bool) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		config.shuffleAddresses = shuffle
		return nil
	}
}

// WithAddresses returns [ClientConfigurationOption] that sets addresses of ignite nodes to connect.
//
// WARNING: Adding result of [WithAddressSupplier] after this will override this [ClientConfigurationOption] and vice versa.
func WithAddresses(addresses ...string) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if len(addresses) == 0 {
			return errors.New("empty addressed supplied")
		}
		preparedAddrs := make([]string, 0)
		for _, addr := range addresses {
			_, _, err := net.SplitHostPort(addr)
			if err != nil {
				addr = net.JoinHostPort(addr, strconv.Itoa(defaultPort))
			}
			_, _, err = net.SplitHostPort(addr)
			if err != nil {
				return err
			}
			preparedAddrs = append(preparedAddrs, addr)
		}
		config.addressesSupplier = func(_ context.Context) ([]string, error) {
			return preparedAddrs, nil
		}
		return nil
	}
}

// WithCredentials returns [ClientConfigurationOption] that sets credentials (username and password) used to authenticate client.
func WithCredentials(username string, password string) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if len(username) != 0 && len(password) != 0 {
			config.user = username
			config.password = password
		}
		return nil
	}
}

// WithTls returns [ClientConfigurationOption] that sets TLS configuration supplier. The supplier must return tls configuration or error if failed.
func WithTls(supplier func() (*tls.Config, error)) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if supplier == nil {
			return errors.New("nil tls configuration supplier")
		}
		config.tlsConfigSupplier = supplier
		return nil
	}
}

// WithRequestTimeout returns [ClientConfigurationOption] that sets requests timeout. Setting zero or negative duration means no timeout.
func WithRequestTimeout(timeout time.Duration) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if timeout <= 0 {
			config.requestTimeout = 0
		} else {
			config.requestTimeout = timeout
		}
		return nil
	}
}

// WithClientAttribute returns [ClientConfigurationOption] that adds key-value pair to optional client connection attributes.
func WithClientAttribute(key string, value string) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if len(key) != 0 && len(value) != 0 {
			if config.attrs == nil {
				config.attrs = make(map[string]string)
			}
			config.attrs[key] = value
		}
		return nil
	}
}

// WithLoggingSink return [ClientConfigurationOption] that configures client logging by setting logger sink.
// See also [logger]
func WithLoggingSink(sink logger.Sink) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		if sink == nil {
			return nil
		}
		config.logger = &logger.Logger{Sink: sink}
		return nil
	}
}

// WithProtocolContext return [ClientConfigurationOption] that sets initial client protocol version and protocol features.
// See [ProtocolContext] for details.
func WithProtocolContext(version ProtocolVersion, features ...AttributeFeature) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		config.protocolContext = NewProtocolContext(version, features...)
		return nil
	}
}

// WithDisabledAutoBinaryConfiguration returns [ClientConfigurationOption] that disables automatic retrieving the current
// binary configuration from the cluster.
func WithDisabledAutoBinaryConfiguration() ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		config.enableAutoBinaryConfig = false
		return nil
	}
}

// WithBinaryCompactFooter returns [ClientConfigurationOption] that sets compact footer support to binary object serialization.
// It is set to true by default.
func WithBinaryCompactFooter(isCompact bool) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		config.compactFooter = isCompact
		return nil
	}
}

// WithBinaryIdMapper returns [ClientConfigurationOption] that sets custom [BinaryIdMapper] to binary object serialization.
// [BinaryBasicIdMapper] is set by default.
func WithBinaryIdMapper(mapper BinaryIdMapper) ClientConfigurationOption {
	return func(config *clientConfiguration) error {
		config.binaryIdMapper = mapper
		return nil
	}
}

// Start creates and initializes a new Client.
// Passing opts parameter allows user to configure Client to be created.
func Start(ctx context.Context, opts ...ClientConfigurationOption) (*Client, error) {
	cfg := clientConfiguration{
		requestTimeout:   0,
		retryLimit:       0,
		shuffleAddresses: true,
		protocolContext: NewProtocolContext(
			ProtocolVersion{1, 7, 0},
			UserAttributesFeature,
			BinaryConfigurationFeature,
		),
		compactFooter:          true,
		enableAutoBinaryConfig: true,
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.logger == nil {
		dfltSink, _ := logger.NewSink(nil, logger.OffLevel)
		cfg.logger = &logger.Logger{Sink: dfltSink}
	}
	if cfg.binaryIdMapper == nil {
		cfg.binaryIdMapper = &BinaryBasicIdMapper{}
	}
	ch, err := createReliableChannel(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	cli := Client{cfg: &cfg, ch: ch}
	marsh := &marshallerImpl{
		cli:           &cli,
		reg:           newBinaryMetadataRegistry(&cli),
		compactFooter: cfg.compactFooter,
		idMapper:      cfg.binaryIdMapper,
	}
	cli.marsh = marsh
	if cfg.enableAutoBinaryConfig {
		err = marsh.checkBinaryConfiguration(ctx)
	}
	return &cli, err
}

type channel interface {
	send(ctx context.Context, opCode int16, requestWriter func(output BinaryOutputStream) error, responseReader func(input BinaryInputStream, err error))
	protocolContext() *ProtocolContext
	close(ctx context.Context)
	isClosed() bool
}
