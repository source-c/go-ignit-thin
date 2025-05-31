package ignite

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/source-c/go-ignit-thin/internal/bitset"
	"github.com/source-c/go-ignit-thin/logger"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	errorFlag                   = 1
	affinityTopologyChangedFlag = 1 << 1
	notificationFlag            = 1 << 2
)

func createClientConnectionError(msg string, err error) *ClientConnectionError {
	return &ClientConnectionError{ClientError{msg}, err}
}

type tcpChannel struct {
	addr              string
	socket            net.Conn
	idGen             atomic.Int64
	protocolCtx       atomic.Value
	topVer            int64
	minTopVer         int32
	pendingCh         chan int64
	pendingRequests   sync.Map
	doneCh            chan struct{}
	serverId          *uuid.UUID
	closed            atomic.Bool
	supportedVersions map[ProtocolVersion]bool
	closeErr          atomic.Value
	closeWg           sync.WaitGroup
	clientCfg         *clientConfiguration
	log               *logger.Logger
}

type pendingRequest struct {
	id           int64
	requestData  []byte
	responseData []byte
	err          error
	doneCh       chan struct{}
}

func responseId(packet []byte) (int64, bool) {
	if len(packet) >= longBytes {
		res := binary.LittleEndian.Uint64(packet)
		return int64(res), true
	}
	return 0, false
}

func newRequest(id int64, opCode int16, requestWriter func(input BinaryOutputStream) error) (*pendingRequest, error) {
	reqInput := NewBinaryOutputStream(64)
	reqInput.WriteInt32(0)
	// Handshake request
	if id != -1 {
		reqInput.WriteInt16(opCode)
		reqInput.WriteInt64(id)
	}
	err := requestWriter(reqInput)
	if err != nil {
		return nil, err
	}
	currPosition := reqInput.Position()
	reqInput.SetPosition(0)
	reqInput.WriteInt32(int32(currPosition - intBytes))
	reqInput.SetPosition(currPosition)
	return &pendingRequest{
		id:          id,
		requestData: reqInput.Data(),
		doneCh:      make(chan struct{}),
	}, nil
}

func (ch *tcpChannel) protocolContext() *ProtocolContext {
	res := ch.protocolCtx.Load()
	if res == nil {
		return nil
	}
	return res.(*ProtocolContext)
}

func (ch *tcpChannel) send(ctx context.Context, opCode int16, requestWriter func(output BinaryOutputStream) error, responseReader func(input BinaryInputStream, err error)) {
	reqId := ch.requestId()
	ch.log.Trace(func() string {
		return fmt.Sprintf("start performing request[id=%d, op=%d] on %s", reqId, opCode, ch)
	})
	responseReaderFacade := responseReader
	if ch.log.Level() <= logger.TraceLevel {
		responseReaderFacade = func(input BinaryInputStream, err error) {
			ch.log.Trace(func() string {
				if err != nil {
					return fmt.Sprintf("request[id=%d, op=%d] failed on %s: %s", reqId, opCode, ch, err.Error())
				}
				return fmt.Sprintf("request[id=%d, op=%d] succeeded on %s", reqId, opCode, ch)
			})
			responseReader(input, err)
		}
	}
	ch.send0(ctx, reqId, opCode, requestWriter, responseReaderFacade)
}

func (ch *tcpChannel) send0(ctx context.Context, id int64, opCode int16, requestWriter func(output BinaryOutputStream) error, responseReader func(input BinaryInputStream, err error)) {
	if ch.closed.Load() {
		responseReader(nil, createClientConnectionError("channel is closed", nil))
		return
	}
	ctx, cancel := setContextDeadline(ctx, ch.clientCfg.requestTimeout)
	defer cancel()

	req, err := newRequest(id, opCode, requestWriter)
	if err != nil {
		responseReader(nil, err)
		return
	}

	reqId := req.id
	ch.pendingRequests.Store(reqId, req)
	ch.pendingCh <- reqId
	defer ch.pendingRequests.Delete(reqId)

	select {
	case <-ctx.Done():
		responseReader(nil, ch.processCloseError("request cancelled"))
		return
	case <-ch.doneCh:
		responseReader(nil, ch.processCloseError("connection closed"))
		return
	case <-req.doneCh:
		if req.err != nil {
			responseReader(nil, req.err)
			return
		}
		input := NewBinaryInputStream(req.responseData, 0)
		// process handshake
		if reqId == -1 {
			responseReader(input, nil)
			return
		}
		resId := input.ReadInt64()
		if resId != reqId {
			panic(fmt.Sprintf("reqId != resId %d, %d", reqId, resId))
		}
		flags := input.ReadInt16()
		if checkFlag(flags, affinityTopologyChangedFlag) {
			ch.topVer = input.ReadInt64()
			ch.minTopVer = input.ReadInt32()
		}
		if checkFlag(flags, errorFlag) {
			statusCode := int(input.ReadInt32())
			var errMsg string
			errMsg, err = unmarshalString(input)
			if err != nil {
				req.err = createClientConnectionError("broken output from server", err)
			} else {
				req.err = &ClientServerError{
					ClientError{
						Message: errMsg,
					},
					ErrorCode(statusCode),
				}
			}
		}
		responseReader(input, req.err)
		return
	}
}

func (ch *tcpChannel) processCloseError(defaultMsg string) error {
	closeErr := ch.closeErr.Load()
	if closeErr == nil {
		return &ClientError{defaultMsg}
	}
	return &ClientConnectionError{ClientError{"connection error"}, closeErr.(error)}
}

func (ch *tcpChannel) beginClose(err error) {
	if !ch.closed.CompareAndSwap(false, true) {
		return
	}
	if err != nil {
		ch.log.Errorf("%s closed with error: %w", ch, err)
		ch.closeErr.Store(err)
	}
	ch.pendingRequests.Range(func(id, val any) bool {
		req, ok := val.(*pendingRequest)
		if !ok {
			return true
		}
		safeClose(req, createClientConnectionError("connection closed", err))
		ch.pendingRequests.Delete(id)
		return true
	})
	close(ch.doneCh)
	_ = ch.socket.Close()
}

func safeClose(req *pendingRequest, err error) (closed bool) {
	defer func() {
		if recover() != nil {
			closed = false
		}
	}()
	if req == nil {
		return false
	}
	req.err = err
	close(req.doneCh)
	return true
}

func (ch *tcpChannel) close(ctx context.Context) {
	ch.beginClose(nil)

	ctx, cancel := setContextDeadline(ctx, ch.clientCfg.requestTimeout)
	defer cancel()

	_, deadlineSet := ctx.Deadline()
	if deadlineSet {
		waitCh := make(chan struct{})
		go func() {
			ch.closeWg.Wait()
			close(waitCh)
		}()
		select {
		case <-ctx.Done():
			ch.log.Warnf("waiting for channel close timed out")
			return
		case <-waitCh:
			return
		}
	} else {
		ch.closeWg.Wait()
	}
}

func (ch *tcpChannel) isClosed() bool {
	return ch.closed.Load()
}

func (ch *tcpChannel) String() string {
	var sb strings.Builder
	sb.WriteString("tcpChannel[addr=")
	sb.WriteString(ch.addr)
	ctx := ch.protocolCtx.Load()
	if ctx != nil {
		version := ctx.(*ProtocolContext).Version()
		sb.WriteString(", protoVer=")
		sb.WriteString(version.String())
	}
	if ch.serverId != nil {
		sb.WriteString(", serverId=")
		sb.WriteString(ch.serverId.String())
	}
	sb.WriteRune(']')
	return sb.String()
}

func (ch *tcpChannel) writeLoop() {
	defer ch.closeWg.Done()
	writer := bufio.NewWriterSize(ch.socket, writeBufferSize)
LOOP:
	for {
		select {
		case id, ok := <-ch.pendingCh:
			if !ok {
				return
			}
			req, ok := ch.pendingRequests.Load(id)
			if !ok {
				continue LOOP
			}
			_, err := writer.Write(req.(*pendingRequest).requestData)
			if err == nil && len(ch.pendingCh) == 0 {
				err = writer.Flush()
			}
			if err != nil {
				ch.beginClose(err)
				return
			}
		case <-ch.doneCh:
			return
		}
	}
}

const (
	messageBufferSize = 128 * 1024
	writeBufferSize   = 16 * 1024
)

func (ch *tcpChannel) readLoop() {
	var err error
	var n int
	defer func() {
		if r := recover(); r != nil {
			ch.beginClose(fmt.Errorf("failed to process data: %s", err))
		}
		ch.closeWg.Done()
	}()

	buf := make([]byte, messageBufferSize)
	packetAcc := newPacketAccumulator()
	handshakeDone := false
LOOP:
	for {
		if ch.closed.Load() {
			break
		}
		if err = ch.socket.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			break
		}
		n, err = ch.socket.Read(buf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			break
		}
		if n <= 0 {
			continue
		}
		packetAcc.append(buf[:n])
		for {
			data := packetAcc.data()
			if data == nil {
				continue LOOP
			}
			if !handshakeDone && ch.protocolContext() != nil {
				handshakeDone = true
			}
			var id int64
			if !handshakeDone {
				id = -1
			} else {
				var ok bool
				id, ok = responseId(data)
				if !ok {
					err = createClientConnectionError("failed to parse response id", nil)
					break LOOP
				}
			}
			val, ok := ch.pendingRequests.Load(id)
			if !ok {
				continue
			}
			req, ok := val.(*pendingRequest)
			if !ok {
				err = createClientConnectionError("invalid data in pending requests", nil)
				break LOOP
			}
			req.responseData = data
			safeClose(req, nil)
		}
	}
	ch.beginClose(err)
}

type packetAccumulator struct {
	buf         *bytes.Buffer
	currentSize int32
}

func newPacketAccumulator() *packetAccumulator {
	return &packetAccumulator{
		buf:         bytes.NewBuffer(make([]byte, 0, messageBufferSize)),
		currentSize: 0,
	}
}

func (pa *packetAccumulator) append(buf []byte) {
	pa.buf.Write(buf)
}

func (pa *packetAccumulator) data() []byte {
	if !pa.processData() {
		return nil
	}
	// prepare result
	size := int(pa.currentSize)
	result := make([]byte, size)
	// read pa.currentSize since buffer is on position 4
	copy(result, pa.buf.Next(size))
	// copy remaining data, reinitialize buffer
	remainDataSlice := pa.buf.Next(pa.buf.Len())
	remainData := make([]byte, len(remainDataSlice))
	copy(remainData, remainDataSlice)
	if pa.buf.Cap() > messageBufferSize {
		pa.buf = bytes.NewBuffer(make([]byte, 0, messageBufferSize))
	} else {
		pa.buf.Reset()
	}
	if len(remainData) > 0 {
		pa.buf.Write(remainData)
	}
	pa.currentSize = 0
	return result
}

func (pa *packetAccumulator) processData() bool {
	if pa.currentSize <= 0 {
		if pa.buf.Len() < intBytes {
			return false
		}
		size := int32(binary.LittleEndian.Uint32(pa.buf.Next(intBytes)))
		if size < byteBytes {
			panic("packet size is less than minimal message size")
		}
		pa.currentSize = size
	}
	if pa.currentSize > 0 {
		size := int(pa.currentSize)
		return pa.buf.Len() >= size
	}
	return false
}

func (ch *tcpChannel) handshake(ctx context.Context, cliProtoCtx *ProtocolContext) error {
	for {
		ch.log.Debug(func() string {
			ver := cliProtoCtx.Version()
			return fmt.Sprintf("connecting to %s, performing handshake with version=%s", ch, ver.String())
		})
		srvCtx, err := ch.handshakeRound(ctx, cliProtoCtx, ch.clientCfg)
		if err != nil {
			return err
		}
		if srvCtx == nil {
			break
		}
		// Try to do handshake with server version.
		cliProtoCtx = srvCtx
	}
	return nil
}

func (ch *tcpChannel) handshakeRound(ctx context.Context, cliProtoCtx *ProtocolContext, cliCfg *clientConfiguration) (*ProtocolContext, error) {
	var err error = nil
	var srvProtoCtx *ProtocolContext = nil
	writer := func(bw BinaryOutputStream) error {
		bw.WriteInt8(1)
		cliProtoCtx.marshal(bw)
		if cliProtoCtx.SupportsAttributeFeature(UserAttributesFeature) {
			if len(cliCfg.attrs) == 0 {
				bw.WriteNull()
			} else {
				bw.WriteInt8(MapType)
				bw.WriteInt32(int32(len(cliCfg.attrs)))
				bw.WriteInt8(HashMap)
				for k, v := range cliCfg.attrs {
					marshalString(bw, k)
					marshalString(bw, v)
				}
			}
		}
		if cliProtoCtx.SupportsAuthorization() && len(cliCfg.user) > 0 {
			marshalString(bw, cliCfg.user)
			marshalString(bw, cliCfg.password)
		}
		return nil
	}
	reader := func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		success := input.ReadBool()
		if success {
			if cliProtoCtx.SupportsBitmapFeatures() {
				var bitMaskBytes []byte
				if bitMaskBytes, err = unmarshalByteArray(input); err != nil {
					err = fmt.Errorf("broken output from server: %w", err)
					return
				}
				if bitMaskBytes != nil {
					cliProtoCtx.updateAttributeFeatures(bitset.FromBytes(bitMaskBytes))
				}
			}
			if cliProtoCtx.SupportsPartitionAwareness() {
				var serverId uuid.UUID
				if serverId, err = unmarshalUuid(input); err != nil {
					err = fmt.Errorf("broken output from server: %w", err)
					return
				}
				ch.serverId = &serverId
			}
			ch.protocolCtx.Store(cliProtoCtx)
		} else {
			srvProtoCtx = NewProtocolContext(
				ProtocolVersion{Major: input.ReadInt16(), Minor: input.ReadInt16(), Patch: input.ReadInt16()},
			)
			var errMsg string
			if errMsg, err = unmarshalString(input); err != nil {
				err = fmt.Errorf("broken output from server: %w", err)
				return
			}
			errCode := Failed
			if input.Available() > 0 {
				errCode = ErrorCode(input.ReadUInt32())
			}
			cliVersion := cliProtoCtx.Version()
			if errCode == AuthFailed {
				err = &ClientAuthenticationError{
					ClientError{errMsg},
				}
			} else if cliVersion.Compare(srvProtoCtx.Version()) == 0 {
				err = &ClientProtocolError{
					ClientError{errMsg},
				}
			} else if exists := ch.supportedVersions[cliVersion]; !exists || (!cliProtoCtx.SupportsAuthorization() && len(cliCfg.user) > 0) {
				errMsg = fmt.Sprintf("Protocol version mismatch: client %v / server %v. Server details: %s",
					cliVersion,
					srvProtoCtx.Version(),
					errMsg,
				)
				err = &ClientProtocolError{
					ClientError{errMsg},
				}
			}
		}
	}
	ch.send0(ctx, -1, 0, writer, reader)
	if err != nil {
		return nil, err
	}
	return srvProtoCtx, nil
}

func checkFlag(flags int16, flag int16) bool {
	return flags&flag != 0
}

func (ch *tcpChannel) requestId() int64 {
	return ch.idGen.Add(1)
}

func createSupportedVersions() map[ProtocolVersion]bool {
	supportedVersions := make(map[ProtocolVersion]bool)
	for _, ver := range []ProtocolVersion{{1, 0, 0}, {1, 1, 0}, {1, 2, 0}, {1, 3, 0}, {1, 4, 0}, {1, 4, 0}, {1, 5, 0}, {1, 6, 0}, {1, 7, 0}} {
		supportedVersions[ver] = true
	}
	return supportedVersions
}

func setContextDeadline(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	retCancel := func() {}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, deadlineSet := ctx.Deadline(); !deadlineSet && defaultTimeout > 0 {
		ctx, retCancel = context.WithTimeout(ctx, defaultTimeout)
	}
	return ctx, retCancel
}

func createTcpChannel(ctx context.Context, addr string, cfg *clientConfiguration) (*tcpChannel, error) {
	ctx, cancel := setContextDeadline(ctx, cfg.requestTimeout)
	defer cancel()
	if cfg.protocolContext == nil {
		return nil, &ClientProtocolError{
			ClientError{"protocol context is not set"},
		}
	}
	supportedVersions := createSupportedVersions()
	if ok := supportedVersions[cfg.protocolContext.Version()]; !ok {
		version := cfg.protocolContext.Version()
		return nil, &ClientProtocolError{
			ClientError{fmt.Sprintf("version %s is not supported", version.String())},
		}
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, createClientConnectionError(fmt.Sprintf("failed to connect to %s", addr), err)
	}
	if cfg.tlsConfigSupplier != nil {
		var tlsCfg *tls.Config
		tlsCfg, err = cfg.tlsConfigSupplier()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain tls config: %w", err)
		}
		tlsCon := tls.Client(conn, tlsCfg)
		if err = tlsCon.HandshakeContext(ctx); err != nil {
			return nil, fmt.Errorf("tls handshake failed: %w", err)
		}
		conn = tlsCon
	}
	ch := tcpChannel{
		addr:              addr,
		socket:            conn,
		pendingCh:         make(chan int64, 1024),
		doneCh:            make(chan struct{}),
		supportedVersions: supportedVersions,
		clientCfg:         cfg,
		log:               cfg.logger,
	}
	ch.idGen.Store(1)
	ch.closeWg.Add(1)
	go ch.writeLoop()
	ch.closeWg.Add(1)
	go ch.readLoop()
	err = ch.handshake(ctx, cfg.protocolContext)
	if err != nil {
		return nil, err
	}
	return &ch, nil
}
