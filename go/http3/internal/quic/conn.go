// Package quic implements the QUIC transport (RFC 9000, 9001 and 9002) that
// HTTP/3 runs on, over datagrams that something else sends and receives.
//
// A Conn does no I/O of its own: it is fed the datagrams that arrive from
// its peer and sends through a PacketConn, which in fib is a UDP
// connection of the engine, one per peer address. A connection therefore
// stays on the address it started on; migration is not supported, and
// servers say so with disable_active_migration.
//
// The TLS 1.3 handshake is crypto/tls's QUICConn. 0-RTT is not supported.
package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"
)

// Packet number spaces.
const (
	spaceInitial = iota
	spaceHandshake
	spaceApp
	numSpaces
)

// PacketConn sends a connection's datagrams to its peer.
type PacketConn interface {
	// Send sends one datagram. The connection does not use it again.
	Send(datagram []byte) error
	// Close releases the path once the connection is over.
	Close()
}

// Handler is told what happens on a connection. Its methods are called one
// at a time per connection, in order, with no lock held, so they may call
// back into the connection and its streams.
type Handler interface {
	// OnHandshake runs once the handshake is complete.
	OnHandshake(c *Conn)
	// OnStreamData delivers a stream's data in order, with fin set on the
	// last call. The first call for a stream the peer opened is how the
	// stream becomes known. data belongs to the handler.
	OnStreamData(s *Stream, data []byte, fin bool)
	// OnStreamReset runs when the peer abandons its sending side.
	OnStreamReset(s *Stream, code uint64)
	// OnStopSending runs when the peer asks this side to stop sending; the
	// sending side has already been reset with the peer's code.
	OnStopSending(s *Stream, code uint64)
	// OnStreamsAvailable runs when the peer allows more streams.
	OnStreamsAvailable(c *Conn)
	// OnClose runs once when the connection is over, with why.
	OnClose(c *Conn, err error)
}

// Config sets up a connection. Zero values take the defaults.
type Config struct {
	// TLSConfig is required. Its NextProtos are the ALPN offer, which QUIC
	// requires to be agreed.
	TLSConfig *tls.Config
	// MaxIdleTimeout closes a connection that hears nothing from its peer
	// for this long. Default 30 seconds.
	MaxIdleTimeout time.Duration
	// HandshakeTimeout bounds the handshake. Default 10 seconds.
	HandshakeTimeout time.Duration
	// KeepAlivePeriod, when set, pings a quiet connection this often so
	// that it does not idle out.
	KeepAlivePeriod time.Duration
	// MaxIncomingStreams and MaxIncomingUniStreams are how many streams of
	// each kind the peer may have open at once. Defaults 100 and 16.
	MaxIncomingStreams    uint64
	MaxIncomingUniStreams uint64
	// StreamReceiveWindow and ConnReceiveWindow are the flow control
	// windows. Defaults 1 MiB and 16 MiB.
	StreamReceiveWindow uint64
	ConnReceiveWindow   uint64
	// ResetKey, on a server, lets its peers recognize stateless resets.
	ResetKey *ResetKey
}

func (config Config) withDefaults() Config {
	if tc := config.TLSConfig; tc != nil && tc.MinVersion < tls.VersionTLS13 {
		// QUIC is TLS 1.3 only, and older versions of crypto/tls insist
		// that the config say so.
		tc = tc.Clone()
		tc.MinVersion = tls.VersionTLS13
		config.TLSConfig = tc
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = 30 * time.Second
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.MaxIncomingStreams == 0 {
		config.MaxIncomingStreams = 100
	}
	if config.MaxIncomingUniStreams == 0 {
		config.MaxIncomingUniStreams = 16
	}
	if config.StreamReceiveWindow == 0 {
		config.StreamReceiveWindow = 1 << 20
	}
	if config.ConnReceiveWindow == 0 {
		config.ConnReceiveWindow = 16 << 20
	}
	return config
}

type pnSpace struct {
	tx, rx    *keys
	discarded bool

	// Sending.
	nextPN               uint64
	largestAcked         int64
	sent                 []*sentPacket
	ackElicitingInFlight int
	lossTime             time.Time
	lastAckElicitingSent time.Time
	probes               int

	// Receiving: what to acknowledge, and whether and when to.
	recv             rangeSet
	largestRecvTime  time.Time
	ackPending       bool
	ackNow           bool
	unackedEliciting int
	ackDeadline      time.Time

	cryptoSend sendBuffer
	cryptoRecv recvBuffer
}

type peerCID struct {
	cid, token []byte
}

// maxBufferedPackets bounds the packets kept until their keys arrive.
const maxBufferedPackets = 16

// maxCryptoBuffer bounds out-of-order handshake data.
const maxCryptoBuffer = 64 << 10

// Conn is one QUIC connection.
type Conn struct {
	mu       sync.Mutex
	pc       PacketConn
	remote   net.Addr
	handler  Handler
	config   Config
	isClient bool
	tls      *tls.QUICConn

	// Connection IDs: scid is this side's, dcid the peer's in use, and odcid
	// the one the client first sent to.
	scid, dcid, odcid []byte
	// A client's Retry state, and whether it has heard from the server.
	retrySCID    []byte
	token        []byte
	heardPeer    bool
	peerCIDs     map[uint64]peerCID
	dcidSeq      uint64
	retiredPrior uint64
	retireCIDs   []uint64
	// peerResetToken lets a client recognize a stateless reset.
	peerResetToken []byte

	spaces             [numSpaces]pnSpace
	handshakeComplete  bool
	handshakeConfirmed bool
	addressValidated   bool
	bytesRecv          uint64
	bytesSent          uint64
	local              transportParams
	peerParams         *transportParams

	// 1-RTT key updates: keyPhase is the current phase, prevRx the keys of
	// the one before for packets that arrive late, and phaseStart the first
	// packet number of the current phase.
	keyPhase   bool
	prevRx     *keys
	nextRx     *keys
	phaseStart uint64

	rtt          rttStats
	cc           newReno
	ptoCount     int
	lossDeadline time.Time

	timer *time.Timer

	// Flow control: what the peer allows this side to send, and what this
	// side allows the peer.
	peerMaxData    uint64
	sentData       uint64
	recvMaxData    uint64
	recvData       uint64
	recvConsumed   uint64
	maxDataPending bool

	streams               map[uint64]*Stream
	openedBidi, openedUni uint64
	peerMaxBidi           uint64
	peerMaxUni            uint64
	// Streams the peer opens: how many, how many are finished, and the
	// limits this side has given.
	peerOpenedBidi, peerOpenedUni uint64
	peerDoneBidi, peerDoneUni     uint64
	maxBidi, maxUni               uint64
	maxStreamsBidiPending         bool
	maxStreamsUniPending          bool
	sendQueue                     []*Stream

	handshakeDonePending bool
	pingPending          bool
	pathResponses        [][8]byte

	created     time.Time
	idleTimeout time.Duration
	// lastActivity restarts the idle timer: it is when a packet last
	// arrived, or when the first ack-eliciting packet since then left
	// (RFC 9000 section 10.1). sentSinceRecv says whether that has happened.
	lastActivity  time.Time
	sentSinceRecv bool

	closed   bool
	closeErr error
	// closeWhenDone is the close CloseWhenDone asked for.
	closeWhenDone *ApplicationError

	events      []func()
	dispatching bool
	// buffered are packets that arrived before their keys.
	buffered [][]byte

	// Context is for the application, which the connection never touches.
	Context any
}

func (c *Conn) now() time.Time { return time.Now() }

func randomCID() []byte {
	b := make([]byte, cidLen)
	_, _ = rand.Read(b)
	return b
}

func newConn(pc PacketConn, remote net.Addr, config Config, handler Handler, isClient bool) *Conn {
	config = config.withDefaults()
	now := time.Now()
	c := &Conn{
		pc:           pc,
		remote:       remote,
		handler:      handler,
		config:       config,
		isClient:     isClient,
		scid:         randomCID(),
		peerCIDs:     make(map[uint64]peerCID),
		rtt:          newRTTStats(),
		cc:           newNewReno(),
		streams:      make(map[uint64]*Stream),
		created:      now,
		lastActivity: now,
		idleTimeout:  config.MaxIdleTimeout,
		recvMaxData:  config.ConnReceiveWindow,
		maxBidi:      config.MaxIncomingStreams,
		maxUni:       config.MaxIncomingUniStreams,
	}
	for i := range c.spaces {
		c.spaces[i].largestAcked = -1
	}
	c.local = defaultParams()
	c.local.maxIdleTimeout = config.MaxIdleTimeout
	c.local.initialMaxData = config.ConnReceiveWindow
	c.local.maxStreamDataBidiL = config.StreamReceiveWindow
	c.local.maxStreamDataBidiR = config.StreamReceiveWindow
	c.local.maxStreamDataUni = config.StreamReceiveWindow
	c.local.maxStreamsBidi = config.MaxIncomingStreams
	c.local.maxStreamsUni = config.MaxIncomingUniStreams
	c.local.initialSCID, c.local.hasInitialSCID = c.scid, true
	return c
}

// Dial starts the client side of a connection, sending its first packets
// through pc. Datagrams from the server go to HandleDatagram.
func Dial(pc PacketConn, remote net.Addr, config Config, handler Handler) (*Conn, error) {
	if config.TLSConfig == nil {
		return nil, errors.New("quic: Config.TLSConfig is required")
	}
	c := newConn(pc, remote, config, handler, true)
	c.odcid = randomCID()
	c.dcid = c.odcid
	c.spaces[spaceInitial].tx, c.spaces[spaceInitial].rx = initialKeys(c.odcid, true)
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: c.config.TLSConfig})
	c.tls.SetTransportParameters(c.local.encode())
	c.mu.Lock()
	err := c.tls.Start(context.Background())
	if err == nil {
		err = c.processTLSEvents()
	}
	if err != nil {
		c.mu.Unlock()
		c.tls.Close()
		return nil, err
	}
	now := c.now()
	c.flushLocked(now)
	c.mu.Unlock()
	return c, nil
}

// Accept starts the server side of a connection from the client's first
// datagram, which IsInitial has approved. It returns nil if the datagram
// does not start a connection after all.
func Accept(pc PacketConn, remote net.Addr, config Config, handler Handler, datagram []byte) *Conn {
	h, err := parseHeader(datagram, cidLen)
	if err != nil || h.typ != packetInitial || len(h.dcid) < 8 || config.TLSConfig == nil {
		return nil
	}
	c := newConn(pc, remote, config, handler, false)
	c.odcid = bytes.Clone(h.dcid)
	c.dcid = bytes.Clone(h.scid)
	c.peerCIDs[0] = peerCID{cid: c.dcid}
	c.local.originalDCID, c.local.hasOriginalDCID = c.odcid, true
	c.local.disableMigration = true
	if config.ResetKey != nil {
		c.local.statelessResetToken = config.ResetKey.token(c.scid)
	}
	c.spaces[spaceInitial].tx, c.spaces[spaceInitial].rx = initialKeys(c.odcid, false)
	c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: c.config.TLSConfig})
	c.mu.Lock()
	if err := c.tls.Start(context.Background()); err != nil {
		c.mu.Unlock()
		c.tls.Close()
		return nil
	}
	c.mu.Unlock()
	c.HandleDatagram(datagram)
	return c
}

// RemoteAddr is the peer's address.
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

// ConnectionState is the TLS state of the connection.
func (c *Conn) ConnectionState() tls.ConnectionState {
	return c.tls.ConnectionState()
}

// HandleDatagram processes a datagram from the peer.
func (c *Conn) HandleDatagram(datagram []byte) {
	c.mu.Lock()
	if !c.closed {
		now := c.now()
		c.handleDatagramLocked(datagram, now)
		c.flushLocked(now)
	}
	c.mu.Unlock()
	c.dispatch()
}

// Abort ends the connection without telling the peer, as when the path
// under it is gone.
func (c *Conn) Abort(err error) {
	c.mu.Lock()
	c.terminateLocked(err)
	c.mu.Unlock()
	c.dispatch()
}

// Close ends the connection with an application error code, telling the
// peer.
func (c *Conn) Close(code uint64, reason string) {
	c.mu.Lock()
	c.closeLocked(&ApplicationError{Code: code, Reason: reason})
	c.mu.Unlock()
	c.dispatch()
}

// CloseWhenDone closes the connection with an application error code once
// every bidirectional stream is finished, its data delivered: a graceful
// end, for after the application protocol has told the peer to open no
// more.
func (c *Conn) CloseWhenDone(code uint64, reason string) {
	c.mu.Lock()
	if !c.closed && c.closeWhenDone == nil {
		c.closeWhenDone = &ApplicationError{Code: code, Reason: reason}
		c.closeIfDoneLocked()
	}
	c.mu.Unlock()
	c.dispatch()
}

func (c *Conn) closeIfDoneLocked() {
	if c.closeWhenDone == nil || c.closed {
		return
	}
	for id := range c.streams {
		if isBidi(id) {
			return
		}
	}
	c.closeLocked(c.closeWhenDone)
}

// OpenStream opens a bidirectional stream, or fails with ErrStreamLimit
// while the peer allows no more; OnStreamsAvailable says when to try again.
func (c *Conn) OpenStream() (*Stream, error) { return c.openStream(true) }

// OpenUniStream opens a unidirectional stream.
func (c *Conn) OpenUniStream() (*Stream, error) { return c.openStream(false) }

func (c *Conn) openStream(bidi bool) (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if !c.handshakeComplete {
		return nil, errors.New("quic: handshake not complete")
	}
	var id uint64
	if bidi {
		if c.openedBidi >= c.peerMaxBidi {
			return nil, ErrStreamLimit
		}
		id = c.openedBidi << 2
		c.openedBidi++
	} else {
		if c.openedUni >= c.peerMaxUni {
			return nil, ErrStreamLimit
		}
		id = c.openedUni<<2 | 2
		c.openedUni++
	}
	if !c.isClient {
		id |= 1
	}
	return c.newStream(id), nil
}

// newStream sets up a stream's state from the limits of both sides.
func (c *Conn) newStream(id uint64) *Stream {
	s := &Stream{conn: c, id: id, recvFin: -1}
	local := isClientStream(id) == c.isClient
	s.hasSend = isBidi(id) || local
	s.hasRecv = isBidi(id) || !local
	p := c.peerParams
	switch {
	case p == nil:
	case !isBidi(id):
		s.sendMax = p.maxStreamDataUni
	case local:
		s.sendMax = p.maxStreamDataBidiR
	default:
		s.sendMax = p.maxStreamDataBidiL
	}
	if s.hasRecv {
		s.recvMax = c.config.StreamReceiveWindow
	}
	c.streams[id] = s
	return s
}

// peerStream finds the stream a frame refers to, opening the peer's streams
// up to it. recvSide says whether the frame is about the stream's receiving
// side. A nil stream with no error is one already finished.
func (c *Conn) peerStream(id uint64, recvSide bool) (*Stream, error) {
	local := isClientStream(id) == c.isClient
	if !isBidi(id) && local == recvSide {
		return nil, transportErr(errStreamState, "frame for the wrong direction of a unidirectional stream")
	}
	if s := c.streams[id]; s != nil {
		return s, nil
	}
	seq := id >> 2
	if local {
		opened := c.openedUni
		if isBidi(id) {
			opened = c.openedBidi
		}
		if seq >= opened {
			return nil, transportErr(errStreamState, "frame for a stream not yet opened")
		}
		return nil, nil
	}
	opened, limit := &c.peerOpenedUni, c.maxUni
	if isBidi(id) {
		opened, limit = &c.peerOpenedBidi, c.maxBidi
	}
	if seq < *opened {
		return nil, nil
	}
	if seq >= limit {
		return nil, transportErr(errStreamLimit, "too many streams")
	}
	for ; *opened <= seq; *opened++ {
		c.newStream(*opened<<2 | id&3)
	}
	return c.streams[id], nil
}

// maybeFinish forgets a stream once both of its sides are done, and gives
// the peer another stream in place of one of its own.
func (c *Conn) maybeFinish(s *Stream) {
	if s.done || !s.sendDone() || !s.recvFinished() {
		return
	}
	s.done = true
	delete(c.streams, s.id)
	c.closeIfDoneLocked()
	if s.local() {
		return
	}
	if isBidi(s.id) {
		c.peerDoneBidi++
		if c.maxBidi-c.peerDoneBidi < c.config.MaxIncomingStreams/2+1 {
			c.maxBidi = c.peerDoneBidi + c.config.MaxIncomingStreams
			c.maxStreamsBidiPending = true
		}
	} else {
		c.peerDoneUni++
		if c.maxUni-c.peerDoneUni < c.config.MaxIncomingUniStreams/2+1 {
			c.maxUni = c.peerDoneUni + c.config.MaxIncomingUniStreams
			c.maxStreamsUniPending = true
		}
	}
}

func (c *Conn) queueStream(s *Stream) {
	if !s.queued && !s.done {
		s.queued = true
		c.sendQueue = append(c.sendQueue, s)
	}
}

// dispatch runs queued handler calls, one goroutine at a time, so that the
// handler sees a connection's events in order and never concurrently.
func (c *Conn) dispatch() {
	c.mu.Lock()
	if c.dispatching {
		c.mu.Unlock()
		return
	}
	c.dispatching = true
	for len(c.events) > 0 {
		events := c.events
		c.events = nil
		c.mu.Unlock()
		for _, f := range events {
			f()
		}
		c.mu.Lock()
	}
	c.dispatching = false
	c.mu.Unlock()
}

func (c *Conn) handleDatagramLocked(d []byte, now time.Time) {
	c.bytesRecv += uint64(len(d))
	whole := d
	processed := false
	for len(d) > 0 && !c.closed {
		h, err := parseHeader(d, len(c.scid))
		if err == errUnknownVersion {
			if c.isClient && h.version == 0 {
				c.handleVersionNegotiation(d)
			}
			return
		}
		if err != nil {
			break
		}
		pkt := d[:h.end]
		d = d[h.end:]
		if h.typ == packetRetry {
			c.handleRetry(pkt, h)
			return
		}
		if h.typ != packet1RTT && !bytes.Equal(h.dcid, c.scid) && (c.isClient || !bytes.Equal(h.dcid, c.odcid)) {
			continue
		}
		if c.handlePacket(pkt, h, now) {
			processed = true
		}
	}
	if !processed && c.isClient && c.isStatelessReset(whole) {
		c.terminateLocked(ErrStatelessReset)
		return
	}
	c.retryBuffered(now)
}

// retryBuffered processes packets that arrived before their keys, now that
// there may be keys for them.
func (c *Conn) retryBuffered(now time.Time) {
	for len(c.buffered) > 0 && !c.closed {
		pending := c.buffered
		c.buffered = nil
		progress := false
		for _, pkt := range pending {
			h, err := parseHeader(pkt, len(c.scid))
			if err != nil {
				continue
			}
			if c.keysFor(h.typ) == nil {
				c.buffered = append(c.buffered, pkt)
				continue
			}
			c.handlePacket(pkt, h, now)
			progress = true
		}
		if !progress {
			return
		}
	}
}

func packetSpace(typ int) int {
	switch typ {
	case packetInitial:
		return spaceInitial
	case packetHandshake:
		return spaceHandshake
	}
	return spaceApp
}

func (c *Conn) keysFor(typ int) *keys {
	s := &c.spaces[packetSpace(typ)]
	if s.discarded {
		return nil
	}
	return s.rx
}

func (c *Conn) isStatelessReset(d []byte) bool {
	if len(d) < 21 || d[0]&0x80 != 0 || c.peerResetToken == nil {
		return false
	}
	return bytes.Equal(d[len(d)-statelessResetTokenLen:], c.peerResetToken)
}

// handlePacket decrypts and processes one packet, reporting whether it was
// authentic.
func (c *Conn) handlePacket(pkt []byte, h header, now time.Time) bool {
	if h.typ == packet0RTT {
		return false
	}
	space := packetSpace(h.typ)
	s := &c.spaces[space]
	if s.discarded {
		return false
	}
	if s.rx == nil {
		if space != spaceInitial && len(c.buffered) < maxBufferedPackets {
			c.buffered = append(c.buffered, pkt)
		}
		return false
	}
	largest := int64(-1)
	if len(s.recv) > 0 {
		largest = int64(s.recv.largest())
	}
	pn, n, ok := unprotect(pkt, h.pnOffset, s.rx.hp, largest)
	if !ok {
		return false
	}
	hdrEnd := h.pnOffset + n
	k := s.rx
	updating := false
	var phase bool
	if space == spaceApp {
		phase = pkt[0]&0x04 != 0
		if phase != c.keyPhase {
			if c.prevRx != nil && pn < c.phaseStart {
				k = c.prevRx
			} else {
				if c.nextRx == nil {
					c.nextRx = s.rx.next()
				}
				k, updating = c.nextRx, true
			}
		}
	}
	var payload []byte
	var err error
	if updating {
		// A failed trial must not spoil the packet for anything else.
		trial := bytes.Clone(pkt)
		payload, err = k.open(trial, hdrEnd, pn)
	} else {
		payload, err = k.open(pkt, hdrEnd, pn)
	}
	if err != nil {
		return false
	}
	if updating {
		if !c.handshakeConfirmed {
			c.closeLocked(transportErr(errKeyUpdate, "key update before the handshake was confirmed"))
			return true
		}
		c.prevRx = s.rx
		s.rx = c.nextRx
		c.nextRx = nil
		s.tx = s.tx.next()
		c.keyPhase = phase
		c.phaseStart = pn
	}
	reserved := byte(0x18)
	if pkt[0]&0x80 != 0 {
		reserved = 0x0c
	}
	if pkt[0]&reserved != 0 {
		c.closeLocked(transportErr(errProtocolViolation, "reserved header bits set"))
		return true
	}
	if !s.recv.add(pn) {
		return true
	}
	if c.isClient && !c.heardPeer && h.typ != packet1RTT {
		// The server picks its own connection ID, which the client uses from
		// its first response on (RFC 9000 section 7.2).
		c.heardPeer = true
		c.dcid = bytes.Clone(h.scid)
		c.peerCIDs[0] = peerCID{cid: c.dcid}
	}
	if !c.isClient && space == spaceHandshake && !c.addressValidated {
		// Only the client could have produced a Handshake packet, so its
		// address is its own (RFC 9000 section 8.1).
		c.addressValidated = true
		c.discardSpace(spaceInitial)
	}
	c.lastActivity = now
	c.sentSinceRecv = false
	ackEliciting, err := c.handleFrames(space, payload, now)
	if err != nil {
		c.closeLocked(err)
		return true
	}
	if c.closed || s.discarded {
		return true
	}
	if int64(pn) >= largest {
		s.largestRecvTime = now
	}
	if ackEliciting {
		s.ackPending = true
		s.unackedEliciting++
		switch {
		case space != spaceApp || s.unackedEliciting >= 2 || int64(pn) != largest+1:
			s.ackNow = true
		case s.ackDeadline.IsZero():
			s.ackDeadline = now.Add(c.local.maxAckDelay)
		}
	}
	return true
}

// handleCrypto takes in handshake data and hands what is in order to TLS.
func (c *Conn) handleCrypto(space int, off uint64, data []byte) error {
	s := &c.spaces[space]
	if off+uint64(len(data)) > s.cryptoRecv.offset+maxCryptoBuffer {
		return transportErr(errCryptoBufferExceeds, "too much out-of-order handshake data")
	}
	s.cryptoRecv.push(off, data)
	level := [...]tls.QUICEncryptionLevel{
		tls.QUICEncryptionLevelInitial,
		tls.QUICEncryptionLevelHandshake,
		tls.QUICEncryptionLevelApplication,
	}[space]
	for {
		chunk := s.cryptoRecv.pop()
		if chunk == nil {
			return nil
		}
		if err := c.tls.HandleData(level, chunk); err != nil {
			return tlsError(err)
		}
		if err := c.processTLSEvents(); err != nil {
			return err
		}
		if s.discarded {
			return nil
		}
	}
}

// tlsError turns a TLS failure into the transport error that carries its
// alert (RFC 9001 section 4.8).
func tlsError(err error) error {
	var alert tls.AlertError
	if errors.As(err, &alert) {
		return &TransportError{Code: errCryptoBase + uint64(alert), Reason: err.Error()}
	}
	return &TransportError{Code: errInternal, Reason: err.Error()}
}

func (c *Conn) processTLSEvents() error {
	for {
		e := c.tls.NextEvent()
		space := spaceApp
		switch e.Level {
		case tls.QUICEncryptionLevelInitial:
			space = spaceInitial
		case tls.QUICEncryptionLevelHandshake:
			space = spaceHandshake
		case tls.QUICEncryptionLevelEarly:
			space = -1
		}
		switch e.Kind {
		case tls.QUICNoEvent:
			return nil
		case tls.QUICSetReadSecret, tls.QUICSetWriteSecret:
			if space < 0 {
				continue
			}
			k, err := newKeys(e.Suite, bytes.Clone(e.Data))
			if err != nil {
				return transportErr(errInternal, err.Error())
			}
			if e.Kind == tls.QUICSetReadSecret {
				c.spaces[space].rx = k
			} else {
				c.spaces[space].tx = k
			}
		case tls.QUICWriteData:
			if space >= 0 && !c.spaces[space].discarded {
				c.spaces[space].cryptoSend.write(e.Data)
			}
		case tls.QUICTransportParameters:
			if err := c.handlePeerParams(e.Data); err != nil {
				return err
			}
		case tls.QUICTransportParametersRequired:
			c.tls.SetTransportParameters(c.local.encode())
		case tls.QUICHandshakeDone:
			if err := c.onHandshakeComplete(); err != nil {
				return err
			}
		}
	}
}

func (c *Conn) handlePeerParams(data []byte) error {
	p, err := parseParams(data, c.isClient)
	if err != nil {
		return err
	}
	if !p.hasInitialSCID || !bytes.Equal(p.initialSCID, c.dcid) {
		return transportErr(errTransportParameter, "initial_source_connection_id does not match")
	}
	if c.isClient {
		if !p.hasOriginalDCID || !bytes.Equal(p.originalDCID, c.odcid) {
			return transportErr(errTransportParameter, "original_destination_connection_id does not match")
		}
		if p.hasRetrySCID != (c.retrySCID != nil) || c.retrySCID != nil && !bytes.Equal(p.retrySCID, c.retrySCID) {
			return transportErr(errTransportParameter, "retry_source_connection_id does not match")
		}
		c.peerResetToken = p.statelessResetToken
		c.peerCIDs[0] = peerCID{cid: c.dcid, token: p.statelessResetToken}
	}
	c.peerParams = &p
	c.peerMaxData = p.initialMaxData
	c.peerMaxBidi = p.maxStreamsBidi
	c.peerMaxUni = p.maxStreamsUni
	if p.maxIdleTimeout > 0 && p.maxIdleTimeout < c.idleTimeout {
		c.idleTimeout = p.maxIdleTimeout
	}
	return nil
}

func (c *Conn) onHandshakeComplete() error {
	if c.peerParams == nil {
		return transportErr(errTransportParameter, "no transport parameters")
	}
	c.handshakeComplete = true
	if !c.isClient {
		c.handshakeDonePending = true
		c.confirmHandshake()
		if tc := c.config.TLSConfig; !tc.SessionTicketsDisabled {
			if err := c.tls.SendSessionTicket(tls.QUICSessionTicketOptions{}); err != nil {
				return tlsError(err)
			}
		}
	}
	c.events = append(c.events, func() { c.handler.OnHandshake(c) })
	return nil
}

// confirmHandshake is when the handshake is known to be over on both sides
// and its keys can go (RFC 9001 section 4.1.2).
func (c *Conn) confirmHandshake() {
	if c.handshakeConfirmed || !c.handshakeComplete {
		return
	}
	c.handshakeConfirmed = true
	c.discardSpace(spaceHandshake)
}

func (c *Conn) handleVersionNegotiation(d []byte) {
	if c.heardPeer {
		return
	}
	h, _ := parseHeader(d, 0)
	if !bytes.Equal(h.dcid, c.scid) || !bytes.Equal(h.scid, c.odcid) {
		return
	}
	p := 7 + len(h.dcid) + len(h.scid)
	for ; p+4 <= len(d); p += 4 {
		if d[p] == 0 && d[p+1] == 0 && d[p+2] == 0 && d[p+3] == 1 {
			// A version negotiation packet listing the version in use is
			// not genuine (RFC 9000 section 6.2).
			return
		}
	}
	c.terminateLocked(ErrVersionNegotiation)
}

// handleRetry restarts a client's handshake towards the connection ID a
// Retry packet names, with the token it carries (RFC 9000 section 17.2.5).
func (c *Conn) handleRetry(pkt []byte, h header) {
	if !c.isClient || c.heardPeer || c.retrySCID != nil || len(h.token) <= 16 {
		return
	}
	tagAt := len(pkt) - 16
	if !bytes.Equal(retryTag(c.odcid, pkt[:tagAt]), pkt[tagAt:]) || bytes.Equal(h.scid, c.dcid) {
		return
	}
	c.retrySCID = bytes.Clone(h.scid)
	c.dcid = c.retrySCID
	c.token = bytes.Clone(h.token[:len(h.token)-16])
	s := &c.spaces[spaceInitial]
	s.tx, s.rx = initialKeys(c.dcid, true)
	// Everything sent so far is sent again under the new keys.
	for _, p := range s.sent {
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
		}
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
	}
	s.sent = nil
	s.cryptoSend.onLost(0, s.cryptoSend.next)
	c.ptoCount = 0
}

// closeLocked ends the connection with err, telling the peer.
func (c *Conn) closeLocked(err error) {
	if c.closed {
		return
	}
	c.sendClose(err)
	c.terminateLocked(err)
}

// sendClose sends CONNECTION_CLOSE in every space the peer may be able to
// read, since this side cannot tell which it has keys for.
func (c *Conn) sendClose(err error) {
	var plans []packetPlan
	size := 0
	padInitial := false
	for space := range c.spaces {
		s := &c.spaces[space]
		if s.tx == nil || s.discarded {
			continue
		}
		if space == spaceApp && !c.handshakeComplete {
			continue
		}
		payload := appendConnectionClose(nil, err, space == spaceApp)
		n := pnLen(s.nextPN, s.largestAcked)
		plan := packetPlan{space: space, pn: s.nextPN, pnLen: n, payload: payload, sp: &sentPacket{pn: s.nextPN}}
		s.nextPN++
		if space == spaceInitial && c.isClient {
			padInitial = true
		}
		plans = append(plans, plan)
		size += c.packetSize(&plan)
	}
	if len(plans) == 0 {
		return
	}
	if padInitial && size < minInitialDatagram {
		last := &plans[len(plans)-1]
		last.payload = append(last.payload, make([]byte, minInitialDatagram-size)...)
	}
	out := make([]byte, 0, max(size, minInitialDatagram))
	for i := range plans {
		out = c.sealPlan(out, &plans[i])
	}
	_ = c.pc.Send(out)
}

// terminateLocked ends the connection at once, without telling the peer.
func (c *Conn) terminateLocked(err error) {
	if c.closed {
		return
	}
	c.closed = true
	c.closeErr = err
	if c.timer != nil {
		c.timer.Stop()
	}
	for _, s := range c.streams {
		s.done = true
	}
	c.streams = nil
	c.sendQueue = nil
	c.buffered = nil
	c.events = append(c.events, func() {
		c.tls.Close()
		c.pc.Close()
		c.handler.OnClose(c, err)
	})
}

// armTimerLocked sets the connection's one timer for the earliest thing
// that has to happen.
func (c *Conn) armTimerLocked(now time.Time) {
	if c.closed {
		return
	}
	deadline := c.idleDeadline()
	earliest := func(t time.Time) {
		if !t.IsZero() && t.Before(deadline) {
			deadline = t
		}
	}
	if !c.handshakeComplete {
		earliest(c.created.Add(c.config.HandshakeTimeout))
	}
	earliest(c.lossDeadline)
	if s := &c.spaces[spaceApp]; s.ackPending && !s.ackNow {
		earliest(s.ackDeadline)
	}
	if c.config.KeepAlivePeriod > 0 && c.handshakeComplete {
		earliest(c.keepAliveAt())
	}
	d := max(deadline.Sub(now), 0)
	if c.timer == nil {
		c.timer = time.AfterFunc(d, c.onTimer)
	} else {
		c.timer.Reset(d)
	}
}

// idleDeadline is when the connection idles out: never sooner than three
// probe timeouts, so that loss recovery gets its chance (RFC 9000 section
// 10.1).
func (c *Conn) idleDeadline() time.Time {
	return c.lastActivity.Add(max(c.idleTimeout, 3*c.rtt.pto()))
}

// keepAliveAt is when a quiet connection next needs a PING.
func (c *Conn) keepAliveAt() time.Time {
	last := c.lastActivity
	if t := c.spaces[spaceApp].lastAckElicitingSent; t.After(last) {
		last = t
	}
	return last.Add(c.config.KeepAlivePeriod)
}

func (c *Conn) onTimer() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	now := c.now()
	switch {
	case !c.handshakeComplete && !now.Before(c.created.Add(c.config.HandshakeTimeout)):
		c.terminateLocked(ErrHandshakeTimeout)
	case !now.Before(c.idleDeadline()):
		c.terminateLocked(ErrIdleTimeout)
	default:
		if !c.lossDeadline.IsZero() && !now.Before(c.lossDeadline) {
			c.onLossDetectionTimeout(now)
		}
		if c.config.KeepAlivePeriod > 0 && c.handshakeComplete && !now.Before(c.keepAliveAt()) {
			c.pingPending = true
		}
		c.flushLocked(now)
	}
	c.mu.Unlock()
	c.dispatch()
}
