package quic

import (
	"errors"
	"fmt"
)

// Transport error codes (RFC 9000 section 20.1).
const (
	errNoError             = 0x00
	errInternal            = 0x01
	errConnectionRefused   = 0x02
	errFlowControl         = 0x03
	errStreamLimit         = 0x04
	errStreamState         = 0x05
	errFinalSize           = 0x06
	errFrameEncoding       = 0x07
	errTransportParameter  = 0x08
	errConnectionIDLimit   = 0x09
	errProtocolViolation   = 0x0a
	errInvalidToken        = 0x0b
	errApplication         = 0x0c
	errCryptoBufferExceeds = 0x0d
	errKeyUpdate           = 0x0e
	errAEADLimitReached    = 0x0f
	errCryptoBase          = 0x100
)

var (
	// ErrIdleTimeout ends a connection that heard nothing from its peer
	// for the idle timeout.
	ErrIdleTimeout = errors.New("quic: idle timeout")
	// ErrHandshakeTimeout ends a connection whose handshake did not finish
	// in time.
	ErrHandshakeTimeout = errors.New("quic: handshake timeout")
	// ErrStatelessReset ends a connection the peer no longer knows.
	ErrStatelessReset = errors.New("quic: stateless reset")
	// ErrClosed is what operations on a closed connection or stream get.
	ErrClosed = errors.New("quic: connection closed")
	// ErrStreamLimit is what opening a stream gets while the peer allows no
	// more.
	ErrStreamLimit = errors.New("quic: too many open streams")
	// ErrVersionNegotiation ends a client connection to a server that does
	// not speak version 1.
	ErrVersionNegotiation = errors.New("quic: server does not support QUIC version 1")
)

// TransportError is a connection ended by a transport error, sent or
// received.
type TransportError struct {
	Code   uint64
	Reason string
	Remote bool
}

func (e *TransportError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	if e.Code >= errCryptoBase && e.Code < errCryptoBase+256 {
		return fmt.Sprintf("quic: %s TLS alert %d: %s", side, e.Code-errCryptoBase, e.Reason)
	}
	return fmt.Sprintf("quic: %s transport error 0x%x: %s", side, e.Code, e.Reason)
}

// ApplicationError is a connection ended by the application, on either
// side, with a code the application protocol defines.
type ApplicationError struct {
	Code   uint64
	Reason string
	Remote bool
}

func (e *ApplicationError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	return fmt.Sprintf("quic: %s application error 0x%x: %s", side, e.Code, e.Reason)
}

func transportErr(code uint64, reason string) *TransportError {
	return &TransportError{Code: code, Reason: reason}
}
