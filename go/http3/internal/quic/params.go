package quic

import (
	"bytes"
	"time"
)

// Transport parameter IDs (RFC 9000 section 18.2).
const (
	paramOriginalDCID          = 0x00
	paramMaxIdleTimeout        = 0x01
	paramStatelessResetToken   = 0x02
	paramMaxUDPPayloadSize     = 0x03
	paramInitialMaxData        = 0x04
	paramMaxStreamDataBidiLoc  = 0x05
	paramMaxStreamDataBidiRem  = 0x06
	paramMaxStreamDataUni      = 0x07
	paramInitialMaxStreamsBidi = 0x08
	paramInitialMaxStreamsUni  = 0x09
	paramAckDelayExponent      = 0x0a
	paramMaxAckDelay           = 0x0b
	paramDisableMigration      = 0x0c
	paramPreferredAddress      = 0x0d
	paramActiveCIDLimit        = 0x0e
	paramInitialSCID           = 0x0f
	paramRetrySCID             = 0x10
)

// transportParams are what each side tells the other about itself during the
// handshake.
type transportParams struct {
	originalDCID        []byte
	initialSCID         []byte
	retrySCID           []byte
	hasOriginalDCID     bool
	hasInitialSCID      bool
	hasRetrySCID        bool
	statelessResetToken []byte
	maxIdleTimeout      time.Duration
	maxUDPPayloadSize   uint64
	initialMaxData      uint64
	maxStreamDataBidiL  uint64
	maxStreamDataBidiR  uint64
	maxStreamDataUni    uint64
	maxStreamsBidi      uint64
	maxStreamsUni       uint64
	ackDelayExponent    uint64
	maxAckDelay         time.Duration
	disableMigration    bool
	activeCIDLimit      uint64
}

func defaultParams() transportParams {
	return transportParams{
		maxUDPPayloadSize: 65527,
		ackDelayExponent:  3,
		maxAckDelay:       25 * time.Millisecond,
		activeCIDLimit:    2,
	}
}

func appendParamVarint(b []byte, id, v uint64) []byte {
	b = AppendVarint(b, id)
	b = AppendVarint(b, uint64(VarintLen(v)))
	return AppendVarint(b, v)
}

func appendParamBytes(b []byte, id uint64, v []byte) []byte {
	b = AppendVarint(b, id)
	b = AppendVarint(b, uint64(len(v)))
	return append(b, v...)
}

func (p *transportParams) encode() []byte {
	var b []byte
	if p.hasOriginalDCID {
		b = appendParamBytes(b, paramOriginalDCID, p.originalDCID)
	}
	if p.maxIdleTimeout > 0 {
		b = appendParamVarint(b, paramMaxIdleTimeout, uint64(p.maxIdleTimeout/time.Millisecond))
	}
	if p.statelessResetToken != nil {
		b = appendParamBytes(b, paramStatelessResetToken, p.statelessResetToken)
	}
	b = appendParamVarint(b, paramInitialMaxData, p.initialMaxData)
	b = appendParamVarint(b, paramMaxStreamDataBidiLoc, p.maxStreamDataBidiL)
	b = appendParamVarint(b, paramMaxStreamDataBidiRem, p.maxStreamDataBidiR)
	b = appendParamVarint(b, paramMaxStreamDataUni, p.maxStreamDataUni)
	b = appendParamVarint(b, paramInitialMaxStreamsBidi, p.maxStreamsBidi)
	b = appendParamVarint(b, paramInitialMaxStreamsUni, p.maxStreamsUni)
	if p.disableMigration {
		b = appendParamBytes(b, paramDisableMigration, nil)
	}
	if p.hasInitialSCID {
		b = appendParamBytes(b, paramInitialSCID, p.initialSCID)
	}
	if p.hasRetrySCID {
		b = appendParamBytes(b, paramRetrySCID, p.retrySCID)
	}
	return b
}

// parseParams reads the peer's parameters. isClient is whether this side is
// the client, which decides which parameters the peer may send.
func parseParams(b []byte, isClient bool) (transportParams, error) {
	p := defaultParams()
	seen := make(map[uint64]bool)
	r := reader{b: b}
	for len(r.b) > 0 {
		id := r.varint()
		v := r.bytes(r.varint())
		if r.bad {
			return p, transportErr(errTransportParameter, "truncated transport parameters")
		}
		if seen[id] {
			return p, transportErr(errTransportParameter, "duplicate transport parameter")
		}
		seen[id] = true
		vr := reader{b: v}
		intValue := func() uint64 {
			n := vr.varint()
			if len(vr.b) > 0 {
				vr.bad = true
			}
			return n
		}
		serverOnly := false
		switch id {
		case paramOriginalDCID:
			p.originalDCID, p.hasOriginalDCID, serverOnly = bytes.Clone(v), true, true
		case paramMaxIdleTimeout:
			p.maxIdleTimeout = time.Duration(intValue()) * time.Millisecond
		case paramStatelessResetToken:
			if len(v) != statelessResetTokenLen {
				vr.bad = true
			}
			p.statelessResetToken, serverOnly = bytes.Clone(v), true
		case paramMaxUDPPayloadSize:
			if p.maxUDPPayloadSize = intValue(); p.maxUDPPayloadSize < 1200 {
				vr.bad = true
			}
		case paramInitialMaxData:
			p.initialMaxData = intValue()
		case paramMaxStreamDataBidiLoc:
			p.maxStreamDataBidiL = intValue()
		case paramMaxStreamDataBidiRem:
			p.maxStreamDataBidiR = intValue()
		case paramMaxStreamDataUni:
			p.maxStreamDataUni = intValue()
		case paramInitialMaxStreamsBidi:
			if p.maxStreamsBidi = intValue(); p.maxStreamsBidi > 1<<60 {
				vr.bad = true
			}
		case paramInitialMaxStreamsUni:
			if p.maxStreamsUni = intValue(); p.maxStreamsUni > 1<<60 {
				vr.bad = true
			}
		case paramAckDelayExponent:
			if p.ackDelayExponent = intValue(); p.ackDelayExponent > 20 {
				vr.bad = true
			}
		case paramMaxAckDelay:
			ms := intValue()
			if ms >= 1<<14 {
				vr.bad = true
			}
			p.maxAckDelay = time.Duration(ms) * time.Millisecond
		case paramDisableMigration:
			if len(v) != 0 {
				vr.bad = true
			}
			p.disableMigration = true
		case paramPreferredAddress:
			// Migrating to it is optional; it is not done.
			serverOnly = true
		case paramActiveCIDLimit:
			if p.activeCIDLimit = intValue(); p.activeCIDLimit < 2 {
				vr.bad = true
			}
		case paramInitialSCID:
			p.initialSCID, p.hasInitialSCID = bytes.Clone(v), true
		case paramRetrySCID:
			p.retrySCID, p.hasRetrySCID, serverOnly = bytes.Clone(v), true, true
		}
		if vr.bad {
			return p, transportErr(errTransportParameter, "invalid transport parameter")
		}
		if serverOnly && !isClient {
			return p, transportErr(errTransportParameter, "server-only transport parameter from a client")
		}
	}
	return p, nil
}
