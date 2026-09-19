package fib

// Layer carries a connection's sends, transforming them on the way to the
// socket. The tls package installs one that encrypts; any other protocol that
// frames or encrypts what a connection sends, such as a DTLS implementation
// over UDP, installs its own the same way.
//
// While a layer is installed, Send, SendOwned and SendParts hand their bytes
// to its Send, and CloseAfterSend calls its CloseAfterSend. The layer reaches
// the socket itself through SendRaw and CloseAfterSendRaw. Incoming bytes are
// not the layer's concern: the handler that installed it sees them in OnData
// and decides what the handler it wraps receives.
type Layer interface {
	// Send transforms first followed by second and sends the result. Neither
	// slice may be retained after it returns.
	Send(first, second []byte) error
	// CloseAfterSend ends the connection once what was already sent has gone
	// out, after whatever closing message the layer's protocol calls for.
	CloseAfterSend()
}

// CloseWithError closes the connection, as Close does, and hands err to
// OnClose. A layer uses it to report why its protocol ended the connection,
// such as a failed handshake. Only the first close's error is reported.
func (c *Connection) CloseWithError(err error) { c.closeWithError(err) }

// Handler returns the handler the engine serves accepted connections with,
// which is also what a dial that names no handler of its own gets.
func (e *Engine) Handler() Handler { return e.handler }

// SetLayer installs l, or removes the layer with nil. Install it in OnOpen,
// before anything can send, so that no send bypasses it.
func (c *Connection) SetLayer(l Layer) { c.layer = l }

// Layer returns the installed layer, or nil.
func (c *Connection) Layer() Layer { return c.layer }

// SendRaw sends data to the socket, bypassing any layer. It copies data, as
// Send does. On a UDP connection each call is one datagram.
func (c *Connection) SendRaw(data []byte) error { return c.sendRaw(data) }

// CloseAfterSendRaw is CloseAfterSend bypassing any layer.
func (c *Connection) CloseAfterSendRaw() { c.closeAfterSendRaw() }
