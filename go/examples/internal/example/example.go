// Package example holds the plumbing every example shares, so that each one
// shows only its own protocol.
package example

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib/go"
)

// Serve runs a server engine until Ctrl-C, then closes it.
func Serve(engine *fib.Engine, banner string) {
	defer engine.Close()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-interrupt; engine.Stop() }()
	fmt.Println(banner)
	if err := engine.Run(); err != nil {
		Fatal(err)
	}
}

// ClientEngine starts an engine with no listener, for dialing, and returns a
// function that stops and closes it.
func ClientEngine() (*fib.Engine, func()) {
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		Fatal(err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := engine.Run(); err != nil {
			Fatal(err)
		}
	}()
	return engine, func() {
		engine.Stop()
		<-stopped
		_ = engine.Close()
	}
}

// Message is the n-th message a client sends.
func Message(n int) string { return fmt.Sprintf("hello from fib #%d", n) }

// Fatal reports err and exits.
func Fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
