//go:build !linux

package epoll

// This package intentionally has no non-Linux implementation: its architecture
// depends on epoll and eventfd.
