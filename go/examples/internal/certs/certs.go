// Package certs gives the TLS examples their certificates.
//
// A server run without -cert and -key issues itself a self-signed certificate
// for localhost and 127.0.0.1 and writes it, without the key, to
// DefaultCertFile. The matching client trusts that file by default, so the
// examples verify the server exactly as a real deployment would, with no
// InsecureSkipVerify.
package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// DefaultCertFile is where a server puts the certificate it issued itself and
// where a client looks for it.
var DefaultCertFile = filepath.Join(os.TempDir(), "fib-example-cert.pem")

// ServerFlags are the flags a TLS example server takes.
type ServerFlags struct {
	certFile, keyFile, certOut *string
}

// RegisterServerFlags adds -cert, -key and -cert-out to the command line.
func RegisterServerFlags() ServerFlags {
	return ServerFlags{
		certFile: flag.String("cert", "", "PEM certificate to serve; empty issues a self-signed one"),
		keyFile:  flag.String("key", "", "PEM private key for -cert"),
		certOut:  flag.String("cert-out", DefaultCertFile, "where to write a self-signed certificate for clients to trust"),
	}
}

// Config loads the certificate the flags name, or issues a self-signed one
// and writes it where clients will look for it.
func (f ServerFlags) Config() (*tls.Config, error) {
	if *f.certFile != "" || *f.keyFile != "" {
		cert, err := tls.LoadX509KeyPair(*f.certFile, *f.keyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	cert, err := selfSigned()
	if err != nil {
		return nil, err
	}
	if *f.certOut != "" {
		block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
		if err := os.WriteFile(*f.certOut, block, 0o644); err != nil {
			return nil, err
		}
		fmt.Printf("self-signed certificate written to %s\n", *f.certOut)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

// ClientFlags are the flags a TLS example client takes.
type ClientFlags struct {
	caFile   *string
	insecure *bool
}

// RegisterClientFlags adds -ca and -insecure to the command line.
func RegisterClientFlags() ClientFlags {
	return ClientFlags{
		caFile:   flag.String("ca", DefaultCertFile, "PEM certificate to trust; empty trusts the system roots"),
		insecure: flag.Bool("insecure", false, "skip verifying the server's certificate"),
	}
}

// Config trusts the certificate the flags name. The server name is left for
// the dialer to take from the address.
func (f ClientFlags) Config() (*tls.Config, error) {
	config := &tls.Config{InsecureSkipVerify: *f.insecure}
	if *f.insecure || *f.caFile == "" {
		return config, nil
	}
	data, err := os.ReadFile(*f.caFile)
	if err != nil {
		return nil, fmt.Errorf("%w (start the server first, or pass -ca or -insecure)", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("no certificate found in " + *f.caFile)
	}
	config.RootCAs = pool
	return config, nil
}

func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fib example"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
