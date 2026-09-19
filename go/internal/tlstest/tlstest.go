// Package tlstest issues a throwaway certificate for tests that need TLS.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"time"
)

var (
	once   sync.Once
	cert   tls.Certificate
	pool   *x509.CertPool
	issued error
)

// Configs returns a server config holding a self-signed certificate for
// localhost and 127.0.0.1, and a client config that trusts it. Each call
// returns fresh configs around the same certificate.
func Configs() (server, client *tls.Config, err error) {
	once.Do(issue)
	if issued != nil {
		return nil, nil, issued
	}
	server = &tls.Config{Certificates: []tls.Certificate{cert}}
	client = &tls.Config{RootCAs: pool, ServerName: "localhost"}
	return server, client, nil
}

func issue() {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		issued = err
		return
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fib test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		issued = err
		return
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		issued = err
		return
	}
	cert = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
	pool = x509.NewCertPool()
	pool.AddCert(leaf)
}
