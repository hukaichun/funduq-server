package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Identity is the keypair this provider *is*: it holds the private half, so
// there is nothing to look up and nothing to be told. The stored file is
// the 32-byte seed in hex — the same on-disk shape souk_agent_sdk uses, so
// a key written by either can be read by the other.
type Identity struct {
	priv ed25519.PrivateKey
}

// loadOrCreateIdentity reads the seed at path, or generates one and writes
// it 0600 if the file is absent. A pod that mounts no volume gets a fresh
// identity every restart, which is the honest thing: a restarted pod is a
// new stall, and its old roster row ages out.
func loadOrCreateIdentity(path string) (*Identity, error) {
	if data, err := os.ReadFile(path); err == nil {
		seed, err := hex.DecodeString(trimSpace(string(data)))
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("identity at %s is not a 32-byte hex seed", path)
		}
		return &Identity{priv: ed25519.NewKeyFromSeed(seed)}, nil
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	if path != "" {
		if dir := filepath.Dir(path); dir != "" {
			_ = os.MkdirAll(dir, 0o700)
		}
		if err := os.WriteFile(path, []byte(hex.EncodeToString(seed)), 0o600); err != nil {
			return nil, fmt.Errorf("write identity: %w", err)
		}
	}
	return &Identity{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

func (i *Identity) PublicHex() string {
	return hex.EncodeToString(i.priv.Public().(ed25519.PublicKey))
}

// Sign returns the hex Ed25519 signature over payload — the encoding every
// signed field on this wire uses.
func (i *Identity) Sign(payload []byte) string {
	return hex.EncodeToString(ed25519.Sign(i.priv, payload))
}

func newNonce() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	return s
}
