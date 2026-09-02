// The whole of what a pod-probe agent is on the wire, written against
// docs/server-mode.md and docs/wire-vectors.json rather than against any
// SDK: this binary is dropped into a pod with a souk URL and a pinned key,
// and must not need the gateway, souk core, or any Python installed to
// come alive. Every payload here is cross-checked byte-for-byte in
// wire_test.go against the vendored upstream vectors
// (docs/upstream-contract-vectors.json, contract revision 7).
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// The handshake this binary speaks: v4, the two-frame ticket handshake. A
// mismatch is refused by name on the souk side rather than failing as a
// bad signature (docs/server-mode.md).
const handshakeVersion = 4

// providerConnectPayload is the exact bytes a provider signs to open a
// link: upstream funduq_contract.provider_connect_payload, tag
// `funduq-connect-provider` over the recipient's public key, the ticket
// (which sits in the funduq-nonce seat — it is the verifier-chosen
// challenge, fetched out of band from POST /tickets and destroyed on use)
// and this side's own nonce. Naming the recipient's key is what keeps a
// proof coaxed out by one souk from being relayed to attach at another.
// What the link will serve is NOT in here — publishing a name happens on
// the open link, unsigned, because the link is the credential.
func providerConnectPayload(funduqPublicKey, ticket, providerNonce string) []byte {
	return []byte("funduq-connect-provider:" + funduqPublicKey + ":" + ticket + ":" + providerNonce)
}

// funduqConnectPayload is what souk signs so this side can tell one souk
// from another answering the same URL: upstream
// funduq_contract.funduq_connect_payload, tag `funduq-connect-funduq` over
// the ticket and this side's nonce. The welcome frame's `answer` is a
// signature over these bytes, verified under the key learned at ticket
// time before the link is treated as open.
func funduqConnectPayload(ticket, providerNonce string) []byte {
	return []byte("funduq-connect-funduq:" + ticket + ":" + providerNonce)
}

// kyokCallPayload is what one Keep Your Own Key completion call signs: the
// run-scoped bearer token, when, and a hash of the exact request body, so a
// captured signature is neither replayable against a different body nor
// usable past the freshness window (funduq_contract.kyok_call_payload).
func kyokCallPayload(bearer string, timestamp int64, bodyHash string) []byte {
	return []byte(fmt.Sprintf("funduq-kyok-call:%s:%d:%s", bearer, timestamp, bodyHash))
}

// fetchTicket is the out-of-band half of the handshake: POST /tickets
// answers a single-use ticket (~60s) and souk's public key. Whoever
// answers is telling us which key the proof will *name* — so a pinned
// provider checks the key here, before signing anything at all. A pin
// mismatch is a hard stop: the whole value of pinning is that a
// substituted souk is refused.
func fetchTicket(ctx context.Context, httpURL, publicKeyHex string, pinned ed25519.PublicKey) (ticket string, funduqKey string, err error) {
	raw, _ := json.Marshal(map[string]string{"publicKey": publicKeyHex})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(httpURL, "/")+"/tickets", bytes.NewReader(raw))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("POST /tickets answered %s", resp.Status)
	}
	var answer struct {
		Ticket          string `json:"ticket"`
		FunduqPublicKey string `json:"funduqPublicKey"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", "", fmt.Errorf("ticket response did not parse: %w", err)
	}
	if answer.Ticket == "" {
		return "", "", errors.New("POST /tickets answered without a ticket")
	}
	presented, err := hex.DecodeString(answer.FunduqPublicKey)
	if err != nil || len(presented) != ed25519.PublicKeySize {
		// souk's identity key is mandatory now, so a souk with nothing
		// valid to present here is broken, not merely anonymous.
		return "", "", errors.New("POST /tickets presented no valid funduqPublicKey")
	}
	if pinned != nil && !pinned.Equal(ed25519.PublicKey(presented)) {
		return "", "", fmt.Errorf("souk is %s…, not the pinned %s… — refusing to sign a proof for it",
			answer.FunduqPublicKey[:16], hex.EncodeToString(pinned)[:16])
	}
	if pinned == nil {
		logf("connecting to souk %s… (pin this via SOUK_PUBLIC_KEY to refuse a substitute)", answer.FunduqPublicKey[:16])
	}
	return answer.Ticket, answer.FunduqPublicKey, nil
}

// helloFrame opens the link. The proof is computed before connecting —
// this side has already decided whom it is addressing — over
// providerConnectPayload(funduqPublicKey, ticket, nonce).
type helloFrame struct {
	Type              string `json:"type"`
	Version           int    `json:"version"`
	PublicKey         string `json:"publicKey"`
	Ticket            string `json:"ticket"`
	Nonce             string `json:"nonce"`
	Proof             string `json:"proof"`
	MaxConcurrentRuns *int   `json:"maxConcurrentRuns,omitempty"`
}

// SoukConn is one provider joined to one souk over one socket: runs arrive
// on it, events and acks leave by it. There is no separate "link" object
// because over a wire the two directions are literally the same socket.
type SoukConn struct {
	id           *Identity
	funduqKey    string // hex; learned (and pin-checked) at ticket time
	ticket       string
	agentNames   []string
	providerName string
	maxRuns      int
	ws           *websocket.Conn
}

// handshake runs the two frames — hello, welcome — and returns once the
// welcome's answer verifies, or an error naming what failed. The welcome
// carries souk's signature over funduqConnectPayload(ticket, nonce); a
// welcome whose answer does not verify under the key learned at ticket
// time means whatever answered does not possess it, and the link is never
// treated as open.
//
// It does not read past welcome: registration is the caller's next step,
// and nothing is offered before it (attach is nameless in v4).
func (c *SoukConn) handshake(ctx context.Context) error {
	providerNonce := newNonce()
	maxRuns := c.maxRuns
	hello := helloFrame{
		Type:              "hello",
		Version:           handshakeVersion,
		PublicKey:         c.id.PublicHex(),
		Ticket:            c.ticket,
		Nonce:             providerNonce,
		Proof:             c.id.Sign(providerConnectPayload(c.funduqKey, c.ticket, providerNonce)),
		MaxConcurrentRuns: &maxRuns,
	}
	helloRaw, err := json.Marshal(hello)
	if err != nil {
		return fmt.Errorf("marshal hello: %w", err)
	}
	if err := c.ws.Write(ctx, websocket.MessageText, helloRaw); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	var welcome struct {
		Type            string `json:"type"`
		FunduqPublicKey string `json:"funduqPublicKey"`
		Answer          string `json:"answer"`
	}
	if err := c.readJSON(ctx, &welcome); err != nil {
		return fmt.Errorf("read welcome: %w", err)
	}
	if welcome.Type != "welcome" {
		return fmt.Errorf("expected welcome, got %q", welcome.Type)
	}
	key, err := hex.DecodeString(c.funduqKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return errors.New("funduq public key from /tickets is not a valid ed25519 key")
	}
	sig, err := hex.DecodeString(welcome.Answer)
	if err != nil {
		return errors.New("welcome answer is not hex")
	}
	if !ed25519.Verify(ed25519.PublicKey(key), funduqConnectPayload(c.ticket, providerNonce), sig) {
		return fmt.Errorf("souk answered the handshake but did not prove possession of %s… (it presented %.16s…)",
			c.funduqKey[:16], welcome.FunduqPublicKey)
	}
	return nil
}

// register publishes the roster on the open link and waits for souk's
// `registered` echo. Unsigned, and that is the point of the handshake:
// the key was proved once, when the link opened. Runs are only offered
// after registration, so serving before `registered` would idle
// registered-as-nothing; a refusal comes back as an `error` frame with the
// socket still open, and is returned so the reconnect loop retries.
func (c *SoukConn) register(ctx context.Context) error {
	frame := map[string]any{
		"type":   "register",
		"agents": agentRecords(c.agentNames),
	}
	if c.providerName != "" {
		frame["providerName"] = c.providerName
	}
	raw, _ := json.Marshal(frame)
	if err := c.ws.Write(ctx, websocket.MessageText, raw); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	var answer struct {
		Type    string   `json:"type"`
		Names   []string `json:"names"`
		Message string   `json:"message"`
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.readJSON(readCtx, &answer); err != nil {
		return fmt.Errorf("read registered: %w", err)
	}
	switch answer.Type {
	case "registered":
		logf("registered %d agent(s)", len(answer.Names))
		return nil
	case "error":
		return fmt.Errorf("souk refused this registration: %s", answer.Message)
	default:
		return fmt.Errorf("expected registered, got %q", answer.Type)
	}
}

func (c *SoukConn) readJSON(ctx context.Context, v any) error {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func agentRecords(names []string) []map[string]any {
	records := make([]map[string]any, 0, len(names))
	for _, name := range names {
		records = append(records, map[string]any{
			"name":        name,
			"description": "Read-only state probe living inside a pod: reports file build/modify times, directory listings, bounded file reads, process and env facts. Cannot write, exec, or change anything.",
		})
	}
	return records
}
