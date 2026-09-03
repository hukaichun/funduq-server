// pod-probe-agent: a single static binary that comes alive inside a pod,
// dials out to a souk, and answers read-only questions about the pod's
// state. No inbound port, no LLM key of its own, no ability to change
// anything it inspects. See README.md for the why.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func main() {
	cfg := loadConfig()

	id, err := loadOrCreateIdentity(cfg.identityPath)
	if err != nil {
		fatal("identity: %v", err)
	}
	logf("provider identity %s… serving agent %q against %s", id.PublicHex()[:16], cfg.agentName, cfg.soukHTTPURL)

	var llm *LLMClient
	if cfg.llmBaseURL != "" || cfg.soukKyokURL != "" {
		llm = NewLLMClient(cfg.llmBaseURL, cfg.llmAPIKey, cfg.llmModel, cfg.soukKyokURL, id)
	}
	brain := NewBrain(cfg.probeRoot, llm)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Registration lives on the link now: every connect is the full
	// ceremony — a fresh ticket, a fresh handshake, a fresh register frame.
	// A dropped socket ends nothing on souk's side — reconnecting and
	// re-registering is the whole recovery, and the reconnect delay keeps a
	// souk that is briefly down from being hammered.
	backoff := time.Second
	for ctx.Err() == nil {
		if err := connectOnce(ctx, cfg, id, brain); err != nil {
			logf("connection ended: %v; reconnecting in %s", err, backoff)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	logf("shutting down")
}

func connectOnce(ctx context.Context, cfg config, id *Identity, brain *Brain) error {
	// The out-of-band half first: a single-use ticket and souk's public
	// key from POST /tickets. A pinned key is checked in there, BEFORE
	// anything is signed — a proof for the wrong souk is never computed.
	ticketCtx, cancelTicket := context.WithTimeout(ctx, 30*time.Second)
	ticket, funduqKey, err := fetchTicket(ticketCtx, cfg.soukHTTPURL, id.PublicHex(), cfg.soukPubKey)
	cancelTicket()
	if err != nil {
		return fmt.Errorf("ticket: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	ws, _, err := websocket.Dial(dialCtx, cfg.wsURL(), nil)
	cancel()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	// Big enough for a full RunAgentInput with resent history; the default
	// read limit is small and a real thread would trip it.
	ws.SetReadLimit(8 << 20)
	defer ws.CloseNow()

	conn := &SoukConn{
		id:           id,
		funduqKey:    funduqKey,
		ticket:       ticket,
		agentNames:   []string{cfg.agentName},
		providerName: cfg.providerName,
		maxRuns:      cfg.maxRuns,
		ws:           ws,
	}
	if err := conn.handshake(ctx); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	// Registration moved onto the authenticated link: nothing is offered
	// until souk echoes `registered`, so a souk that restarted and forgot
	// this provider is healed by the reconnect itself.
	if err := conn.register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	logf("attached; serving runs")
	return conn.serve(ctx, brain.Answer)
}

type config struct {
	soukHTTPURL  string
	soukKyokURL  string
	soukPubKey   ed25519.PublicKey
	identityPath string
	agentName    string
	providerName string
	probeRoot    string
	maxRuns      int
	llmBaseURL   string
	llmAPIKey    string
	llmModel     string
}

func (c config) wsURL() string {
	u := strings.TrimRight(c.soukHTTPURL, "/") + "/ws/provider"
	if strings.HasPrefix(u, "https://") {
		return "wss://" + strings.TrimPrefix(u, "https://")
	}
	return "ws://" + strings.TrimPrefix(u, "http://")
}

func loadConfig() config {
	c := config{
		soukHTTPURL:  env("SOUK_HTTP_URL", "http://souk:8000"),
		identityPath: env("SOUK_IDENTITY_KEY_PATH", "/data/probe_identity.key"),
		agentName:    env("PROBE_AGENT_NAME", defaultAgentName()),
		providerName: env("PROBE_PROVIDER_NAME", "pod probe"),
		probeRoot:    env("PROBE_ROOT", "/app"),
		llmBaseURL:   env("LLM_BASE_URL", ""),
		llmAPIKey:    env("LLM_API_KEY", ""),
		llmModel:     env("LLM_MODEL_NAME", "gpt-4"),
	}
	c.soukKyokURL = env("SOUK_KYOK_URL", strings.TrimRight(c.soukHTTPURL, "/")+"/kyok/v1/chat/completions")

	c.maxRuns = 1
	if v := os.Getenv("PROBE_MAX_CONCURRENT_RUNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.maxRuns = n
		}
	}

	if pinned := os.Getenv("SOUK_PUBLIC_KEY"); pinned != "" {
		key, err := hex.DecodeString(pinned)
		if err != nil || len(key) != ed25519.PublicKeySize {
			fatal("SOUK_PUBLIC_KEY is not a 32-byte hex ed25519 key")
		}
		c.soukPubKey = ed25519.PublicKey(key)
	}
	return c
}

// defaultAgentName uses the pod's hostname, which Kubernetes sets to the pod
// name — so a probe in each pod shows up on the roster as that pod, and the
// docent's map of who-is-online reads as a map of which pods are alive.
func defaultAgentName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return "probe-" + h
	}
	return "pod-probe"
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[pod-probe] "+format+"\n", args...)
}

func fatal(format string, args ...any) {
	logf(format, args...)
	os.Exit(1)
}
