package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// These vectors are the cross-language source of truth. If these tests
// pass, this binary's connect proofs, welcome verification and KYOK call
// payloads are byte-identical to what the gateway and funduq core verify
// against — proven without running either. The signatures are
// deterministic Ed25519, so reproducing them under the published test key
// is equivalent to the verifier accepting them.
//
// Two files, both at the repo root and both REQUIRED (a missing file is a
// failure, not a skip — a checkout without them cannot claim the wire is
// verified):
//   - docs/wire-vectors.json: this repo's frame vocabulary + handshake
//     version.
//   - docs/upstream-contract-vectors.json: upstream funduq's payload
//     vectors, vendored verbatim at contract revision 16.

func repoRoot(t *testing.T) string {
	// providers/pod-probe-agent -> repo root
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func mustRead(t *testing.T, elem ...string) []byte {
	path := filepath.Join(append([]string{repoRoot(t)}, elem...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("required vector file missing: %v", err)
	}
	return data
}

// TestHandshakeVersionAndVocabulary pins this binary to the wire version
// and frame shapes the gateway publishes: hello carries exactly the fields
// wire.go serializes, welcome exactly the ones handshake() reads.
func TestHandshakeVersionAndVocabulary(t *testing.T) {
	data := mustRead(t, "docs", "wire-vectors.json")
	var vf struct {
		HandshakeVersion int    `json:"handshake_version"`
		PayloadVectors   string `json:"payload_vectors"`
		Handshake        struct {
			Hello   []string `json:"hello"`
			Welcome []string `json:"welcome"`
		} `json:"handshake"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatal(err)
	}

	if vf.HandshakeVersion != handshakeVersion {
		t.Fatalf("handshake version drift: file %d, binary %d", vf.HandshakeVersion, handshakeVersion)
	}
	if vf.PayloadVectors != "docs/upstream-contract-vectors.json" {
		t.Fatalf("payload vectors moved: %q — update the tests below", vf.PayloadVectors)
	}

	wantHello := map[string]bool{}
	for _, f := range vf.Handshake.Hello {
		wantHello[f] = true
	}
	n := 1
	raw, err := json.Marshal(helloFrame{MaxConcurrentRuns: &n})
	if err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	if err := json.Unmarshal(raw, &sent); err != nil {
		t.Fatal(err)
	}
	for field := range sent {
		if !wantHello[field] && !wantHello[field+"?"] {
			t.Errorf("hello sends %q, which the published vocabulary does not name", field)
		}
	}
	for _, field := range vf.Handshake.Welcome {
		switch field {
		case "type", "funduqPublicKey", "answer":
		default:
			t.Errorf("welcome vocabulary names %q, which handshake() does not read", field)
		}
	}
}

// TestContractVectors replays the vendored upstream payload vectors: build
// each payload this binary can produce or must verify from `inputs`,
// assert the exact bytes, and reproduce the published deterministic
// signature under the published test key. provider-connect is the proof in
// the hello frame (the ticket sits in the funduq_nonce seat);
// funduq-connect is the welcome's answer this side verifies; kyok-call is
// what a KYOK completion call signs; resolution, cancel and view are the
// singular-act family, which this binary does not sign today but keeps
// byte-exact so a reshape upstream is caught here.
//
// Every kind published at revision 16 is replayed — there is no longer a
// vector this file skips. `delegation` is gone from the file entirely
// (revision 15 deleted the certificate and the funduq-delegate tag with
// it), and `resolution` changed shape at 16: an ask hash where a
// timestamp used to be.
func TestContractVectors(t *testing.T) {
	data := mustRead(t, "docs", "upstream-contract-vectors.json")
	var cf struct {
		Contract struct {
			Revision int `json:"revision"`
		} `json:"contract"`
		TestKey struct {
			PrivateHex string `json:"private_key_hex"`
			PublicHex  string `json:"public_key_hex"`
		} `json:"test_key"`
		Vectors []struct {
			Kind   string `json:"kind"`
			Inputs struct {
				FunduqPublicKey string   `json:"funduq_public_key"`
				FunduqNonce     string   `json:"funduq_nonce"`
				ProviderNonce   string   `json:"provider_nonce"`
				Bearer          string   `json:"bearer"`
				Timestamp       int64    `json:"timestamp"`
				BodyUTF8        string   `json:"body_utf8"`
				BodyHashHex     string   `json:"body_sha256_hex"`
				RunID           string   `json:"run_id"`
				AskIDs          []string `json:"ask_ids"`
			} `json:"inputs"`
			PayloadUTF8  string `json:"payload_utf8"`
			SignatureHex string `json:"signature_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatal(err)
	}
	if cf.Contract.Revision != 16 {
		t.Fatalf("vendored vectors are contract revision %d; this binary is written against 16 — re-read the changelog before bumping", cf.Contract.Revision)
	}

	testKey := ed25519.NewKeyFromSeed(mustHex(t, cf.TestKey.PrivateHex))
	seen := map[string]bool{}
	for _, v := range cf.Vectors {
		var payload []byte
		switch v.Kind {
		case "provider-connect":
			// The ticket occupies the funduq_nonce seat: it is the
			// verifier-chosen challenge, fetched from POST /tickets.
			payload = providerConnectPayload(v.Inputs.FunduqPublicKey, v.Inputs.FunduqNonce, v.Inputs.ProviderNonce)
		case "funduq-connect":
			payload = funduqConnectPayload(v.Inputs.FunduqNonce, v.Inputs.ProviderNonce)
		case "kyok-call":
			digest := sha256.Sum256([]byte(v.Inputs.BodyUTF8))
			if got := hex.EncodeToString(digest[:]); got != v.Inputs.BodyHashHex {
				t.Errorf("kyok-call body hash:\n  got  %s\n  want %s", got, v.Inputs.BodyHashHex)
			}
			payload = kyokCallPayload(v.Inputs.Bearer, v.Inputs.Timestamp, v.Inputs.BodyHashHex)
		case "resolution":
			// The published ask ids are deliberately out of order
			// (["tool_b", "int_1"]), so a builder that forgot to sort
			// fails this line and not some later integration.
			if len(v.Inputs.AskIDs) < 2 || sort.StringsAreSorted(v.Inputs.AskIDs) {
				t.Errorf("resolution vector no longer carries unsorted ask ids (%q) — it can no longer catch a builder that skips the sort", v.Inputs.AskIDs)
			}
			payload = resolvePayload(v.Inputs.RunID, v.Inputs.AskIDs)
		case "cancel":
			payload = cancelPayload(v.Inputs.RunID, v.Inputs.Timestamp)
		case "view":
			payload = viewPayload(v.Inputs.RunID, v.Inputs.Timestamp)
		default:
			t.Errorf("vendored vectors carry an unknown kind %q — read the changelog and either implement it or say here why this binary ignores it", v.Kind)
			continue
		}
		seen[v.Kind] = true
		if string(payload) != v.PayloadUTF8 {
			t.Errorf("%s payload:\n  got  %q\n  want %q", v.Kind, payload, v.PayloadUTF8)
			continue
		}
		// Deterministic Ed25519: our signature must equal the published
		// one — and for funduq-connect this doubles as proof that the
		// verification in handshake() checks the right bytes.
		got := hex.EncodeToString(ed25519.Sign(testKey, payload))
		if got != v.SignatureHex {
			t.Errorf("%s signature:\n  got  %s\n  want %s", v.Kind, got, v.SignatureHex)
		}
	}
	for _, kind := range []string{"provider-connect", "funduq-connect", "kyok-call", "resolution", "cancel", "view"} {
		if !seen[kind] {
			t.Errorf("vendored vectors carry no %q entry — the file is truncated or the kind was renamed", kind)
		}
	}
	if seen["delegation"] {
		t.Error("a delegation vector is back — the certificate was deleted at revision 15; do not re-implement it without an entry in the changelog saying it returned")
	}
}

// TestResolvePayloadCanonicalization pins the two properties the published
// vector alone cannot show, because it carries exactly one ask set: order
// must not matter, and the ask set must be all that does.
func TestResolvePayloadCanonicalization(t *testing.T) {
	a := string(resolvePayload("run_1", []string{"tool_b", "int_1"}))
	b := string(resolvePayload("run_1", []string{"int_1", "tool_b"}))
	if a != b {
		t.Errorf("ask id order changed the payload:\n  %s\n  %s", a, b)
	}
	// The empty set is legal and hashes the empty byte string — the same
	// sha256 upstream's builder produces for it.
	empty := string(resolvePayload("run_1", nil))
	const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if want := "funduq-resolve:run_1:" + emptyHash; empty != want {
		t.Errorf("empty ask set:\n  got  %s\n  want %s", empty, want)
	}
	// NUL-joined, not concatenated: two ids must not collide with one.
	if string(resolvePayload("run_1", []string{"a", "b"})) == string(resolvePayload("run_1", []string{"ab"})) {
		t.Error("ask ids are being concatenated without the NUL separator")
	}
	// The caller's slice is the caller's: sorting in place would reorder
	// the ask list the caller still has to send alongside the proof.
	given := []string{"tool_b", "int_1"}
	resolvePayload("run_1", given)
	if given[0] != "tool_b" {
		t.Errorf("resolvePayload sorted the caller's slice in place: %q", given)
	}
}

// TestRegisterDeclaresInterjectionCapability holds the per-agent field souk
// needs at registration. Core overwrites its stored value from what the
// link declares, so an omission is not "unknown" — it is a claim, and this
// probe's claim is false.
func TestRegisterDeclaresInterjectionCapability(t *testing.T) {
	records := agentRecords([]string{"pod-probe"})
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}
	value, present := records[0]["takesInterjections"]
	if !present {
		t.Fatal("register omits takesInterjections; souk would record this agent's capability from a field that is not there")
	}
	if value != false {
		t.Errorf("takesInterjections = %v; the probe never pauses, so it takes none", value)
	}
	// Registration forbids extra keys upstream, so a stray field is a
	// refused registration rather than an ignored one.
	for field := range records[0] {
		switch field {
		case "name", "description", "agentCardExtra", "metadata", "takesInterjections":
		default:
			t.Errorf("register sends %q, which Registration (extra=forbid) would refuse", field)
		}
	}
}

// TestWelcomeVerification drives handshake()'s verification logic through
// the vendored funduq-connect vector: an answer signed by the published
// test key verifies under its public key, and a tampered one does not.
func TestWelcomeVerification(t *testing.T) {
	data := mustRead(t, "docs", "upstream-contract-vectors.json")
	var cf struct {
		TestKey struct {
			PublicHex string `json:"public_key_hex"`
		} `json:"test_key"`
		Vectors []struct {
			Kind   string `json:"kind"`
			Inputs struct {
				FunduqNonce   string `json:"funduq_nonce"`
				ProviderNonce string `json:"provider_nonce"`
			} `json:"inputs"`
			SignatureHex string `json:"signature_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatal(err)
	}
	pub := ed25519.PublicKey(mustHex(t, cf.TestKey.PublicHex))
	for _, v := range cf.Vectors {
		if v.Kind != "funduq-connect" {
			continue
		}
		payload := funduqConnectPayload(v.Inputs.FunduqNonce, v.Inputs.ProviderNonce)
		if !ed25519.Verify(pub, payload, mustHex(t, v.SignatureHex)) {
			t.Error("published funduq-connect signature does not verify — payload template is wrong")
		}
		wrong := funduqConnectPayload(v.Inputs.FunduqNonce, "not-the-nonce")
		if ed25519.Verify(pub, wrong, mustHex(t, v.SignatureHex)) {
			t.Error("signature verified over the wrong nonce — verification is not binding the payload")
		}
		return
	}
	t.Fatal("no funduq-connect vector found")
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
