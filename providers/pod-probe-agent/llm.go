package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// KyokToken is the run-scoped bearer credential a caller opts a run into,
// carried in the RunAgentInput's forwardedProps.kyok.token. Presenting it on
// a completion call routes the call through souk's /kyok/v1 relay and bills
// it to the caller's own key — so this binary, sitting in a pod, never holds
// a model API key. Each call is additionally signed afresh (see
// kyokCallPayload), because the static token alone proves only that whoever
// calls holds a copy of it, not that they hold the identity it names.
type KyokToken struct {
	Token string
}

// LLMClient talks to one OpenAI-compatible endpoint. Its own base URL and
// key are the fallback for a run with no KYOK token; a run *with* one
// overrides both, calling souk's relay signed with this provider's identity.
type LLMClient struct {
	baseURL string
	apiKey  string
	model   string
	// soukKyokURL is where a KYOK-routed call goes — the souk this provider
	// already reaches, since forwarded props deliberately do not carry a
	// base URL (the caller's souk URL is often unreachable from inside the
	// pod's network; this one is not).
	soukKyokURL string
	identity    *Identity
	http        *http.Client
}

func NewLLMClient(baseURL, apiKey, model, soukKyokURL string, id *Identity) *LLMClient {
	return &LLMClient{
		baseURL:     baseURL,
		apiKey:      apiKey,
		model:       model,
		soukKyokURL: soukKyokURL,
		identity:    id,
		http:        &http.Client{Timeout: 120 * time.Second},
	}
}

// Interpret asks the model to answer the caller's question from the
// already-gathered facts, and nothing else. The system prompt fixes that
// boundary: the model is told these facts are all it has and to say so
// rather than invent, which keeps it honest about a pod it cannot itself
// inspect.
func (c *LLMClient) Interpret(ctx context.Context, question, facts string, kyok *KyokToken) (string, error) {
	if question == "" {
		question = "Summarize the state of this pod and flag anything modified after the pod started."
	}
	system := "You interpret read-only diagnostic facts gathered from inside a Kubernetes pod. " +
		"You are given a fixed report and a question. Answer only from the report. " +
		"If the report does not contain what the question asks, say so plainly — never guess or invent file contents, times, or processes. " +
		"Pay particular attention to files modified after the pod started: those indicate someone changed a running pod by hand."
	body := map[string]any{
		"model": c.model,
		"messages": []map[string]string{
			{"role": "system", "content": system},
			{"role": "user", "content": "Question: " + question + "\n\n--- read-only pod report ---\n" + facts},
		},
		"stream": false,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	url, headers := c.endpoint(kyok, raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("model endpoint answered %s: %s", resp.Status, truncate(string(respBody), 300))
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("model response did not parse: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("model returned no choices")
	}
	return parsed.Choices[0].Message.Content, nil
}

// endpoint picks the URL and headers for one completion call. With a KYOK
// token the call goes to souk's relay, the token as Bearer and a fresh
// per-call signature binding token+time+body; without one it goes to this
// provider's own configured endpoint with its own key.
func (c *LLMClient) endpoint(kyok *KyokToken, body []byte) (string, map[string]string) {
	if kyok != nil && kyok.Token != "" {
		timestamp := time.Now().Unix()
		digest := sha256.Sum256(body)
		bodyHash := hex.EncodeToString(digest[:])
		return c.soukKyokURL, map[string]string{
			"Content-Type":            "application/json",
			"Authorization":           "Bearer " + kyok.Token,
			"X-Souk-Kyok-Timestamp":   strconv.FormatInt(timestamp, 10),
			"X-Souk-Kyok-Signature":   c.identity.Sign(kyokCallPayload(kyok.Token, timestamp, bodyHash)),
		}
	}
	headers := map[string]string{"Content-Type": "application/json"}
	if c.apiKey != "" {
		headers["Authorization"] = "Bearer " + c.apiKey
	}
	return c.baseURL, headers
}

// extractKyokToken pulls a run's KYOK token out of the RunAgentInput's
// forwardedProps, or returns nil if the caller did not opt this run in. The
// token is the only thing souk forwards there, deliberately.
func extractKyokToken(input json.RawMessage) *KyokToken {
	// Everything funduq adds to forwardedProps sits under its own key
	// since contract revision 18 — `forwardedProps.funduq.kyok`, not
	// `forwardedProps.kyok`. Reading the old path does not fail: it finds
	// nothing, returns no token, and this agent quietly answers without a
	// model, which is the same shape as a caller who never opted in. The
	// vectors are what tell the two apart, so the delivered-run frame in
	// docs/wire-vectors.json is replayed through this function in
	// wire_test.go.
	var parsed struct {
		ForwardedProps struct {
			Funduq struct {
				Kyok struct {
					Token string `json:"token"`
				} `json:"kyok"`
			} `json:"funduq"`
		} `json:"forwardedProps"`
	}
	if err := json.Unmarshal(input, &parsed); err != nil {
		return nil
	}
	if parsed.ForwardedProps.Funduq.Kyok.Token == "" {
		return nil
	}
	return &KyokToken{Token: parsed.ForwardedProps.Funduq.Kyok.Token}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
