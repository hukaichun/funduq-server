package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/coder/websocket"
)

// runFrame is the offer that arrives after registration: {"type": "run"}
// plus the DeliveredRun envelope, model_dump(by_alias=True) on souk's side
// (see docs/upstream-contract-vectors.json, kind "delivered-run").
// agentName rides along because this provider routes by it and the
// RunAgentInput does not name it. runInput is the AG-UI RunAgentInput,
// kept raw so a KYOK token in its forwardedProps survives untouched.
type runFrame struct {
	Type      string          `json:"type"`
	RunID     string          `json:"runId"`
	ThreadID  string          `json:"threadId"`
	AgentName string          `json:"agentName"`
	Input     json.RawMessage `json:"runInput"`
}

// serve reads frames until the socket dies. This is the whole loop, and it
// is one loop on purpose: the first frame after `registered` may already be
// a run (nothing is offered before registration, but nothing waits after
// it), so there is no seam for a race to live in. Writes are serialized
// through writeFrame's mutex so a
// run's events and its finish leave in order and never interleave mid-frame
// with another run's.
func (c *SoukConn) serve(ctx context.Context, answer AnswerFunc) error {
	var writeMu sync.Mutex
	write := func(frame map[string]any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		data, err := json.Marshal(frame)
		if err != nil {
			return err
		}
		return c.ws.Write(ctx, websocket.MessageText, data)
	}

	cancels := &cancelSet{m: map[string]context.CancelFunc{}}

	for {
		_, data, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		var head struct {
			Type  string `json:"type"`
			RunID string `json:"runId"`
		}
		if err := json.Unmarshal(data, &head); err != nil {
			_ = write(map[string]any{"type": "error", "message": "unparseable frame"})
			continue
		}
		switch head.Type {
		case "run":
			var run runFrame
			if err := json.Unmarshal(data, &run); err != nil {
				_ = write(map[string]any{"type": "ack", "runId": head.RunID, "accepted": false, "reason": "run frame did not parse"})
				continue
			}
			c.startRun(ctx, run, answer, write, cancels)
		case "cancel":
			cancels.cancel(head.RunID)
		case "queryResult":
			// This agent asks souk nothing, so an answer to a question it
			// never posed is ignored rather than treated as an error.
		case "error":
			logf("souk rejected a frame: %s", string(data))
		default:
			logf("ignoring unknown frame type %q", head.Type)
		}
	}
}

// startRun acks acceptance, then runs the probe on its own goroutine so the
// read loop keeps serving — cancels for other runs, and this run's own
// cancel, must still be read while the probe works. Every path ends the run
// with a finish: souk decides the outcome from the stream ending, and a run
// whose stream never ends is a run that hangs.
func (c *SoukConn) startRun(ctx context.Context, run runFrame, answer AnswerFunc, write func(map[string]any) error, cancels *cancelSet) {
	if err := write(map[string]any{"type": "ack", "runId": run.RunID, "accepted": true}); err != nil {
		logf("run %s: ack failed: %v", run.RunID, err)
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	cancels.add(run.RunID, cancel)

	go func() {
		defer cancels.remove(run.RunID)
		defer cancel()

		question := extractQuestion(run.Input)
		kyok := extractKyokToken(run.Input)

		messageID := run.RunID + "-msg"
		emit := func(event map[string]any) {
			_ = write(map[string]any{"type": "event", "runId": run.RunID, "event": event})
		}
		emit(map[string]any{"type": "RUN_STARTED", "threadId": run.ThreadID, "runId": run.RunID})

		text, err := answer(runCtx, question, kyok)
		if err != nil {
			// A failed probe is still an answer to the caller — the words go
			// out as the message, and the run finishes normally rather than
			// as a RUN_ERROR, because nothing here is souk's fault to record.
			text = fmt.Sprintf("probe could not complete: %v", err)
		}

		emit(map[string]any{"type": "TEXT_MESSAGE_START", "messageId": messageID, "role": "assistant"})
		emit(map[string]any{"type": "TEXT_MESSAGE_CONTENT", "messageId": messageID, "delta": text})
		emit(map[string]any{"type": "TEXT_MESSAGE_END", "messageId": messageID})
		emit(map[string]any{"type": "RUN_FINISHED", "threadId": run.ThreadID, "runId": run.RunID})

		_ = write(map[string]any{"type": "finish", "runId": run.RunID})
	}()
}

// AnswerFunc turns a caller's question into a text answer, given an optional
// KYOK token for LLM calls. It is the whole seam between the wire and the
// brain: the wire never inspects what the brain does, and the brain never
// touches a frame.
type AnswerFunc func(ctx context.Context, question string, kyok *KyokToken) (string, error)

type cancelSet struct {
	mu sync.Mutex
	m  map[string]context.CancelFunc
}

func (s *cancelSet) add(runID string, c context.CancelFunc) {
	s.mu.Lock()
	s.m[runID] = c
	s.mu.Unlock()
}

func (s *cancelSet) remove(runID string) {
	s.mu.Lock()
	delete(s.m, runID)
	s.mu.Unlock()
}

func (s *cancelSet) cancel(runID string) {
	s.mu.Lock()
	c := s.m[runID]
	s.mu.Unlock()
	if c != nil {
		c()
	}
}

// extractQuestion pulls the caller's latest message text out of a
// RunAgentInput. An AG-UI client resends its whole history each turn, so the
// last user message is the current ask. content may be a plain string or the
// AG-UI parts array; both are handled, anything else yields "".
func extractQuestion(input json.RawMessage) string {
	var parsed struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(input, &parsed); err != nil {
		return ""
	}
	for i := len(parsed.Messages) - 1; i >= 0; i-- {
		if parsed.Messages[i].Role != "user" {
			continue
		}
		return contentText(parsed.Messages[i].Content)
	}
	return ""
}

func contentText(raw json.RawMessage) string {
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return asString
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) == nil {
		var b []byte
		for _, p := range parts {
			if p.Text != "" {
				b = append(b, p.Text...)
			}
		}
		return string(b)
	}
	return ""
}
