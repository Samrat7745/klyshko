/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package castor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type recorded struct {
	method, path, accept, contentType string
}

func recordingServer(t *testing.T, status int, body string) (*httptest.Server, func() []recorded) {
	var mu sync.Mutex
	var reqs []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, recorded{r.Method, r.URL.Path, r.Header.Get("Accept"), r.Header.Get("Content-Type")})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []recorded { mu.Lock(); defer mu.Unlock(); return append([]recorded(nil), reqs...) }
}

func TestActivateRequestShape(t *testing.T) {
	srv, reqs := recordingServer(t, 200, "")
	id := uuid.New()
	if err := NewClient(srv.URL).ActivateTupleChunk(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got := reqs()
	if len(got) != 1 || got[0].method != http.MethodPut || got[0].path != "/intra-vcp/tuple-chunks/activate/"+id.String() {
		t.Fatalf("unexpected request %+v", got)
	}
}

func TestActivateStatusCodes(t *testing.T) {
	for _, code := range []int{201, 204, 400, 404, 409, 500, 503} {
		srv, _ := recordingServer(t, code, "body")
		err := NewClient(srv.URL).ActivateTupleChunk(context.Background(), uuid.New())
		if err == nil {
			t.Errorf("status %d must be an error", code)
		} else if !strings.Contains(err.Error(), "status code") {
			t.Errorf("status %d: error should mention status code: %v", code, err)
		}
	}
	srv, _ := recordingServer(t, 200, "")
	if err := NewClient(srv.URL).ActivateTupleChunk(context.Background(), uuid.New()); err != nil {
		t.Fatal(err)
	}
}

func TestActivateTimeoutAndCancellation(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	c := NewClient(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.ActivateTupleChunk(ctx, uuid.New()); err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("expected deadline error quickly, got %v after %v", err, time.Since(start))
	}
	if _, err := c.GetTelemetry(ctx); err == nil {
		t.Fatal("telemetry: expected deadline error")
	}

	// Already cancelled context: no request is sent at all
	var hits atomic.Int32
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv2.Close()
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if err := NewClient(srv2.URL).ActivateTupleChunk(cctx, uuid.New()); err == nil {
		t.Fatal("expected cancellation error")
	}
	if hits.Load() != 0 {
		t.Fatal("request sent despite cancelled context")
	}

	// Client-level timeout applies without context deadline
	slow := NewClient(srv.URL)
	slow.client.Timeout = 100 * time.Millisecond
	if err := slow.ActivateTupleChunk(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected client timeout")
	}
}

func TestRedirects(t *testing.T) {
	var target atomic.Int32
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			target.Add(1)
		}
	}))
	defer final.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redir.Close()
	if err := NewClient(redir.URL).ActivateTupleChunk(context.Background(), uuid.New()); err != nil || target.Load() != 1 {
		t.Fatalf("307 should be followed keeping PUT: err=%v hits=%d", err, target.Load())
	}

	loop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer loop.Close()
	if err := NewClient(loop.URL).ActivateTupleChunk(context.Background(), uuid.New()); err == nil {
		t.Fatal("redirect loop must fail")
	}
}

func TestTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"metrics":[]}`)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if err := c.ActivateTupleChunk(context.Background(), uuid.New()); err == nil {
		t.Fatal("untrusted certificate must be rejected")
	}
	if _, err := c.GetTelemetry(context.Background()); err == nil {
		t.Fatal("untrusted certificate must be rejected (telemetry)")
	}
	c.client = srv.Client()
	if _, err := c.GetTelemetry(context.Background()); err != nil {
		t.Fatalf("trusted certificate: %v", err)
	}
	// plain HTTP client against TLS server fails on handshake; TLS client against plain server too
	plain, _ := recordingServer(t, 200, "")
	tlsURL := "https" + strings.TrimPrefix(plain.URL, "http")
	if err := NewClient(tlsURL).ActivateTupleChunk(context.Background(), uuid.New()); err == nil {
		t.Fatal("https against plain http server must fail")
	}
}

func TestTelemetryRequestAndParsing(t *testing.T) {
	want := Telemetry{TupleMetrics: []TupleMetrics{{Available: 5, ConsumptionRate: 2, TupleType: "BIT_GFP"}, {Available: 0, TupleType: "X"}}}
	enc, _ := json.Marshal(want)
	srv, reqs := recordingServer(t, 200, string(enc))
	got, err := NewClient(srv.URL).GetTelemetry(context.Background())
	if err != nil || len(got.TupleMetrics) != 2 || got.TupleMetrics[0] != want.TupleMetrics[0] {
		t.Fatalf("got %v %v", got, err)
	}
	r := reqs()
	if len(r) != 1 || r[0].method != "GET" || r[0].path != "/intra-vcp/telemetry" || r[0].accept != "application/json" || r[0].contentType != "application/json" {
		t.Fatalf("unexpected request %+v", r)
	}

	for name, body := range map[string]string{
		"empty": "", "garbage": "<html>", "wrong type": `{"metrics":"x"}`, "truncated": `{"metrics":[{"available":`, "bad field": `{"metrics":[{"available":"many"}]}`,
	} {
		s, _ := recordingServer(t, 200, body)
		if _, err := NewClient(s.URL).GetTelemetry(context.Background()); err == nil {
			t.Errorf("%s: expected parse error", name)
		}
	}
	// Tolerated shapes
	for name, body := range map[string]string{"null": `null`, "empty object": `{}`, "unknown fields": `{"metrics":[],"extra":1}`} {
		s, _ := recordingServer(t, 200, body)
		got, err := NewClient(s.URL).GetTelemetry(context.Background())
		if err != nil || len(got.TupleMetrics) != 0 {
			t.Errorf("%s: %v %v", name, got, err)
		}
	}
	for _, code := range []int{404, 500} {
		s, _ := recordingServer(t, code, `{"metrics":[]}`)
		if _, err := NewClient(s.URL).GetTelemetry(context.Background()); err == nil {
			t.Errorf("status %d must error", code)
		}
	}
	// unreachable
	s, _ := recordingServer(t, 200, "{}")
	s.Close()
	if _, err := NewClient(s.URL).GetTelemetry(context.Background()); err == nil {
		t.Error("unreachable must error")
	}
	if _, err := NewClient("http://bad host").GetTelemetry(context.Background()); err == nil {
		t.Error("invalid URL must error")
	}
	if err := NewClient("http://bad host").ActivateTupleChunk(context.Background(), uuid.New()); err == nil {
		t.Error("invalid URL must error")
	}
}

func TestTelemetryDeepCopyIsIndependent(t *testing.T) {
	orig := Telemetry{TupleMetrics: []TupleMetrics{{Available: 1}}}
	c := orig.DeepCopy()
	c.TupleMetrics[0].Available = 99
	if orig.TupleMetrics[0].Available != 1 {
		t.Fatal("copy shares memory")
	}
	if len(Telemetry{}.DeepCopy().TupleMetrics) != 0 {
		t.Fatal("empty copy")
	}
}

type trackingBody struct {
	io.Reader
	closed *atomic.Bool
}

func (b trackingBody) Close() error { b.closed.Store(true); return nil }

type bodyTrackingTransport struct {
	closed atomic.Bool
	status int
}

func (b *bodyTrackingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: b.status, Status: "x", Header: http.Header{}, Body: trackingBody{strings.NewReader("{}"), &b.closed}, Request: r}, nil
}

// Response bodies must be closed on every path, including error statuses, to avoid leaking connections.
func TestResponseBodyAlwaysClosed(t *testing.T) {
	for _, status := range []int{200, 404, 500} {
		tr := &bodyTrackingTransport{status: status}
		c := NewClient("http://castor")
		c.client = &http.Client{Transport: tr}
		_ = c.ActivateTupleChunk(context.Background(), uuid.New())
		if !tr.closed.Load() {
			t.Errorf("activate status %d: body not closed", status)
		}
		tr = &bodyTrackingTransport{status: status}
		c.client = &http.Client{Transport: tr}
		_, _ = c.GetTelemetry(context.Background())
		if !tr.closed.Load() {
			t.Errorf("telemetry status %d: body not closed", status)
		}
	}
}

func FuzzTelemetryJSON(f *testing.F) {
	f.Add([]byte(`{"metrics":[{"available":1,"consumptionRate":2,"type":"A"}]}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"metrics":[{"available":9223372036854775808}]}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
		defer srv.Close()
		tel, err := NewClient(srv.URL).GetTelemetry(context.Background())
		if err == nil {
			_ = tel.DeepCopy()
		}
	})
}
