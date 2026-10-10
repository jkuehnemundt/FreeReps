package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testNotifier() *Notifier {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testTarget(url string) Target {
	return Target{URL: url, Hostname: "freereps"}
}

// TestSendFillsTheRequiredContract covers the four fields a consumer filters on.
// A message missing one of them is dropped on the receiving side, where the
// reason stays — the sender sees a 200 from ntfy, so the absence looks like a
// working channel.
func TestSendFillsTheRequiredContract(t *testing.T) {
	var got map[string]any
	var contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decoding posted body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := testNotifier().Send(context.Background(), testTarget(srv.URL), Payload{
		MonitorID: 9201,
		Service:   "freereps - withings sync",
		Status:    StatusProblem,
		Msg:       "3 consecutive failed runs",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	// `schema` has to be the number 1, not the string "1".
	if v, ok := got["schema"].(float64); !ok || v != 1 {
		t.Errorf("schema = %v (%T), want the number 1", got["schema"], got["schema"])
	}
	if v, ok := got["monitor_id"].(float64); !ok || v != 9201 {
		t.Errorf("monitor_id = %v (%T), want the number 9201", got["monitor_id"], got["monitor_id"])
	}
	if got["service"] != "freereps - withings sync" {
		t.Errorf("service = %v, want the sync name", got["service"])
	}
	if v, ok := got["status"].(float64); !ok || v != 0 {
		t.Errorf("status = %v (%T), want the number 0", got["status"], got["status"])
	}
	if got["hostname"] != "freereps" {
		t.Errorf("hostname = %v, want freereps", got["hostname"])
	}
	if got["since"] == "" || got["since"] == nil {
		t.Error("since is empty; Send is supposed to stamp it")
	}
}

// TestSendRejectsIncompletePayload verifies that a payload the adapter would drop
// fails here instead. A dropped message is invisible on this side; a returned
// error names the missing field in the log.
func TestSendRejectsIncompletePayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an incomplete payload was posted")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := testNotifier()
	target := testTarget(srv.URL)
	cases := []struct {
		name string
		p    Payload
	}{
		{"no monitor_id", Payload{Service: "x", Status: StatusProblem}},
		{"no service", Payload{MonitorID: 9201, Status: StatusProblem}},
		{"status out of range", Payload{MonitorID: 9201, Service: "x", Status: 7}},
	}
	for _, c := range cases {
		if err := n.Send(context.Background(), target, c.p); err == nil {
			t.Errorf("%s: Send returned no error", c.name)
		}
	}
}

// TestSendReportsHTTPFailure exists because the watcher stores the alert state
// only after a successful send — a non-2xx answer that read as success would
// silence the retry on the next check.
func TestSendReportsHTTPFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	err := testNotifier().Send(context.Background(), testTarget(srv.URL), Payload{
		MonitorID: 9204,
		Service:   "freereps - apple health ingest",
		Status:    StatusResolved,
	})
	if err == nil {
		t.Fatal("Send returned no error on HTTP 502")
	}
}

// TestSendWithoutTargetFails covers the state a fresh database is in: no URL
// stored. Posting to an empty URL would fail with a transport error that reads
// like an outage instead of like a missing configuration.
func TestSendWithoutTargetFails(t *testing.T) {
	err := testNotifier().Send(context.Background(), Target{}, Payload{
		MonitorID: 9200,
		Service:   "freereps - channel test",
		Status:    StatusResolved,
	})
	if err == nil {
		t.Fatal("Send returned no error without a target URL")
	}
}

// TestSendNtfyFormat covers the format a person reads in the ntfy app: the
// message as the body, the service and status in the title, and a problem at a
// higher priority than its resolution, so a client sounds for the first only.
func TestSendNtfyFormat(t *testing.T) {
	type posted struct {
		contentType, title, priority, tags, body string
	}
	var got []posted
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, posted{
			contentType: r.Header.Get("Content-Type"),
			title:       r.Header.Get("X-Title"),
			priority:    r.Header.Get("X-Priority"),
			tags:        r.Header.Get("X-Tags"),
			body:        string(b),
		})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target := Target{URL: srv.URL, Hostname: "freereps", Format: FormatNtfy}
	n := testNotifier()
	for _, status := range []int{StatusProblem, StatusResolved} {
		if err := n.Send(context.Background(), target, Payload{
			MonitorID: 9201,
			Service:   "freereps - withings sync",
			Status:    status,
			Msg:       "3 consecutive failed runs — user 2: timeout",
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(got) != 2 {
		t.Fatalf("posted %d messages, want 2", len(got))
	}
	problem, resolved := got[0], got[1]
	if strings.HasPrefix(problem.body, "{") {
		t.Errorf("body is JSON, want text: %q", problem.body)
	}
	if !strings.HasPrefix(problem.body, "3 consecutive failed runs — user 2: timeout") {
		t.Errorf("body = %q, want it to start with the message", problem.body)
	}
	if !strings.Contains(problem.body, "monitor_id 9201") {
		t.Errorf("body = %q, want the monitor_id in it", problem.body)
	}
	if !strings.HasPrefix(problem.contentType, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", problem.contentType)
	}
	if problem.title != "freereps - withings sync: problem" {
		t.Errorf("problem title = %q", problem.title)
	}
	if resolved.title != "freereps - withings sync: resolved" {
		t.Errorf("resolved title = %q", resolved.title)
	}
	if problem.priority != "4" || resolved.priority != "3" {
		t.Errorf("priorities = %q / %q, want 4 / 3", problem.priority, resolved.priority)
	}
	if problem.tags == "" || problem.tags == resolved.tags {
		t.Errorf("tags = %q / %q, want two different tags", problem.tags, resolved.tags)
	}
}

// TestSendRejectsUnknownFormat keeps a stored value this package cannot produce
// from being posted in some other format.
func TestSendRejectsUnknownFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a message in an unknown format was posted")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := testNotifier().Send(context.Background(), Target{URL: srv.URL, Format: "xml"}, Payload{
		MonitorID: 9200,
		Service:   "freereps - channel test",
		Status:    StatusResolved,
	})
	if err == nil {
		t.Fatal("Send returned no error for an unknown format")
	}
}
