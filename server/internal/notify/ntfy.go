// Package notify posts alerts to an ntfy topic in one of two formats.
//
// FormatNtfy is ntfy's own publishing format: the message text as a plain body,
// with title, priority and tags in headers. A person subscribed to the topic in
// the ntfy app reads it as a notification.
//
// FormatKumaJSON is a JSON object in the shape Uptime Kuma's webhook
// notification produces. A consumer that already parses Kuma's webhooks
// therefore needs no second parser, which is the reason for following someone
// else's field names rather than inventing better ones. In the ntfy app the
// same message shows as raw JSON.
//
// Both formats are built from one Payload, so the fields below are required in
// either; in FormatNtfy they become the title and the priority.
//
// The four required fields — `schema` (the number 1), `monitor_id` (a number),
// `service` (a non-empty string) and `status` (0 = problem, 1 = resolved) — are
// required because a consumer that filters on them drops a message missing one
// of them, and it drops it on its own side: the sender sees a 200 from ntfy and
// nothing else. Send therefore rejects such a payload here, where the reason is
// visible. `hostname`, `monitor_type`, `since` and `msg` are optional.
//
// `monitor_id` identifies the condition, not the firing. A consumer is free to
// group messages by it — an alerting UI shows one row per id, a chat bot one
// thread — and two conditions sharing an id become indistinguishable there.
//
// The deployment this was built for documents its receiving side in its own
// infrastructure repository; nothing in this package depends on that document.

package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Format names the wire format of a message on the topic.
type Format string

const (
	// FormatNtfy posts the message as text with ntfy's title, priority and tags
	// headers.
	FormatNtfy Format = "ntfy"
	// FormatKumaJSON posts a JSON object in Uptime Kuma's webhook shape.
	FormatKumaJSON Format = "kuma-json"
)

// ValidFormat reports whether f is one of the formats Send can produce.
func ValidFormat(f Format) bool {
	return f == FormatNtfy || f == FormatKumaJSON
}

// Status values as the adapter reads them.
const (
	StatusProblem  = 0
	StatusResolved = 1
)

// Payload is one message on the topic.
type Payload struct {
	Schema      int    `json:"schema"`
	MonitorID   int    `json:"monitor_id"`
	Service     string `json:"service"`
	Status      int    `json:"status"`
	Hostname    string `json:"hostname,omitempty"`
	MonitorType string `json:"monitor_type,omitempty"`
	Since       string `json:"since,omitempty"`
	Msg         string `json:"msg,omitempty"`
}

// Target is where a payload goes. It is passed per call rather than held by the
// Notifier because the channel is configured in the Settings UI: an edit has to
// take effect on the next check, without a restart.
type Target struct {
	// URL is the full topic URL, e.g. https://ntfy.example.com/freereps-alerts
	URL string
	// Hostname fills the payload's `hostname`, naming the deployment the alert
	// came from.
	Hostname string
	// Format selects the wire format; empty means FormatKumaJSON, the only
	// format before the field existed.
	Format Format
}

// Sender posts an alert. The watcher takes this interface so its rules can be
// tested without an HTTP server.
type Sender interface {
	Send(ctx context.Context, target Target, p Payload) error
}

// Notifier posts payloads over HTTP.
type Notifier struct {
	httpClient *http.Client
	log        *slog.Logger
}

// New returns a Notifier.
func New(log *slog.Logger) *Notifier {
	return &Notifier{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		log:        log,
	}
}

// Send posts one payload. It fills the fields the contract requires and this
// package owns — `schema`, `hostname`, `monitor_type` and, when the caller left
// it empty, `since`.
//
// A payload a consumer would drop is rejected here instead of being sent, so the
// failure names the missing field rather than surfacing as an alert that was
// never delivered.
func (n *Notifier) Send(ctx context.Context, target Target, p Payload) error {
	if target.URL == "" {
		return fmt.Errorf("no ntfy url configured")
	}
	if p.MonitorID == 0 {
		return fmt.Errorf("monitor_id is required")
	}
	if p.Service == "" {
		return fmt.Errorf("service is required")
	}
	if p.Status != StatusProblem && p.Status != StatusResolved {
		return fmt.Errorf("status must be %d or %d, got %d", StatusProblem, StatusResolved, p.Status)
	}

	p.Schema = 1
	if p.Hostname == "" {
		p.Hostname = target.Hostname
	}
	if p.MonitorType == "" {
		p.MonitorType = "freereps"
	}
	if p.Since == "" {
		p.Since = time.Now().UTC().Format(time.RFC3339)
	}

	var req *http.Request
	var err error
	switch target.Format {
	case FormatNtfy:
		req, err = ntfyRequest(ctx, target.URL, p)
	case FormatKumaJSON, "":
		req, err = kumaJSONRequest(ctx, target.URL, p)
	default:
		return fmt.Errorf("unknown alert format %q", target.Format)
	}
	if err != nil {
		return err
	}

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("posting alert: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("ntfy answered %d", resp.StatusCode)
	}

	n.log.Info("alert sent",
		"format", target.Format,
		"monitor_id", p.MonitorID,
		"service", p.Service,
		"status", p.Status,
	)
	return nil
}

// kumaJSONRequest builds the POST for FormatKumaJSON.
func kumaJSONRequest(ctx context.Context, url string, p Payload) (*http.Request, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshalling alert: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating alert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// ntfyRequest builds the POST for FormatNtfy, using the headers documented at
// https://docs.ntfy.sh/publish/.
//
// A problem is sent at priority 4 (high), a resolution at 3 (the default): a
// problem is what a subscriber has to see, and ntfy clients sort and sound by
// priority. The title carries the service and the status, because a client's
// notification list shows the title and truncates the body.
//
// Header values are ASCII here: the service names and the status words are
// constants of this package and the watcher, and the hostname is a host name.
// The body is UTF-8 and may carry the em dash and ellipsis of the messages.
func ntfyRequest(ctx context.Context, url string, p Payload) (*http.Request, error) {
	title := p.Service + ": problem"
	priority := "4"
	tags := "warning"
	if p.Status == StatusResolved {
		title = p.Service + ": resolved"
		priority = "3"
		tags = "white_check_mark"
	}

	body := p.Msg
	if body == "" {
		body = title
	}
	body += fmt.Sprintf("\n\nmonitor_id %d · host %s · since %s", p.MonitorID, p.Hostname, p.Since)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating alert request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("X-Title", title)
	req.Header.Set("X-Priority", priority)
	req.Header.Set("X-Tags", tags)
	return req, nil
}
