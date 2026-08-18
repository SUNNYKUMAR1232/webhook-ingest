package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"sync"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 50

	var wg sync.WaitGroup
	wg.Add(deliveries)

	for i := 0; i < deliveries; i++ {
		go func(delivery int) {
			defer wg.Done()

			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				t.Errorf(
					"delivery=%d: webhook request failed: %v",
					delivery,
					err,
				)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf(
					"delivery=%d: got status=%d, want=%d",
					delivery,
					resp.StatusCode,
					http.StatusOK,
				)
			}
		}(i)
	}

	wg.Wait()

	// Verify that only one event was persisted.
	var eventCount int

	err := st.Pool().QueryRow(
		ctx,
		`SELECT count(*)
		 FROM events
		 WHERE event_id = $1`,
		eventID,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("failed to count events: %v", err)
	}

	t.Logf(
		"event persistence result: event_id=%s, deliveries=%d, stored_events=%d",
		eventID,
		deliveries,
		eventCount,
	)

	if eventCount != 1 {
		t.Fatalf(
			"duplicate event records detected: event_id=%s, stored_events=%d, want=1",
			eventID,
			eventCount,
		)
	}

	// Verify that accounting happened only once.
	var callCount int64

	err = st.Pool().QueryRow(
		ctx,
		`SELECT call_count
		 FROM account_stats
		 WHERE account_id = $1`,
		accountID,
	).Scan(&callCount)
	if err != nil {
		t.Fatalf(
			"failed to read account stats: account_id=%s, error=%v",
			accountID,
			err,
		)
	}

	t.Logf(
		"accounting result: account_id=%s, deliveries=%d, call_count=%d",
		accountID,
		deliveries,
		callCount,
	)

	if callCount != 1 {
		t.Fatalf(
			"call count drift detected: account_id=%s, call_count=%d, want=1, deliveries=%d",
			accountID,
			callCount,
			deliveries,
		)
	}
}

func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status=%d, want=%d", resp.StatusCode, http.StatusOK)
	}

	const timeout = 2 * time.Second
	const interval = 10 * time.Millisecond

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var processed bool

		err := st.Pool().QueryRow(
			ctx,
			`SELECT recording_processed
			 FROM calls
			 WHERE call_id = $1`,
			callID,
		).Scan(&processed)

		if err == nil && processed {
			t.Logf(
				"recording processing verified: event_id=%s, call_id=%s",
				eventID,
				callID,
			)
			return
		}

		time.Sleep(interval)
	}

	t.Fatalf(
		"recording was not marked processed: event_id=%s, call_id=%s",
		eventID,
		callID,
	)
}