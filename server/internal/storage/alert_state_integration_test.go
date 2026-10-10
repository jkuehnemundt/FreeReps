//go:build integration

package storage

import (
	"context"
	"testing"
	"time"
)

// TestRunHistoryCountsTheSourcesOwnColumns catches the stale-source rule reading
// the wrong column: Hevy writes workouts and sets and never metrics_inserted, so
// a predicate on metrics_inserted alone would report a Hevy user who trains
// every day as never having stored a row. It also checks that the newest status
// is the newest run's, since the rule stays silent when that run failed.
func TestRunHistoryCountsTheSourcesOwnColumns(t *testing.T) {
	db := aggTestDB(t)
	ctx := context.Background()
	runHistoryTestUser, err := db.GetOrCreateUser(ctx, "run-history-test", "")
	if err != nil {
		t.Fatalf("creating user: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `DELETE FROM import_logs WHERE user_id = $1`, runHistoryTestUser); err != nil {
		t.Fatalf("clearing rows: %v", err)
	}

	stored := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC)
	for _, r := range []struct {
		at                    time.Time
		status                string
		workouts, sets, metrs int64
	}{
		{stored, "success", 1, 12, 0},
		{stored.Add(24 * time.Hour), "success", 0, 0, 0},
		{newest, "error", 0, 0, 0},
	} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO import_logs (user_id, created_at, source, status, workouts_inserted, sets_inserted, metrics_inserted)
			 VALUES ($1, $2, 'hevy_sync', $3, $4, $5, $6)`,
			runHistoryTestUser, r.at, r.status, r.workouts, r.sets, r.metrs); err != nil {
			t.Fatalf("inserting import log: %v", err)
		}
	}

	byUser, err := db.RunHistoryByUser(ctx, "hevy_sync")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := byUser[runHistoryTestUser]
	if !ok {
		t.Fatal("no history for the test user")
	}
	if !got.LastStoredAt.Equal(stored) {
		t.Errorf("LastStoredAt = %s, want %s", got.LastStoredAt, stored)
	}
	if !got.NewestRunAt.Equal(newest) || got.NewestStatus != "error" {
		t.Errorf("newest = %s %q, want %s \"error\"", got.NewestRunAt, got.NewestStatus, newest)
	}
}

// TestAlertSettingsKeepTheFormat checks the column order of the three
// statements that name `format`: the parameter it is bound to is the last one,
// out of column order, and a shifted binding would store the hostname as the
// format or fail the CHECK. It also checks that the CHECK refuses a value the
// notifier cannot produce.
func TestAlertSettingsKeepTheFormat(t *testing.T) {
	db := aggTestDB(t)
	ctx := context.Background()

	// alert_settings holds one row for the whole database; put back what was
	// there so a scratch database shared with other tests keeps its contents.
	prev, known, err := db.GetAlertSettings(ctx)
	if err != nil {
		t.Fatalf("reading settings: %v", err)
	}
	t.Cleanup(func() {
		if known {
			_ = db.SetAlertSettings(ctx, prev)
		} else {
			_, _ = db.Pool.Exec(ctx, `DELETE FROM alert_settings`)
		}
	})

	want := AlertSettings{
		Enabled:          true,
		NtfyURL:          "https://ntfy.example.com/freereps",
		Format:           "ntfy",
		Hostname:         "freereps-test",
		CheckInterval:    5 * time.Minute,
		FailureThreshold: 3,
		AppleSilence:     36 * time.Hour,
	}
	if err := db.SetAlertSettings(ctx, want); err != nil {
		t.Fatalf("writing settings: %v", err)
	}
	got, _, err := db.GetAlertSettings(ctx)
	if err != nil {
		t.Fatalf("reading settings: %v", err)
	}
	if got.Format != want.Format || got.Hostname != want.Hostname || got.NtfyURL != want.NtfyURL {
		t.Errorf("read back format=%q hostname=%q url=%q, want %q %q %q",
			got.Format, got.Hostname, got.NtfyURL, want.Format, want.Hostname, want.NtfyURL)
	}

	want.Format = "kuma-json"
	if err := db.SetAlertSettings(ctx, want); err != nil {
		t.Fatalf("writing settings: %v", err)
	}
	if got, _, _ := db.GetAlertSettings(ctx); got.Format != "kuma-json" {
		t.Errorf("format = %q after update, want kuma-json", got.Format)
	}

	want.Format = "xml"
	if err := db.SetAlertSettings(ctx, want); err == nil {
		t.Error("an unknown format was stored")
	}
}
