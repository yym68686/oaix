package importer

import (
	"context"
	"testing"

	"github.com/yym68686/oaix/internal/store"
)

func TestJobAndItemTransitions(t *testing.T) {
	if !CanTransitionJob(JobQueued, JobRunning) || CanTransitionJob(JobCompleted, JobRunning) {
		t.Fatal("unexpected job transition result")
	}
	if !CanTransitionItem(ItemValidating, ItemValidated) || CanTransitionItem(ItemFailed, ItemQueued) {
		t.Fatal("unexpected item transition result")
	}
}

func TestWorkerValidateBatchAccessTokens(t *testing.T) {
	worker := &Worker{MaxConcurrency: 2}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{
		{ID: 1, Payload: map[string]any{"access_token": "at-1"}},
		{ID: 2, Payload: map[string]any{"bad": "payload"}},
	})
	if len(updates) != 2 {
		t.Fatalf("updates len = %d", len(updates))
	}
	if updates[0].Status != string(ItemValidated) || updates[0].ValidatedPayload["access_token"] != "at-1" {
		t.Fatalf("unexpected first update: %+v", updates[0])
	}
	if updates[1].Status != string(ItemFailed) || updates[1].ErrorMessage == "" {
		t.Fatalf("unexpected second update: %+v", updates[1])
	}
	stats := worker.Stats()
	if stats.Claimed != 2 || stats.Validated != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
