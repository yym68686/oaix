package proxy

import "testing"

func TestPipelineActiveRequestsAreIsolatedByCallerOwner(t *testing.T) {
	pipeline := &Pipeline{}
	releaseA1 := pipeline.trackActiveRequest(101)
	releaseA2 := pipeline.trackActiveRequest(101)
	releaseB := pipeline.trackActiveRequest(202)

	if got := pipeline.ActiveRequestsForOwner(101); got != 2 {
		t.Fatalf("owner 101 active requests = %d, want 2", got)
	}
	if got := pipeline.ActiveRequestsForOwner(202); got != 1 {
		t.Fatalf("owner 202 active requests = %d, want 1", got)
	}

	releaseA1()
	if got := pipeline.ActiveRequestsForOwner(101); got != 1 {
		t.Fatalf("owner 101 active requests after release = %d, want 1", got)
	}
	if got := pipeline.ActiveRequestsForOwner(202); got != 1 {
		t.Fatalf("owner 202 changed after another owner release: %d", got)
	}

	releaseA2()
	releaseB()
	if got := pipeline.ActiveRequestsForOwner(101); got != 0 {
		t.Fatalf("owner 101 active requests after final release = %d, want 0", got)
	}
	if got := pipeline.ActiveRequestsForOwner(202); got != 0 {
		t.Fatalf("owner 202 active requests after release = %d, want 0", got)
	}
}

func TestPipelineActiveRequestsIgnoreMissingOwner(t *testing.T) {
	pipeline := &Pipeline{}
	release := pipeline.trackActiveRequest(0)
	release()
	if got := pipeline.ActiveRequestsForOwner(0); got != 0 {
		t.Fatalf("missing owner active requests = %d, want 0", got)
	}
	var nilPipeline *Pipeline
	if got := nilPipeline.ActiveRequestsForOwner(12); got != 0 {
		t.Fatalf("nil pipeline active requests = %d, want 0", got)
	}
}
