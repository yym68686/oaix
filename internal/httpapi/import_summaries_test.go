package httpapi

import (
	"context"
	"reflect"
	"testing"

	"github.com/yym68686/oaix/internal/store"
)

type importJobSummarizerStub struct {
	calls               int
	jobs                []store.ImportJob
	includeObservedCost bool
	result              []store.ImportBatchSummary
}

func (s *importJobSummarizerStub) ImportJobSummaries(_ context.Context, jobs []store.ImportJob, includeObservedCost bool) ([]store.ImportBatchSummary, error) {
	s.calls++
	s.jobs = append([]store.ImportJob(nil), jobs...)
	s.includeObservedCost = includeObservedCost
	return s.result, nil
}

func TestImportSummariesForJobsUsesOneBulkCall(t *testing.T) {
	jobs := []store.ImportJob{{ID: 23}, {ID: 17}}
	want := []store.ImportBatchSummary{{ImportJob: jobs[0]}, {ImportJob: jobs[1]}}
	stub := &importJobSummarizerStub{result: want}

	got, err := importSummariesForJobs(context.Background(), stub, jobs, true)
	if err != nil {
		t.Fatal(err)
	}
	if stub.calls != 1 {
		t.Fatalf("expected one bulk summary call, got %d", stub.calls)
	}
	if !reflect.DeepEqual(stub.jobs, jobs) {
		t.Fatalf("bulk call jobs changed: got %+v, want %+v", stub.jobs, jobs)
	}
	if !stub.includeObservedCost {
		t.Fatal("expected observed cost option to be preserved")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("summary result changed: got %+v, want %+v", got, want)
	}
}
