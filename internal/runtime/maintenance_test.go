package runtime

import (
	"context"
	"testing"

	"github.com/yym68686/oaix/internal/store"
)

type fakeFastCostRepricer struct {
	results []store.FastCostRepriceResult
	calls   int
}

func (f *fakeFastCostRepricer) RepriceFastRequestCosts(context.Context) (store.FastCostRepriceResult, error) {
	f.calls++
	if len(f.results) == 0 {
		return store.FastCostRepriceResult{}, nil
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, nil
}

func TestRunFastRequestCostRepricingStopsWhileWaitingForGPT56(t *testing.T) {
	repricer := &fakeFastCostRepricer{results: []store.FastCostRepriceResult{
		{Phase: "waiting_gpt56"},
		{Phase: "initial", ScannedLogs: 1},
	}}
	if err := runFastRequestCostRepricing(context.Background(), nil, repricer); err != nil {
		t.Fatalf("runFastRequestCostRepricing returned error: %v", err)
	}
	if repricer.calls != 1 {
		t.Fatalf("waiting repricer calls = %d, want 1", repricer.calls)
	}
}
