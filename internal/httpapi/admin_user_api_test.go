package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type fakePlatformPoolSummaryStore struct {
	poolByOwner          map[int64]store.OwnerTokenPoolSummary
	usageByOwner         map[int64]store.OwnerUsageSummary
	observedCostsByOwner map[int64]float64
	sub2APICostsByOwner  map[int64]float64
	poolErr              error
	usageErr             error
	costErr              error
	sub2APICostErr       error
}

func (f fakePlatformPoolSummaryStore) TokenPoolSummariesByOwner(context.Context, []int64, time.Time) (map[int64]store.OwnerTokenPoolSummary, error) {
	return f.poolByOwner, f.poolErr
}

func (f fakePlatformPoolSummaryStore) RequestUsageByTokenOwner(context.Context, []int64, int) (map[int64]store.OwnerUsageSummary, error) {
	return f.usageByOwner, f.usageErr
}

func (f fakePlatformPoolSummaryStore) TokenObservedCostsByOwner(context.Context, []int64) (map[int64]float64, error) {
	return f.observedCostsByOwner, f.costErr
}

func (f fakePlatformPoolSummaryStore) Sub2APIUsageCostsByOwner(context.Context, []int64) (map[int64]float64, error) {
	return f.sub2APICostsByOwner, f.sub2APICostErr
}

func TestLoadPlatformPoolSummaryDataRejectsPartialResults(t *testing.T) {
	sentinel := errors.New("query failed")
	tests := []struct {
		name string
		db   fakePlatformPoolSummaryStore
	}{
		{name: "pool", db: fakePlatformPoolSummaryStore{poolErr: sentinel}},
		{name: "usage", db: fakePlatformPoolSummaryStore{usageErr: sentinel}},
		{name: "cost", db: fakePlatformPoolSummaryStore{costErr: sentinel}},
		{name: "sub2api cost", db: fakePlatformPoolSummaryStore{sub2APICostErr: sentinel}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, usage, costs, err := loadPlatformPoolSummaryData(context.Background(), tt.db, []int64{1}, 24, true)
			if !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want sentinel", err)
			}
			if pool != nil || usage != nil || costs != nil {
				t.Fatalf("partial results leaked: pool=%v usage=%v costs=%v", pool, usage, costs)
			}
		})
	}
}

func TestLoadPlatformPoolSummaryDataReturnsCompleteResults(t *testing.T) {
	db := fakePlatformPoolSummaryStore{
		poolByOwner: map[int64]store.OwnerTokenPoolSummary{
			1: {Counts: store.TokenCounts{Total: 7, Available: 2, Cooling: 3, Disabled: 2}},
		},
		usageByOwner: map[int64]store.OwnerUsageSummary{
			1: {OwnerUserID: 1, RequestCount: 9},
		},
		observedCostsByOwner: map[int64]float64{1: 1.25},
		sub2APICostsByOwner:  map[int64]float64{1: 2.5},
	}
	pool, usage, costs, err := loadPlatformPoolSummaryData(context.Background(), db, []int64{1}, 24, true)
	if err != nil {
		t.Fatal(err)
	}
	if pool[1].Counts.Total != 7 || usage[1].RequestCount != 9 || costs[1] != 1.25 {
		t.Fatalf("unexpected results: pool=%v usage=%v costs=%v", pool, usage, costs)
	}
	if usage[1].ObservedCostUSD != 1.25 || usage[1].Sub2APIObservedCostUSD != 2.5 || usage[1].CombinedObservedCostUSD != 3.75 {
		t.Fatalf("unexpected combined usage costs: %#v", usage[1])
	}
}

func TestLoadPlatformPoolSummaryDataCanSkipUsage(t *testing.T) {
	db := fakePlatformPoolSummaryStore{
		poolByOwner: map[int64]store.OwnerTokenPoolSummary{
			1: {Counts: store.TokenCounts{Total: 7, Available: 2, Cooling: 3, Disabled: 2}},
		},
		usageErr: errors.New("usage must not be queried"),
		costErr:  errors.New("costs must not be queried"),
	}
	pool, usage, costs, err := loadPlatformPoolSummaryData(context.Background(), db, []int64{1}, 24, false)
	if err != nil {
		t.Fatal(err)
	}
	if pool[1].Counts.Total != 7 {
		t.Fatalf("pool = %v", pool)
	}
	if usage[1].OwnerUserID != 1 || usage[1].Hours != 24 || costs[1] != 0 {
		t.Fatalf("unexpected skipped usage placeholders: usage=%v costs=%v", usage, costs)
	}
}
