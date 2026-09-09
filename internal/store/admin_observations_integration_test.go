package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/admindiag"
	"github.com/yym68686/oaix/internal/observability"
)

func TestAdminObservationMigrationPolicyAndRetention(t *testing.T) {
	db, ctx, _, _ := performanceFixture(t)
	p, err := db.GetAdminPolicy(ctx)
	if err != nil || p.Enabled {
		t.Fatal(p, err)
	}
	p, err = db.UpdateAdminPolicy(ctx, admindiag.Policy{Enabled: true, SuccessSamplePercent: 1, MaxRecordsPerMinute: 60})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.pool.Exec(ctx, `drop table gateway_admin_observations; update schema_migrations set version=31 where name='oaix_go'`); err != nil {
		t.Fatal(err)
	}
	lock, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err = lock.Exec(ctx, `lock table gateway_request_logs,codex_tokens,proxy_channels,token_proxy_bindings in access exclusive mode`); err != nil {
		t.Fatal(err)
	}
	if err = db.MigrateForStartup(ctx); err != nil {
		t.Fatal("migration blocks serving", err)
	}
	lock.Rollback(ctx)
	got, err := db.GetAdminPolicy(ctx)
	if err != nil || !got.UpdatedAt.Equal(p.UpdatedAt) {
		t.Fatal(got, err)
	}
	if err = db.MigrateForStartup(ctx); err != nil {
		t.Fatal(err)
	}
	d := admindiag.Record{ID: strings.Repeat("a", 32), RequestID: "test", StartedAt: time.Now()}
	raw, _ := json.Marshal(d)
	for i := 0; i < 2; i++ {
		if err = db.SaveAdminObservation(ctx, d, raw); err != nil {
			t.Fatal(err)
		}
	}
	items, err := db.ListAdminObservations(ctx, "test")
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	if _, err = db.pool.Exec(ctx, `update gateway_admin_observations set created_at=now()-interval '25 hours'`); err != nil {
		t.Fatal(err)
	}
	if err = db.CleanupAdminObservations(ctx); err != nil {
		t.Fatal(err)
	}
	items, err = db.ListAdminObservations(ctx, "test")
	if err != nil || len(items) != 0 {
		t.Fatal(items, err)
	}
	if _, err = db.diagnosticReader.Exec(ctx, `create table should_not_exist(id int)`); err == nil {
		t.Fatal("sampler is not read only")
	}
}
func TestAdminObservationSamplesSleepAndLockAndCancellation(t *testing.T) {
	db, ctx, _, _ := performanceFixture(t)
	r := db.AdminRecorder()
	r.SetPolicy(admindiag.Policy{Enabled: true, MaxRecordsPerMinute: 60, SuccessSamplePercent: 100})
	deliveries := make(chan admindiag.Record, 4)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(runCtx, func(_ context.Context, d admindiag.Record, _ []byte) error { deliveries <- d; return nil }, nil)
	}()
	defer func() { stop(); <-done }()
	for _, kind := range []string{"sleep", "lock"} {
		t.Run(kind, func(t *testing.T) {
			var release func()
			sql := `select pg_sleep(5)`
			if kind == "lock" {
				tx, err := db.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(ctx, `lock table platform_users in access exclusive mode`); err != nil {
					t.Fatal(err)
				}
				release = func() { tx.Rollback(ctx) }
				defer release()
				sql = `select id from platform_users limit 1`
			}
			c, trace := r.Begin(observability.ContextWithRequestID(ctx, "observe-"+kind), "/api/admin/users", "page", "", "")
			c, cancel := context.WithCancel(c)
			defer cancel()
			c, end := admindiag.Stage(c, "fixture")
			queryDone := make(chan error, 1)
			go func() {
				rows, err := db.pool.Query(c, sql)
				if err == nil {
					for rows.Next() {
					}
					err = rows.Err()
					rows.Close()
				}
				queryDone <- err
			}()
			deadline := time.Now().Add(3 * time.Second)
			for len(r.Targets()) == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if len(r.Targets()) == 0 {
				t.Fatal("query not registered")
			}
			db.SampleAdminQueries(ctx)
			cancel()
			err := <-queryDone
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("query cancellation changed: %v", err)
			}
			end(err)
			r.Finish(trace, 503)
			select {
			case d := <-deliveries:
				if len(d.Samples) == 0 {
					t.Fatalf("no sample: %+v", d)
				}
				s := d.Samples[0]
				if kind == "lock" && (s.WaitType != "Lock" || len(s.Blockers) == 0) {
					t.Fatal(s)
				}
				if kind == "sleep" && s.Wait != "PgSleep" {
					t.Fatal(s)
				}
				if s.BackendStart.IsZero() || s.QueryStart.IsZero() {
					t.Fatal("missing backend identity")
				}
				raw, _ := json.Marshal(d)
				if strings.Contains(string(raw), sql) {
					t.Fatal("raw SQL leaked")
				}
			case <-time.After(time.Second):
				t.Fatal("record not delivered")
			}
		})
	}
}
func TestAdminObservationTracksAcquireWhenBusinessPoolFull(t *testing.T) {
	db, ctx, _, _ := performanceFixture(t)
	r := db.AdminRecorder()
	r.SetPolicy(admindiag.Policy{Enabled: true, MaxRecordsPerMinute: 60, SuccessSamplePercent: 100})
	// Reserve every business connection; the diagnostic reader remains separate.
	held := make([]func(), 0, 8)
	for i := 0; i < 8; i++ {
		c, err := db.pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c.Release)
	}
	defer func() {
		for _, release := range held {
			release()
		}
	}()
	c, d := r.Begin(ctx, "/api/admin/users", "", "", "")
	c, cancel := context.WithTimeout(c, 100*time.Millisecond)
	defer cancel()
	_, err := db.pool.Exec(c, `select 1`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	r.Finish(d, 503)
	delivered := make(chan admindiag.Record, 1)
	run, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(run, func(_ context.Context, d admindiag.Record, _ []byte) error { delivered <- d; return nil }, nil)
	}()
	select {
	case got := <-delivered:
		if len(got.Events) != 1 || got.Events[0].Kind != "acquire" || got.Events[0].Error != "deadline" {
			t.Fatal(got.Events)
		}
	case <-time.After(time.Second):
		t.Fatal("missing acquire event")
	}
	stop()
	<-done
}
