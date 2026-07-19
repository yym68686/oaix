package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresGatewayIdempotencyLifecycle(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL: configURL(dsn), MaxConns: 4, MinConns: 1, ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate returned error: %v", err)
	}

	var ownerUserID int64
	if err := db.pool.QueryRow(ctx, `select id from platform_users where email = 'platform@oaix.local'`).Scan(&ownerUserID); err != nil {
		t.Fatalf("load platform owner: %v", err)
	}
	keyHash := integrationSHA256("gateway-idempotency-key-" + time.Now().UTC().Format(time.RFC3339Nano))
	requestHash := integrationSHA256("gateway-idempotency-request")
	conflictingHash := integrationSHA256("gateway-idempotency-conflict")
	defer db.pool.Exec(context.Background(), `delete from gateway_idempotency_records where owner_user_id = $1 and key_hash = $2`, ownerUserID, keyHash)

	begin := GatewayIdempotencyBegin{
		OwnerUserID: ownerUserID, KeyHash: keyHash, RequestHash: requestHash,
		RequestID: "integration-request-1", LeaseToken: "integration-lease-1",
		LeaseDuration: time.Minute, RecordTTL: time.Hour,
	}
	claimed, err := db.BeginGatewayIdempotency(ctx, begin)
	if err != nil || claimed.Action != GatewayIdempotencyExecute || claimed.Record.Generation != 1 {
		t.Fatalf("initial claim = %+v, %v", claimed, err)
	}
	waiting := begin
	waiting.LeaseToken = "integration-lease-waiting"
	waiting.RequestID = "integration-request-waiting"
	waitResult, err := db.BeginGatewayIdempotency(ctx, waiting)
	if err != nil || waitResult.Action != GatewayIdempotencyWait {
		t.Fatalf("concurrent claim = %+v, %v", waitResult, err)
	}
	conflict := waiting
	conflict.RequestHash = conflictingHash
	conflictResult, err := db.BeginGatewayIdempotency(ctx, conflict)
	if err != nil || conflictResult.Action != GatewayIdempotencyConflict {
		t.Fatalf("conflicting claim = %+v, %v", conflictResult, err)
	}

	headers := json.RawMessage(`{"Content-Type":["application/json"],"X-Test":["stored"]}`)
	body := []byte(`{"id":"stored-response"}`)
	completed, err := db.CompleteGatewayIdempotency(ctx, GatewayIdempotencyCompletion{
		OwnerUserID: ownerUserID, KeyHash: keyHash, LeaseToken: begin.LeaseToken,
		StatusCode: 200, ResponseHeaders: headers, ResponseBody: body, RecordTTL: time.Hour,
	})
	if err != nil || !completed {
		t.Fatalf("complete = %v, %v", completed, err)
	}
	replayed, err := db.BeginGatewayIdempotency(ctx, waiting)
	if err != nil || replayed.Action != GatewayIdempotencyReplay || replayed.Record.StatusCode == nil || *replayed.Record.StatusCode != 200 {
		t.Fatalf("replay = %+v, %v", replayed, err)
	}
	var gotHeaders map[string][]string
	var wantHeaders map[string][]string
	if err := json.Unmarshal(replayed.Record.ResponseHeaders, &gotHeaders); err != nil {
		t.Fatalf("decode replayed headers: %v", err)
	}
	if err := json.Unmarshal(headers, &wantHeaders); err != nil {
		t.Fatalf("decode expected headers: %v", err)
	}
	if !bytes.Equal(replayed.Record.ResponseBody, body) || !reflect.DeepEqual(gotHeaders, wantHeaders) {
		t.Fatalf("stored response changed: headers=%s body=%s", replayed.Record.ResponseHeaders, replayed.Record.ResponseBody)
	}
	if staleCompletion, err := db.CompleteGatewayIdempotency(ctx, GatewayIdempotencyCompletion{
		OwnerUserID: ownerUserID, KeyHash: keyHash, LeaseToken: "stale-lease",
		StatusCode: 201, ResponseHeaders: headers, ResponseBody: body, RecordTTL: time.Hour,
	}); err != nil || staleCompletion {
		t.Fatalf("stale completion = %v, %v", staleCompletion, err)
	}

	if _, err := db.pool.Exec(ctx, `
		update gateway_idempotency_records
		set state = 'in_progress', lease_token = 'expired-lease', lease_expires_at = now() - interval '1 second',
		    response_headers = null, response_body = null, replayable = false
		where owner_user_id = $1 and key_hash = $2
	`, ownerUserID, keyHash); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaimed := begin
	reclaimed.LeaseToken = "integration-lease-2"
	reclaimed.RequestID = "integration-request-2"
	reclaimResult, err := db.BeginGatewayIdempotency(ctx, reclaimed)
	if err != nil || reclaimResult.Action != GatewayIdempotencyExecute || reclaimResult.Record.Generation != 2 {
		t.Fatalf("reclaim = %+v, %v", reclaimResult, err)
	}
	failed, err := db.FailGatewayIdempotency(ctx, ownerUserID, keyHash, reclaimed.LeaseToken, "test_failure", time.Hour)
	if err != nil || !failed {
		t.Fatalf("fail = %v, %v", failed, err)
	}
	retry := reclaimed
	retry.LeaseToken = "integration-lease-3"
	retry.RequestID = "integration-request-3"
	retryResult, err := db.BeginGatewayIdempotency(ctx, retry)
	if err != nil || retryResult.Action != GatewayIdempotencyExecute || retryResult.Record.Generation != 3 {
		t.Fatalf("failed record retry = %+v, %v", retryResult, err)
	}
	renewed, err := db.RenewGatewayIdempotencyLease(ctx, ownerUserID, keyHash, retry.LeaseToken, time.Minute, time.Hour)
	if err != nil || !renewed {
		t.Fatalf("renew = %v, %v", renewed, err)
	}
	if _, err := db.pool.Exec(ctx, `
		update gateway_idempotency_records
		set state = 'failed', lease_token = null, lease_expires_at = null, expires_at = now() - interval '1 second'
		where owner_user_id = $1 and key_hash = $2
	`, ownerUserID, keyHash); err != nil {
		t.Fatalf("expire record: %v", err)
	}
	deleted, err := db.DeleteExpiredGatewayIdempotencyRecords(ctx, 10)
	if err != nil || deleted < 1 {
		t.Fatalf("cleanup = %d, %v", deleted, err)
	}
}

func integrationSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
