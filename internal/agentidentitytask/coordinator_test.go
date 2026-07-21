package agentidentitytask

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/store"
)

func TestCoordinatorSerializesTaskRecoveryAcrossCallers(t *testing.T) {
	credentials := coordinatorTestCredentials(t, "task-old")
	credentialStore := &coordinatorTestStore{credentials: credentials}
	var registrations atomic.Int32
	doer := coordinatorDoerFunc(func(context.Context, *http.Request) (*http.Response, error) {
		registrations.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"task_id":"task-new"}`)),
			Header:     make(http.Header),
		}, nil
	})
	refresher := &coordinatorTestRefresher{}
	coordinator := New(credentialStore, doer, "https://auth.example.test", refresher, nil)

	const callers = 16
	start := make(chan struct{})
	results := make(chan Result, callers)
	errors := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := coordinator.Recover(t.Context(), 42, 7, "task-old")
			if err != nil {
				errors <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Fatal(err)
	}
	for result := range results {
		if result.Credentials.TaskID != "task-new" {
			t.Fatalf("task id = %q", result.Credentials.TaskID)
		}
	}
	if got := registrations.Load(); got != 1 {
		t.Fatalf("registrations = %d, want 1", got)
	}
	if credentialStore.updateCalls != 1 {
		t.Fatalf("task updates = %d, want 1", credentialStore.updateCalls)
	}
	if refresher.global.Load() != 1 || refresher.owner.Load() != 1 {
		t.Fatalf("snapshot refreshes global=%d owner=%d", refresher.global.Load(), refresher.owner.Load())
	}
}

type coordinatorDoerFunc func(context.Context, *http.Request) (*http.Response, error)

func (f coordinatorDoerFunc) Do(ctx context.Context, request *http.Request) (*http.Response, error) {
	return f(ctx, request)
}

type coordinatorTestStore struct {
	mu          sync.Mutex
	credentials agentidentity.Credentials
	updateCalls int
}

func (s *coordinatorTestStore) GetAgentIdentityCredentials(context.Context, int64) (agentidentity.Credentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.credentials, nil
}

func (s *coordinatorTestStore) UpdateAgentIdentityTask(_ context.Context, _ int64, expectedTaskID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.credentials.TaskID) != strings.TrimSpace(expectedTaskID) {
		return store.ErrAgentIdentityTaskChanged
	}
	s.credentials.TaskID = strings.TrimSpace(taskID)
	s.updateCalls++
	return nil
}

type coordinatorTestRefresher struct {
	global atomic.Int32
	owner  atomic.Int32
}

func (r *coordinatorTestRefresher) Refresh(context.Context) error {
	r.global.Add(1)
	return nil
}

func (r *coordinatorTestRefresher) RefreshOwner(context.Context, int64) error {
	r.owner.Add(1)
	return nil
}

func coordinatorTestCredentials(t *testing.T, taskID string) agentidentity.Credentials {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return agentidentity.Credentials{
		RuntimeID:  "runtime-coordinator",
		PrivateKey: base64.StdEncoding.EncodeToString(der),
		TaskID:     taskID,
		AccountID:  "workspace-coordinator",
		UserID:     "user-coordinator",
	}
}
