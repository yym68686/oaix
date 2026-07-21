package agentidentitytask

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/store"
)

const recoveryTimeout = 30 * time.Second

type CredentialStore interface {
	GetAgentIdentityCredentials(ctx context.Context, tokenID int64) (agentidentity.Credentials, error)
	UpdateAgentIdentityTask(ctx context.Context, tokenID int64, expectedTaskID string, taskID string) error
}

type SnapshotRefresher interface {
	Refresh(ctx context.Context) error
	RefreshOwner(ctx context.Context, ownerUserID int64) error
}

type Coordinator struct {
	store     CredentialStore
	doer      agentidentity.RequestDoer
	authURL   string
	refresher SnapshotRefresher
	logger    *slog.Logger
	locks     sync.Map
}

type Result struct {
	Credentials agentidentity.Credentials
	Registered  bool
}

func New(store CredentialStore, doer agentidentity.RequestDoer, authURL string, refresher SnapshotRefresher, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		store:     store,
		doer:      doer,
		authURL:   authURL,
		refresher: refresher,
		logger:    logger,
	}
}

// Recover returns the latest credentials for a token. Registrations are
// serialized per token across every caller sharing this coordinator, and the
// credentials are re-read while holding the lock so stale request snapshots
// cannot cause sequential duplicate registrations.
func (c *Coordinator) Recover(parent context.Context, tokenID, ownerUserID int64, expectedTaskID string) (Result, error) {
	if c == nil || tokenID <= 0 {
		return Result{}, errors.New("agent identity token is unavailable")
	}
	if c.store == nil {
		return Result{}, errors.New("agent identity credential store is unavailable")
	}
	lockValue, _ := c.locks.LoadOrStore(tokenID, &sync.Mutex{})
	lock, ok := lockValue.(*sync.Mutex)
	if !ok {
		return Result{}, errors.New("agent identity task lock is invalid")
	}
	lock.Lock()
	defer lock.Unlock()

	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, recoveryTimeout)
	defer cancel()

	credentials, err := c.store.GetAgentIdentityCredentials(ctx, tokenID)
	if err != nil {
		return Result{}, fmt.Errorf("load agent identity credentials: %w", err)
	}
	if err := credentials.Validate(); err != nil {
		return Result{}, err
	}
	currentTaskID := strings.TrimSpace(credentials.TaskID)
	expectedTaskID = strings.TrimSpace(expectedTaskID)
	if currentTaskID != "" && (expectedTaskID == "" || currentTaskID != expectedTaskID) {
		return Result{Credentials: credentials}, nil
	}

	newTaskID, err := agentidentity.RegisterTask(ctx, c.doer, c.authURL, credentials)
	if err != nil {
		return Result{}, err
	}
	registered := true
	if err := c.store.UpdateAgentIdentityTask(ctx, tokenID, currentTaskID, newTaskID); err != nil {
		if !errors.Is(err, store.ErrAgentIdentityTaskChanged) {
			return Result{}, fmt.Errorf("persist agent identity task: %w", err)
		}
		registered = false
		credentials, err = c.store.GetAgentIdentityCredentials(ctx, tokenID)
		if err != nil {
			return Result{}, fmt.Errorf("reload agent identity credentials: %w", err)
		}
		if err := credentials.Validate(); err != nil {
			return Result{}, err
		}
		if strings.TrimSpace(credentials.TaskID) == "" {
			return Result{}, errors.New("concurrent agent identity task update returned an empty task")
		}
	} else {
		credentials.TaskID = newTaskID
	}

	if registered {
		c.refreshSnapshots(ctx, ownerUserID)
		if c.logger != nil {
			c.logger.Info("agent identity task registered", "token_id", tokenID, "owner_user_id", ownerUserID)
		}
	}
	return Result{Credentials: credentials, Registered: registered}, nil
}

func (c *Coordinator) refreshSnapshots(ctx context.Context, ownerUserID int64) {
	if c.refresher == nil {
		return
	}
	if err := c.refresher.Refresh(ctx); err != nil && c.logger != nil {
		c.logger.Warn("token snapshot refresh after agent identity task registration failed", "owner_user_id", ownerUserID, "error", err)
	}
	if ownerUserID > 0 {
		if err := c.refresher.RefreshOwner(ctx, ownerUserID); err != nil && c.logger != nil {
			c.logger.Warn("owner token snapshot refresh after agent identity task registration failed", "owner_user_id", ownerUserID, "error", err)
		}
	}
}
