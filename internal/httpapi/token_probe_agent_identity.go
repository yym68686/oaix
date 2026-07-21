package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/store"
)

const (
	probeCredentialModeOAuth         = "oauth"
	probeCredentialModeAgentIdentity = "agent_identity"

	probeStageLocalPreflight        = "local_preflight"
	probeStageCredentialPreparation = "credential_preparation"
	probeStageUpstreamTransport     = "upstream_transport"
	probeStageUpstreamResponse      = "upstream_response"
	probeStageStatePersistence      = "state_persistence"
)

type agentIdentityProbeCredentialStore interface {
	GetAgentIdentityCredentials(ctx context.Context, tokenID int64) (agentidentity.Credentials, error)
	UpdateAgentIdentityTask(ctx context.Context, tokenID int64, expectedTaskID string, taskID string) error
}

type tokenProbeAuthorization struct {
	Value         string
	AccountID     string
	FedRAMP       bool
	AgentIdentity *agentidentity.Credentials
}

type fallbackTokenProbeDoer struct {
	client *http.Client
}

func (d fallbackTokenProbeDoer) Do(ctx context.Context, request *http.Request) (*http.Response, error) {
	client := d.client
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(request.WithContext(ctx))
}

var defaultTokenProbeDoer agentidentity.RequestDoer = fallbackTokenProbeDoer{
	client: &http.Client{Timeout: probeRequestTimeout},
}

func tokenProbeCredentialMode(token store.Token) string {
	if token.IsAgentIdentity() {
		return probeCredentialModeAgentIdentity
	}
	return probeCredentialModeOAuth
}

func (a *App) tokenProbeRequestDoer() agentidentity.RequestDoer {
	if a != nil && a.probeDoer != nil {
		return a.probeDoer
	}
	return defaultTokenProbeDoer
}

func (a *App) agentIdentityProbeCredentialStore() agentIdentityProbeCredentialStore {
	if a != nil && a.probeIdentityStore != nil {
		return a.probeIdentityStore
	}
	if a != nil && a.store != nil {
		return a.store
	}
	return nil
}

func (a *App) prepareTokenProbeAuthorization(parent context.Context, token store.Token) (tokenProbeAuthorization, tokenProbeAttempt, bool) {
	if !token.IsAgentIdentity() {
		accessToken := strings.TrimSpace(token.AccessToken)
		if accessToken == "" {
			return tokenProbeAuthorization{}, localTokenProbeFailure(
				http.StatusBadRequest,
				probeStageCredentialPreparation,
				"missing_access_token",
				"missing access token",
			), false
		}
		authorization := tokenProbeAuthorization{Value: "Bearer " + accessToken}
		if token.AccountID != nil {
			authorization.AccountID = strings.TrimSpace(*token.AccountID)
		}
		return authorization, tokenProbeAttempt{}, true
	}

	credentials, err := a.loadAgentIdentityProbeCredentials(parent, token)
	if err != nil {
		if a != nil && a.logger != nil {
			a.logger.Warn("manual token probe agent identity credential load failed", "token_id", token.ID, "error", err)
		}
		return tokenProbeAuthorization{}, localTokenProbeFailure(
			http.StatusServiceUnavailable,
			probeStageCredentialPreparation,
			"agent_identity_credentials_unavailable",
			"agent identity credentials are unavailable",
		), false
	}
	if strings.TrimSpace(credentials.TaskID) == "" {
		credentials, err = a.recoverAgentIdentityProbeTask(parent, token, "")
		if err != nil {
			if a != nil && a.logger != nil {
				a.logger.Warn("manual token probe agent identity task registration failed", "token_id", token.ID, "error", err)
			}
			return tokenProbeAuthorization{}, localTokenProbeFailure(
				http.StatusBadGateway,
				probeStageCredentialPreparation,
				"agent_identity_task_registration_failed",
				"agent identity task registration failed",
			), false
		}
	}
	assertion, err := credentials.BuildAssertion(time.Now())
	if err != nil {
		if a != nil && a.logger != nil {
			a.logger.Warn("manual token probe agent identity assertion failed", "token_id", token.ID, "error", err)
		}
		return tokenProbeAuthorization{}, localTokenProbeFailure(
			http.StatusBadGateway,
			probeStageCredentialPreparation,
			"agent_identity_assertion_failed",
			"agent identity assertion could not be created",
		), false
	}
	credentialsCopy := credentials
	return tokenProbeAuthorization{
		Value:         assertion,
		AccountID:     strings.TrimSpace(credentials.AccountID),
		FedRAMP:       credentials.FedRAMP,
		AgentIdentity: &credentialsCopy,
	}, tokenProbeAttempt{}, true
}

func (a *App) loadAgentIdentityProbeCredentials(parent context.Context, token store.Token) (agentidentity.Credentials, error) {
	if token.AgentIdentity != nil {
		credentials := *token.AgentIdentity
		if err := credentials.Validate(); err != nil {
			return agentidentity.Credentials{}, err
		}
		return credentials, nil
	}
	credentialStore := a.agentIdentityProbeCredentialStore()
	if credentialStore == nil {
		return agentidentity.Credentials{}, errors.New("agent identity credential store is unavailable")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	credentials, err := credentialStore.GetAgentIdentityCredentials(ctx, token.ID)
	if err != nil {
		return agentidentity.Credentials{}, fmt.Errorf("load agent identity credentials: %w", err)
	}
	if err := credentials.Validate(); err != nil {
		return agentidentity.Credentials{}, err
	}
	return credentials, nil
}

func (a *App) recoverAgentIdentityProbeTask(parent context.Context, token store.Token, expectedTaskID string) (agentidentity.Credentials, error) {
	credentialStore := a.agentIdentityProbeCredentialStore()
	if credentialStore == nil {
		return agentidentity.Credentials{}, errors.New("agent identity credential store is unavailable")
	}
	key := "manual-probe-agent-task:" + strconv.FormatInt(token.ID, 10)
	value, err, _ := a.probeIdentityRefresh.Do(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(parent, 30*time.Second)
		defer cancel()
		credentials, loadErr := credentialStore.GetAgentIdentityCredentials(ctx, token.ID)
		if loadErr != nil {
			return agentidentity.Credentials{}, fmt.Errorf("load agent identity credentials: %w", loadErr)
		}
		if validateErr := credentials.Validate(); validateErr != nil {
			return agentidentity.Credentials{}, validateErr
		}
		currentTaskID := strings.TrimSpace(credentials.TaskID)
		expectedTaskID = strings.TrimSpace(expectedTaskID)
		if currentTaskID != "" && (expectedTaskID == "" || currentTaskID != expectedTaskID) {
			return credentials, nil
		}
		newTaskID, registerErr := agentidentity.RegisterTask(
			ctx,
			a.tokenProbeRequestDoer(),
			a.cfg.Upstream.AgentIdentityAuthAPIURL,
			credentials,
		)
		if registerErr != nil {
			return agentidentity.Credentials{}, registerErr
		}
		if updateErr := credentialStore.UpdateAgentIdentityTask(ctx, token.ID, currentTaskID, newTaskID); updateErr != nil {
			if !errors.Is(updateErr, store.ErrAgentIdentityTaskChanged) {
				return agentidentity.Credentials{}, fmt.Errorf("persist agent identity task: %w", updateErr)
			}
			credentials, loadErr = credentialStore.GetAgentIdentityCredentials(ctx, token.ID)
			if loadErr != nil {
				return agentidentity.Credentials{}, fmt.Errorf("reload agent identity credentials: %w", loadErr)
			}
		} else {
			credentials.TaskID = newTaskID
		}
		if a.tokens != nil {
			if refreshErr := a.tokens.Refresh(ctx); refreshErr != nil && a.logger != nil {
				a.logger.Warn("token snapshot refresh after manual probe agent identity task registration failed", "token_id", token.ID, "error", refreshErr)
			}
		}
		if a.logger != nil {
			a.logger.Info("manual token probe agent identity task registered", "token_id", token.ID, "owner_user_id", token.OwnerUserID)
		}
		return credentials, nil
	})
	if err != nil {
		return agentidentity.Credentials{}, err
	}
	credentials, ok := value.(agentidentity.Credentials)
	if !ok {
		return agentidentity.Credentials{}, errors.New("agent identity task recovery returned an invalid result")
	}
	return credentials, nil
}

func redactAgentIdentityProbeAttempt(attempt tokenProbeAttempt, credentials *agentidentity.Credentials) tokenProbeAttempt {
	if credentials == nil {
		return attempt
	}
	attempt.RawResponse = string(agentidentity.RedactSensitiveBody([]byte(attempt.RawResponse), *credentials))
	attempt.Detail = string(agentidentity.RedactSensitiveBody([]byte(attempt.Detail), *credentials))
	attempt.ErrorCode = string(agentidentity.RedactSensitiveBody([]byte(attempt.ErrorCode), *credentials))
	return attempt
}
