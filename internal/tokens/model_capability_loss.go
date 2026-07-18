package tokens

import (
	"context"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type tokenModelCapabilityKey struct {
	tokenID int64
	model   string
}

type tokenModelCapabilityLoss struct {
	ownerUserID        int64
	validUntil         time.Time
	pendingPersistence bool
}

type tokenModelCapabilityLossSource interface {
	ListTokenModelCapabilityLosses(ctx context.Context, scope store.ResourceScope) ([]store.TokenModelCapabilityLoss, error)
}

func (m *Manager) MarkTokenModelCapabilityLoss(tokenID, ownerUserID int64, model string, validUntil time.Time) {
	if m == nil || tokenID <= 0 || validUntil.IsZero() {
		return
	}
	key := tokenModelCapabilityKey{tokenID: tokenID, model: normalizeModelKey(model)}
	if key.model == "" {
		return
	}
	m.modelLossMu.Lock()
	existing := m.modelLosses[key]
	if existing.validUntil.After(validUntil) {
		validUntil = existing.validUntil
	}
	m.modelLosses[key] = tokenModelCapabilityLoss{
		ownerUserID:        ownerUserID,
		validUntil:         validUntil.UTC(),
		pendingPersistence: true,
	}
	m.modelLossMu.Unlock()
}

func (m *Manager) ConfirmTokenModelCapabilityLoss(tokenID int64, model string) {
	if m == nil || tokenID <= 0 {
		return
	}
	key := tokenModelCapabilityKey{tokenID: tokenID, model: normalizeModelKey(model)}
	m.modelLossMu.Lock()
	if loss, ok := m.modelLosses[key]; ok {
		loss.pendingPersistence = false
		m.modelLosses[key] = loss
	}
	m.modelLossMu.Unlock()
}

func (m *Manager) ClearTokenModelCapabilityLoss(tokenID int64, model string) {
	if m == nil || tokenID <= 0 {
		return
	}
	key := tokenModelCapabilityKey{tokenID: tokenID, model: normalizeModelKey(model)}
	m.modelLossMu.Lock()
	delete(m.modelLosses, key)
	m.modelLossMu.Unlock()
}

func (m *Manager) withTokenModelCapabilityLossExclusions(intent Intent) Intent {
	if m == nil {
		return intent
	}
	model := normalizeModelKey(intent.Model)
	if model == "" {
		return intent
	}
	now := time.Now().UTC()
	m.modelLossMu.RLock()
	var blocked []int64
	for key, loss := range m.modelLosses {
		if key.model == model && loss.validUntil.After(now) {
			blocked = append(blocked, key.tokenID)
		}
	}
	m.modelLossMu.RUnlock()
	if len(blocked) == 0 {
		return intent
	}
	intent.ExcludeTokenIDs = mergeExcluded(intent.ExcludeTokenIDs, tokenIDSet(blocked))
	return intent
}

func tokenIDSet(tokenIDs []int64) map[int64]struct{} {
	out := make(map[int64]struct{}, len(tokenIDs))
	for _, tokenID := range tokenIDs {
		if tokenID > 0 {
			out[tokenID] = struct{}{}
		}
	}
	return out
}

func (m *Manager) refreshTokenModelCapabilityLosses(ctx context.Context, scope store.ResourceScope) error {
	source, ok := m.source.(tokenModelCapabilityLossSource)
	if !ok || source == nil {
		return nil
	}
	rows, err := source.ListTokenModelCapabilityLosses(ctx, scope)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	loaded := make(map[tokenModelCapabilityKey]tokenModelCapabilityLoss, len(rows))
	for _, row := range rows {
		key := tokenModelCapabilityKey{tokenID: row.TokenID, model: normalizeModelKey(row.Model)}
		if key.tokenID <= 0 || key.model == "" || !row.ValidUntil.After(now) {
			continue
		}
		loaded[key] = tokenModelCapabilityLoss{
			ownerUserID: row.OwnerUserID,
			validUntil:  row.ValidUntil.UTC(),
		}
	}

	m.modelLossMu.Lock()
	for key, loss := range m.modelLosses {
		if !loss.validUntil.After(now) {
			delete(m.modelLosses, key)
			continue
		}
		if !tokenModelCapabilityLossInScope(loss, scope) || loss.pendingPersistence {
			continue
		}
		delete(m.modelLosses, key)
	}
	for key, loss := range loaded {
		if existing, ok := m.modelLosses[key]; ok && existing.pendingPersistence {
			if loss.validUntil.After(existing.validUntil) {
				existing.validUntil = loss.validUntil
				m.modelLosses[key] = existing
			}
			continue
		}
		m.modelLosses[key] = loss
	}
	m.modelLossMu.Unlock()
	return nil
}

func tokenModelCapabilityLossInScope(loss tokenModelCapabilityLoss, scope store.ResourceScope) bool {
	if scope.AllowAll {
		return true
	}
	return scope.OwnerUserID != nil && *scope.OwnerUserID > 0 && loss.ownerUserID == *scope.OwnerUserID
}
