package tokens

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"
)

const unknownPlanKey = "<unknown>"

type FastCapabilityResolver func(ctx context.Context, ownerUserID int64) error

type planCapabilityKey struct {
	ownerUserID int64
	plan        string
}

type planModelCapability struct {
	clientVersion string
	fastModels    map[string]struct{}
	validUntil    time.Time
}

// SetPlanModelCapabilities records the last successful official catalog for a
// plan. Capabilities stay owner-scoped so one workspace's policy cannot grant
// Fast routing to another workspace that happens to use the same plan name.
func (m *Manager) SetPlanModelCapabilities(ownerUserID int64, plan, clientVersion string, fastModels []string, validUntil time.Time) {
	if m == nil || ownerUserID <= 0 {
		return
	}
	key := planCapabilityKey{
		ownerUserID: ownerUserID,
		plan:        normalizePlanKey(plan),
	}
	models := make(map[string]struct{}, len(fastModels))
	for _, model := range fastModels {
		if normalized := normalizeModelKey(model); normalized != "" {
			models[normalized] = struct{}{}
		}
	}
	m.capabilityMu.Lock()
	now := time.Now().UTC()
	if m.capabilityWrites.Add(1)%128 == 0 {
		for existingKey, capability := range m.capabilities {
			if capabilityExpired(capability, now) {
				delete(m.capabilities, existingKey)
			}
		}
	}
	if existing, ok := m.capabilities[key]; ok && !capabilityExpired(existing, now) && compareClientVersions(clientVersion, existing.clientVersion) < 0 {
		m.capabilityMu.Unlock()
		return
	}
	m.capabilities[key] = planModelCapability{
		clientVersion: strings.TrimSpace(clientVersion),
		fastModels:    models,
		validUntil:    validUntil,
	}
	m.capabilityMu.Unlock()
}

// SetFastCapabilityResolver installs the on-demand catalog probe used when a
// Fast request arrives before this process has observed any plan capability.
func (m *Manager) SetFastCapabilityResolver(resolver FastCapabilityResolver) {
	if m == nil {
		return
	}
	m.capabilityMu.Lock()
	m.fastResolver = resolver
	m.capabilityMu.Unlock()
}

// AvailablePlansForOwner returns the normalized plans represented by the
// owner's current ready-token snapshot.
func (m *Manager) AvailablePlansForOwner(ownerUserID int64) []string {
	if m == nil || ownerUserID <= 0 {
		return nil
	}
	snapshot := m.SnapshotForOwner(ownerUserID)
	seen := make(map[string]struct{})
	for _, token := range snapshot.Ready {
		if token == nil {
			continue
		}
		seen[planKeyForToken(token)] = struct{}{}
	}
	plans := make([]string, 0, len(seen))
	for plan := range seen {
		plans = append(plans, plan)
	}
	sort.Strings(plans)
	return plans
}

// FastEligibleTokenIDs returns the hard allow-list for a Fast request. Unknown,
// expired, and explicitly non-Fast plan catalogs are all excluded.
func (m *Manager) FastEligibleTokenIDs(ctx context.Context, intent Intent, model string, now time.Time) (map[int64]struct{}, error) {
	eligible := make(map[int64]struct{})
	if m == nil || normalizeModelKey(model) == "" {
		return eligible, nil
	}
	snapshot, _, err := m.snapshotForClaim(ctx, intent)
	if err != nil {
		return eligible, err
	}
	m.capabilityMu.RLock()
	known := m.fastCapabilitiesKnownLocked(snapshot, now)
	for _, token := range snapshot.Ready {
		if token == nil || !m.tokenSupportsFastLocked(token, model, now) {
			continue
		}
		eligible[token.Token.ID] = struct{}{}
	}
	resolver := m.fastResolver
	m.capabilityMu.RUnlock()
	if len(eligible) > 0 || known || resolver == nil || intent.OwnerUserID <= 0 {
		return eligible, nil
	}
	if err := resolver(ctx, intent.OwnerUserID); err != nil {
		return eligible, err
	}
	m.capabilityMu.RLock()
	for _, token := range snapshot.Ready {
		if token == nil || !m.tokenSupportsFastLocked(token, model, now) {
			continue
		}
		eligible[token.Token.ID] = struct{}{}
	}
	m.capabilityMu.RUnlock()
	return eligible, nil
}

func (m *Manager) fastCapabilitiesKnownLocked(snapshot *Snapshot, now time.Time) bool {
	if snapshot == nil || len(snapshot.Ready) == 0 {
		return false
	}
	for _, token := range snapshot.Ready {
		if token == nil {
			continue
		}
		capability, ok := m.capabilities[planCapabilityKey{
			ownerUserID: token.Token.OwnerUserID,
			plan:        planKeyForToken(token),
		}]
		if !ok || capabilityExpired(capability, now) {
			return false
		}
	}
	return true
}

// FastModelsForOwner returns the union of Fast-capable models for plans that
// still have at least one ready token in the owner's pool.
func (m *Manager) FastModelsForOwner(ownerUserID int64, clientVersion string, now time.Time) map[string]struct{} {
	models := make(map[string]struct{})
	if m == nil || ownerUserID <= 0 {
		return models
	}
	snapshot := m.SnapshotForOwner(ownerUserID)
	readyPlans := make(map[string]struct{})
	for _, token := range snapshot.Ready {
		if token != nil {
			readyPlans[planKeyForToken(token)] = struct{}{}
		}
	}
	m.capabilityMu.RLock()
	defer m.capabilityMu.RUnlock()
	clientVersion = strings.TrimSpace(clientVersion)
	for key, capability := range m.capabilities {
		if key.ownerUserID != ownerUserID {
			continue
		}
		if clientVersion != "" && capability.clientVersion != clientVersion {
			continue
		}
		if _, ok := readyPlans[key.plan]; !ok || capabilityExpired(capability, now) {
			continue
		}
		for model := range capability.fastModels {
			models[model] = struct{}{}
		}
	}
	return models
}

func (m *Manager) tokenSupportsFastLocked(token *RuntimeToken, model string, now time.Time) bool {
	capability, ok := m.capabilities[planCapabilityKey{
		ownerUserID: token.Token.OwnerUserID,
		plan:        planKeyForToken(token),
	}]
	return ok && !capabilityExpired(capability, now) && fastModelSetMatches(capability.fastModels, model)
}

func capabilityExpired(capability planModelCapability, now time.Time) bool {
	return !capability.validUntil.IsZero() && !now.Before(capability.validUntil)
}

func fastModelSetMatches(models map[string]struct{}, requested string) bool {
	requested = normalizeModelKey(requested)
	for model := range models {
		if requested == model || strings.HasPrefix(requested, model+"-") {
			return true
		}
	}
	return false
}

func planKeyForToken(token *RuntimeToken) string {
	if token == nil || token.Token.PlanType == nil {
		return unknownPlanKey
	}
	return normalizePlanKey(*token.Token.PlanType)
}

func normalizePlanKey(plan string) string {
	plan = strings.ToLower(strings.TrimSpace(plan))
	if plan == "" {
		return unknownPlanKey
	}
	return plan
}

func normalizeModelKey(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

func compareClientVersions(left, right string) int {
	leftParts, leftOK := numericVersionParts(left)
	rightParts, rightOK := numericVersionParts(right)
	if leftOK != rightOK {
		if leftOK {
			return 1
		}
		return -1
	}
	if leftOK && rightOK {
		length := len(leftParts)
		if len(rightParts) > length {
			length = len(rightParts)
		}
		for index := 0; index < length; index++ {
			var leftPart, rightPart int64
			if index < len(leftParts) {
				leftPart = leftParts[index]
			}
			if index < len(rightParts) {
				rightPart = rightParts[index]
			}
			if leftPart < rightPart {
				return -1
			}
			if leftPart > rightPart {
				return 1
			}
		}
		return 0
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
}

func numericVersionParts(version string) ([]int64, bool) {
	version = strings.TrimSpace(strings.TrimPrefix(version, "v"))
	if version == "" {
		return nil, false
	}
	parts := strings.Split(version, ".")
	result := make([]int64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseInt(part, 10, 64)
		if err != nil || value < 0 {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}
