package proxy

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const settlementReceiptField = "oaix_settlement_receipt"
const settlementReceiptDomain = "oaix-settlement-v1\x00"

func (p *Pipeline) settlementReceiptEnabled(attempt Attempt) bool {
	_, err := hex.DecodeString(attempt.Intent.SettlementNonce)
	return err == nil && len(attempt.Intent.SettlementNonce) == 64 && isMarketplaceIntent(attempt.Intent) && isResponsesStreamEndpoint(attempt.Intent.Endpoint) && attempt.Intent.ImageResponseFormat == "" && len(p.cfg.Auth.ServiceAPIKeys) > 0
}

func (p *Pipeline) writeSettlementJSON(w http.ResponseWriter, resp *http.Response, attempt Attempt) (AttemptResult, error) {
	limit := p.cfg.Upstream.NonStreamMaxResponseBytes
	if limit <= 0 {
		limit = 64 * 1024 * 1024
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	result := AttemptResult{Status: http.StatusBadGateway, Retry: true}
	if err != nil {
		return result, err
	}
	if int64(len(raw)) > limit {
		return result, fmt.Errorf("upstream response exceeds configured body limit")
	}
	var payload map[string]any
	if err = json.Unmarshal(raw, &payload); err != nil {
		return result, err
	}
	p.addSettlementReceipt(payload, attempt)
	raw, err = json.Marshal(payload)
	if err != nil {
		return result, err
	}
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	_, err = w.Write(raw)
	result = AttemptResult{Status: resp.StatusCode, Committed: true}
	result.Usage, result.ResponseID = extractResponseMetrics(raw, attempt.Intent.Model, attempt.Intent.RequireFast, p.GPT6AstraLongContextPricingEnabled())
	return result, err
}

// Only a caller that requests this additive contract receives receipts. The
// receipt is minted from the successful attempt, never from committed headers.
func (p *Pipeline) addSettlementReceipt(response map[string]any, attempt Attempt) {
	usage, ok := response["usage"].(map[string]any)
	if !ok {
		return
	}
	delete(usage, settlementReceiptField) // Never trust an upstream-supplied receipt.
	if len(attempt.Intent.SettlementNonce) != 64 || !isMarketplaceIntent(attempt.Intent) || attempt.Claim == nil || attempt.Claim.Token == nil {
		return
	}
	if _, err := hex.DecodeString(attempt.Intent.SettlementNonce); err != nil {
		return
	}
	if status, _ := response["status"].(string); status != "" && status != "completed" {
		return
	}
	claim := attempt.Claim
	payload := map[string]any{
		"v": 1, "request_id": attempt.RequestID, "nonce": attempt.Intent.SettlementNonce,
		"token_id": claim.TokenID(), "owner_id": claim.Token.Token.OwnerUserID,
		"price_bps": claimMarketplacePriceBPS(claim), "price_source": claimMarketplacePriceSource(claim),
		"price_locked": claim.MarketplacePriceLocked, "contract_key": claim.MarketplacePriceContractKey,
		"issued_at": time.Now().Unix(),
	}
	if claim.MarketplacePriceLockedAt != nil {
		payload["price_locked_at"] = claim.MarketplacePriceLockedAt.UTC().Format(time.RFC3339Nano)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	signatures := make([]string, 0, len(p.cfg.Auth.ServiceAPIKeys))
	for _, key := range p.cfg.Auth.ServiceAPIKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		mac := hmac.New(sha256.New, []byte(key))
		mac.Write([]byte(settlementReceiptDomain + encoded))
		signatures = append(signatures, hex.EncodeToString(mac.Sum(nil)))
	}
	if len(signatures) > 0 {
		usage[settlementReceiptField] = map[string]any{"payload": encoded, "signatures": signatures}
	}
}
