package httpapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/modelaccess"
	"github.com/yym68686/oaix/internal/store"
)

func sub2APILoadUsage(token store.Token, saved *store.QuotaSnapshot) (map[string]any, map[string]any, error) {
	usage := map[string]any{"source": "passive", "five_hour": nil}
	quota := map[string]any{"account_id": stringPtr(token.AccountID), "email": stringPtr(token.Email), "plan_type": stringPtr(token.PlanType), "fetched_at": int64(0)}
	if saved == nil {
		return usage, quota, nil
	}
	var snapshot codexQuotaSnapshot
	if err := json.Unmarshal(saved.Snapshot, &snapshot); err != nil {
		return nil, nil, err
	}
	usage["updated_at"] = saved.FetchedAt
	quota["fetched_at"] = saved.FetchedAt.Unix()
	if snapshot.Error != nil {
		usage["error"] = "OAIX quota snapshot is unavailable"
		usage["error_code"] = "network_error"
		return usage, quota, nil
	}
	rate := map[string]any{}
	if snapshot.Allowed != nil {
		rate["allowed"] = *snapshot.Allowed
	}
	if snapshot.LimitReached != nil {
		rate["limit_reached"] = *snapshot.LimitReached
	}
	now := time.Now().UTC()
	for _, window := range snapshot.Windows {
		if window.UsedPercent == nil || window.LimitWindowSeconds == nil {
			continue
		}
		remaining := int64(0)
		reset := int64(0)
		if window.ResetAt != nil {
			reset = window.ResetAt.Unix()
			remaining = int64(max(0, int(window.ResetAt.Sub(now).Seconds())))
		}
		progress := map[string]any{"utilization": *window.UsedPercent, "resets_at": window.ResetAt, "remaining_seconds": remaining}
		raw := map[string]any{"used_percent": *window.UsedPercent, "limit_window_seconds": *window.LimitWindowSeconds, "reset_after_seconds": remaining, "reset_at": reset}
		switch *window.LimitWindowSeconds {
		case quotaWindow5HSeconds:
			usage["five_hour"] = progress
			rate["primary_window"] = raw
		case quotaWindow7DSeconds:
			usage["seven_day"] = progress
			rate["secondary_window"] = raw
		}
	}
	if len(rate) > 0 {
		quota["rate_limit"] = rate
	}
	if snapshot.RateLimitResetCredits != nil {
		quota["rate_limit_reset_credits"] = snapshot.RateLimitResetCredits
	}
	return usage, quota, nil
}
func (a *App) sub2APIAccountUsage(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	snapshots, err := a.store.LatestTokenQuotaSnapshots(r.Context(), store.OwnerResources(token.OwnerUserID), []int64{token.ID})
	if err != nil {
		sub2APIReply(w, 503, nil, "Quota snapshot unavailable")
		return
	}
	var saved *store.QuotaSnapshot
	if value, ok := snapshots[token.ID]; ok {
		saved = &value
	}
	usage, quota, err := sub2APILoadUsage(*token, saved)
	if err != nil {
		sub2APIReply(w, 503, nil, "Quota snapshot unavailable")
		return
	}
	if strings.HasSuffix(r.URL.Path, "/quota") {
		sub2APIReply(w, 200, quota, "")
	} else {
		sub2APIReply(w, 200, usage, "")
	}
}
func sub2APIUsageDates(days int) (time.Time, time.Time) {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return today.AddDate(0, 0, -days+1), today.AddDate(0, 0, 1)
}
func sub2APIWindowStats(rows []store.TokenUsageBucket) map[string]any {
	var requests, tokens int64
	var cost float64
	for _, row := range rows {
		requests += row.Requests
		tokens += row.TotalTokens
		cost += row.EstimatedCostUSD
	}
	return map[string]any{"requests": requests, "tokens": tokens, "cost": cost, "standard_cost": cost, "user_cost": cost}
}
func sub2APIUsageStats(rows []store.TokenUsageBucket, days int) map[string]any {
	byDay := map[string][]store.TokenUsageBucket{}
	byModel := map[string][]store.TokenUsageBucket{}
	byEndpoint := map[string][]store.TokenUsageBucket{}
	var duration float64
	var durationCount int64
	for _, row := range rows {
		byDay[row.Date] = append(byDay[row.Date], row)
		byModel[row.Model] = append(byModel[row.Model], row)
		byEndpoint[row.Endpoint] = append(byEndpoint[row.Endpoint], row)
		duration += row.DurationMSSum
		durationCount += row.DurationCount
	}
	history := []map[string]any{}
	from, to := sub2APIUsageDates(days)
	today := to.AddDate(0, 0, -1).Format("2006-01-02")
	var topCost, topRequests map[string]any
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		date := day.Format("2006-01-02")
		s := sub2APIWindowStats(byDay[date])
		cost := s["cost"].(float64)
		requests := s["requests"].(int64)
		item := map[string]any{"date": date, "label": day.Format("01-02"), "requests": requests, "tokens": s["tokens"], "cost": cost, "actual_cost": cost, "user_cost": cost}
		history = append(history, item)
		if topCost == nil || cost > topCost["cost"].(float64) {
			topCost = item
		}
		if topRequests == nil || requests > topRequests["requests"].(int64) {
			topRequests = item
		}
	}
	total := sub2APIWindowStats(rows)
	cost := total["cost"].(float64)
	requests := total["requests"].(int64)
	count := total["tokens"].(int64)
	div := float64(max(1, len(byDay)))
	todayStats := sub2APIWindowStats(byDay[today])
	todayStats["date"] = today
	summary := map[string]any{"days": days, "actual_days_used": len(byDay), "total_cost": cost, "total_user_cost": cost, "total_standard_cost": cost, "total_requests": requests, "total_tokens": count, "avg_daily_cost": cost / div, "avg_daily_user_cost": cost / div, "avg_daily_requests": float64(requests) / div, "avg_daily_tokens": float64(count) / div, "avg_duration_ms": duration / float64(max(1, int(durationCount))), "today": todayStats, "highest_cost_day": topCost, "highest_request_day": topRequests}
	models := []map[string]any{}
	names := []string{}
	for name := range byModel {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		rs := byModel[name]
		s := sub2APIWindowStats(rs)
		var in, out, cached, write int64
		for _, v := range rs {
			in += v.InputTokens
			out += v.OutputTokens
			cached += v.CachedTokens
			write += v.CacheWriteTokens
		}
		models = append(models, map[string]any{"model": name, "requests": s["requests"], "input_tokens": in, "output_tokens": out, "cache_creation_tokens": write, "cache_read_tokens": cached, "total_tokens": s["tokens"], "cost": s["cost"], "actual_cost": s["cost"], "account_cost": s["cost"]})
	}
	endpoints := []map[string]any{}
	names = nil
	for name := range byEndpoint {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s := sub2APIWindowStats(byEndpoint[name])
		endpoints = append(endpoints, map[string]any{"endpoint": name, "requests": s["requests"], "total_tokens": s["tokens"], "cost": s["cost"], "actual_cost": s["cost"]})
	}
	return map[string]any{"history": history, "summary": summary, "models": models, "endpoints": endpoints, "upstream_endpoints": []any{}}
}
func (a *App) sub2APIAccountStats(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	days := 30
	if n, err := strconv.Atoi(r.URL.Query().Get("days")); err == nil && n > 0 && n <= 90 {
		days = n
	}
	today := strings.HasSuffix(r.URL.Path, "/today-stats")
	if today {
		days = 1
	}
	from, to := sub2APIUsageDates(days)
	rows, err := a.store.TokenUsageBuckets(r.Context(), store.OwnerResources(token.OwnerUserID), []int64{token.ID}, from, to)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account usage statistics unavailable")
		return
	}
	if today {
		sub2APIReply(w, 200, sub2APIWindowStats(rows), "")
	} else {
		sub2APIReply(w, 200, sub2APIUsageStats(rows, days), "")
	}
}
func (a *App) sub2APIBatchQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountIDs []int64 `json:"account_ids"`
	}
	if !sub2APIDecode(w, r, &req) {
		return
	}
	if req.AccountIDs == nil || len(req.AccountIDs) > 1000 {
		sub2APIReply(w, 400, nil, "account_ids must contain at most 1000 IDs")
		return
	}
	owner, ok := a.sub2APIQueryOwner(w, r)
	if !ok {
		return
	}
	rows, err := a.store.ListTokensByIDsScoped(r.Context(), store.OwnerResources(owner), req.AccountIDs)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account query unavailable")
		return
	}
	values := map[string]any{}
	errs := map[string]string{}
	if strings.Contains(r.URL.Path, "/today-stats/") {
		ids := []int64{}
		for _, t := range rows {
			ids = append(ids, t.ID)
			values[strconv.FormatInt(t.ID, 10)] = sub2APIWindowStats(nil)
		}
		from, to := sub2APIUsageDates(1)
		buckets, err := a.store.TokenUsageBuckets(r.Context(), store.OwnerResources(owner), ids, from, to)
		if err != nil {
			sub2APIReply(w, 503, nil, "Account usage statistics unavailable")
			return
		}
		byID := map[int64][]store.TokenUsageBucket{}
		for _, b := range buckets {
			byID[b.TokenID] = append(byID[b.TokenID], b)
		}
		for id, bs := range byID {
			values[strconv.FormatInt(id, 10)] = sub2APIWindowStats(bs)
		}
		sub2APIReply(w, 200, map[string]any{"stats": values}, "")
		return
	}
	ids := []int64{}
	for _, t := range rows {
		ids = append(ids, t.ID)
	}
	snapshots, err := a.store.LatestTokenQuotaSnapshots(r.Context(), store.OwnerResources(owner), ids)
	if err != nil {
		sub2APIReply(w, 503, nil, "Quota snapshot unavailable")
		return
	}
	for _, t := range rows {
		key := strconv.FormatInt(t.ID, 10)
		var saved *store.QuotaSnapshot
		if v, ok := snapshots[t.ID]; ok {
			saved = &v
		}
		usage, _, err := sub2APILoadUsage(t, saved)
		if err != nil {
			errs[key] = "Quota snapshot unavailable"
		} else {
			values[key] = usage
		}
	}
	for _, id := range req.AccountIDs {
		if id > 0 {
			key := strconv.FormatInt(id, 10)
			if _, ok := values[key]; !ok && errs[key] == "" {
				errs[key] = "Account not found"
			}
		}
	}
	sub2APIReply(w, 200, map[string]any{"usage": values, "errors": errs}, "")
}
func (a *App) sub2APIAccountModels(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	global, err := a.store.GetTokenModelAccess(r.Context())
	if err != nil {
		sub2APIReply(w, 503, nil, "Model policy unavailable")
		return
	}
	user, err := a.store.GetUserTokenModelAccess(r.Context(), token.OwnerUserID)
	if err != nil {
		sub2APIReply(w, 503, nil, "Model policy unavailable")
		return
	}
	plan := store.CanonicalTokenPlan(stringPtr(token.PlanType))
	allowed, configured := user.PlanModels[plan]
	if !configured {
		allowed, configured = global.PlanModels[plan]
	}
	ids := advertisedModelIDs()
	if a.tokens != nil {
		if known, ok := a.tokens.ModelsForOwnerPlan(token.OwnerUserID, plan, r.URL.Query().Get("client_version"), time.Now().UTC()); ok {
			ids = known
		}
	}
	items := []map[string]any{}
	for _, id := range ids {
		if configured && !modelaccess.Matches(allowed, id) {
			continue
		}
		if !configured && !modelaccess.DefaultAllows(plan, id) {
			continue
		}
		items = append(items, map[string]any{"id": id, "object": "model", "type": "model", "display_name": id})
	}
	sub2APIReply(w, 200, items, "")
}
