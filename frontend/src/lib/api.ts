const KEY_STORAGE = "oaix.serviceApiKey";
const LEGACY_KEY_STORAGE = "oaix.serviceKey";

export type TokenCounts = {
  total?: number;
  available?: number;
  active?: number;
  cooling?: number;
  disabled?: number;
};

export type TokenPlanCount = {
  plan: string;
  label?: string;
  count: number;
};

export type TokenErrorCount = {
  error_type: string;
  count: number;
};

export type TokenQuotaWindow = {
  id: string;
  label: string;
  limit_window_seconds?: number | null;
  used_percent?: number | null;
  remaining_percent?: number | null;
  reset_at?: string | null;
  exhausted?: boolean;
};

export type TokenQuotaResetCredits = {
  available_count?: number | null;
};

export type TokenQuotaSnapshot = {
  fetched_at?: string | null;
  error?: string | null;
  plan_type?: string | null;
  disabled?: boolean;
  windows?: TokenQuotaWindow[];
  rate_limit_reset_credits?: TokenQuotaResetCredits | null;
};

export type TokenQuotaResetCreditsResponse = {
  quota?: TokenQuotaSnapshot | null;
  rate_limit_reset_credits?: TokenQuotaResetCredits | null;
};

export type TokenQuotaResetCredit = {
  id?: string;
  reset_type?: string;
  status?: string;
  granted_at?: string;
  expires_at?: string;
  redeem_started_at?: string;
  redeemed_at?: string;
};

export type TokenQuotaResetResult = {
  code?: string;
  credit?: TokenQuotaResetCredit | null;
  windows_reset?: number;
};

export type TokenItem = {
  id: number;
  owner_user_id?: number | null;
  email?: string | null;
  account_id?: string | null;
  plan_type?: string | null;
  remark?: string | null;
  source_file?: string | null;
  is_active: boolean;
  cooldown_until?: string | null;
  reset_at?: string | null;
  disabled_at?: string | null;
  last_used_at?: string | null;
  last_error?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  status?: string | null;
  active_streams?: number | null;
  active_stream_cap?: number | null;
  observed_cost_usd?: number | null;
  quota?: TokenQuotaSnapshot | null;
};

export type TokenObservedCostItem = {
  id: number;
  observed_cost_usd?: number | null;
};

export type TokenProbeResponse = {
  id: number;
  outcome?: "reactivated" | "cooling" | "disabled" | "inconclusive" | string;
  status_code?: number | null;
  message?: string | null;
  detail?: string | null;
  raw_response?: string | null;
  cooldown_seconds?: number | null;
  probe_model?: string | null;
  probe_input?: string | null;
  response_model?: string | null;
};

export type Pagination = {
  total: number;
  limit: number;
  offset: number;
  returned: number;
  has_next?: boolean;
  has_previous?: boolean;
};

export type TokenListResponse = {
  counts?: TokenCounts;
  filtered_counts?: TokenCounts;
  plan_counts?: TokenPlanCount[];
  items?: TokenItem[];
  pagination?: Pagination;
};

export type ImportBatch = {
  id: number;
  owner_user_id?: number | null;
  status: string;
  import_queue_position?: string | null;
  submitted_at?: string | null;
  started_at?: string | null;
  heartbeat_at?: string | null;
  finished_at?: string | null;
  completed_at?: string | null;
  total_count?: number;
  processed_count?: number;
  created_count?: number;
  updated_count?: number;
  skipped_count?: number;
  failed_count?: number;
  token_count?: number;
  available?: number;
  cooling?: number;
  disabled?: number;
  missing?: number;
  token_ids?: number[];
  observed_cost_usd?: number | null;
  average_observed_cost_usd?: number | null;
  last_error?: string | null;
  error_message?: string | null;
};

export type ImportBatchItem = {
  id: number;
  job_id: number;
  item_index: number;
  status: string;
  token_id?: number | null;
  action?: string | null;
  error_message?: string | null;
  validation_ms?: number | null;
  publish_ms?: number | null;
  validation_started_at?: string | null;
  validation_finished_at?: string | null;
  published_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ImportBatchDetail = {
  job?: ImportBatch;
  items?: ImportBatchItem[];
  tokens?: TokenItem[];
};

export type ImportParseResponse = {
  items?: Array<string | Record<string, unknown>>;
  summary?: {
    total?: number;
    deduplicated?: boolean;
  };
};

export type OpenAIOAuthStartResponse = {
  auth_url: string;
  session_id?: string;
  state?: string;
  redirect_uri?: string;
  expires_in_sec?: number;
};

export type OpenAIOAuthExchangeResponse = {
  job?: ImportBatch;
  result?: Record<string, unknown>;
};

export type RequestItem = {
  id?: number;
  request_id?: string;
  owner_user_id?: number | null;
  api_key_id?: number | null;
  started_at?: string | null;
  endpoint?: string | null;
  model?: string | null;
  model_name?: string | null;
  prompt_cache_source?: string | null;
  status_code?: number | null;
  success?: boolean | null;
  ttft_ms?: number | null;
  attempt_count?: number | null;
  estimated_cost_usd?: number | null;
  error_message?: string | null;
};

export type RequestSummary = {
  total?: number;
  success?: number;
  failure?: number;
  total_tokens?: number;
  estimated_cost_usd?: number;
  average_ttft_ms?: number;
};

export type PlatformUser = {
  id: number;
  email: string;
  display_name?: string | null;
  role: "service" | "admin" | "readonly_admin" | "user" | string;
  status: "active" | "disabled" | "suspended" | "deleted" | string;
  plan?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  last_seen_at?: string | null;
  last_login_at?: string | null;
};

export type APIKeyItem = {
  id: number;
  user_id?: number | null;
  name?: string | null;
  prefix?: string | null;
  key_prefix?: string | null;
  kind?: string | null;
  role?: string | null;
  scopes?: string[];
  created_at?: string | null;
  last_used_at?: string | null;
  expires_at?: string | null;
  revoked_at?: string | null;
};

export type CreatedAPIKey = APIKeyItem & {
  value?: string;
  plaintext_key?: string;
};

export type MeResponse = {
  principal_type?: string;
  user?: PlatformUser | null;
  role?: string;
  scopes?: string[];
  capabilities?: string[];
  act_as_user_id?: number | null;
};

export type PoolSummaryResponse = {
  counts?: TokenCounts;
  plan_counts?: TokenPlanCount[];
  error_counts?: TokenErrorCount[];
};

export type UsageSummary = {
  hours?: number;
  total?: number;
  success?: number;
  failure?: number;
  input_tokens?: number;
  cached_input_tokens?: number;
  total_tokens?: number;
  estimated_cost_usd?: number;
  cache_hit_ratio?: number;
  average_ttft_ms?: number;
  average_duration_ms?: number;
};

export type OwnerUsageSummary = {
  owner_user_id?: number;
  hours?: number;
  request_count?: number;
  success_count?: number;
  failure_count?: number;
  streaming_count?: number;
  input_tokens?: number;
  cached_input_tokens?: number;
  total_tokens?: number;
  estimated_cost_usd?: number;
  observed_cost_usd?: number;
  success_rate?: number;
  cache_hit_ratio?: number;
};

export type AdminUserListResponse = {
  items?: PlatformUser[];
  pagination?: Pagination;
};

export type SettingItem = {
  key: string;
  value: unknown;
  updated_at?: string | null;
};

export type Sub2APITarget = {
  id: number;
  name: string;
  base_url: string;
  admin_key_set?: boolean;
  enabled: boolean;
  owner_user_id: number;
  owner_email?: string | null;
  plan_filters?: string[];
  token_status_filters?: string[];
  target_group_ids?: number[];
  target_group_names?: string[];
  check_interval_seconds: number;
  min_available: number;
  top_up_batch_size: number;
  account_concurrency: number;
  account_priority: number;
  proxy_id: number;
  auto_sync_new: boolean;
  last_checked_at?: string | null;
  last_synced_at?: string | null;
  last_status?: string | null;
  last_error?: string | null;
  last_remote_count?: number | null;
  last_synced_count?: number;
  last_run_id?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type Sub2APIGroup = {
  id: number;
  name: string;
  platform?: string;
  status?: string;
};

export type Sub2APIProxy = {
  id: number;
  name: string;
  protocol?: string;
  host?: string;
  port?: number;
  status?: string;
  account_count?: number;
};

export type Sub2APIRun = {
  id: number;
  target_id: number;
  trigger: string;
  status: string;
  remote_count: number;
  threshold: number;
  needed_count: number;
  selected_count: number;
  synced_count: number;
  failed_count: number;
  error_message?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  details?: Record<string, unknown>;
};

export type HealthResponse = {
  ok?: boolean;
  degraded?: boolean;
  service_key_protected?: boolean;
  counts?: TokenCounts;
  token_pool?: Record<string, unknown>;
};

let currentAuth: MeResponse | null = null;

export function setAuthContext(value: MeResponse | null): void {
  currentAuth = value;
}

export function getAuthContext(): MeResponse | null {
  return currentAuth;
}

export function isAdminPrincipal(value: MeResponse | null = currentAuth): boolean {
  const role = String(value?.role || value?.user?.role || "").toLowerCase();
  return role === "admin" || role === "readonly_admin" || role === "service" || Boolean(value?.capabilities?.some((item) => item.startsWith("admin:")));
}

export function isServicePrincipal(value: MeResponse | null = currentAuth): boolean {
  const role = String(value?.role || value?.user?.role || "").toLowerCase();
  return value?.principal_type === "service" || role === "service";
}

export function isSelfUserMode(): boolean {
  return Boolean(currentAuth?.user?.id) && !isAdminPrincipal(currentAuth);
}

export function hasUserPrincipal(value: MeResponse | null = currentAuth): boolean {
  return Boolean(value?.user?.id);
}

export function getServiceKey(): string {
  try {
    const current = window.localStorage.getItem(KEY_STORAGE) || "";
    if (current) {
      return current;
    }
    const legacy = window.localStorage.getItem(LEGACY_KEY_STORAGE) || "";
    if (legacy) {
      window.localStorage.setItem(KEY_STORAGE, legacy);
    }
    return legacy;
  } catch {
    return "";
  }
}

export function setServiceKey(value: string): void {
  try {
    const key = value.trim();
    if (key) {
      window.localStorage.setItem(KEY_STORAGE, key);
      window.localStorage.setItem(LEGACY_KEY_STORAGE, key);
    } else {
      window.localStorage.removeItem(KEY_STORAGE);
      window.localStorage.removeItem(LEGACY_KEY_STORAGE);
    }
  } catch {}
}

export function hasServiceKey(): boolean {
  return getServiceKey().trim() !== "";
}

export function isAuthContextPending(): boolean {
  return hasServiceKey() && currentAuth == null;
}

async function requestJSON<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers || {});
  const key = getServiceKey();
  if (key) {
    headers.set("Authorization", `Bearer ${key}`);
  }
  const response = await fetch(path, { ...init, headers });
  const text = await response.text();
  const payload = text ? parseJSON(text) : {};
  if (!response.ok) {
    const message =
      payload?.error?.message ||
      payload?.error ||
      payload?.detail?.message ||
      payload?.detail ||
      payload?.message ||
      response.statusText ||
      "请求失败";
    throw Object.assign(new Error(String(message)), {
      payload,
      status: response.status,
    });
  }
  return payload as T;
}

function parseJSON(text: string): any {
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function postJSON<T>(path: string, body: unknown): Promise<T> {
  return requestJSON<T>(path, {
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
}

function postForm<T>(path: string, body: FormData): Promise<T> {
  return requestJSON<T>(path, {
    body,
    method: "POST",
  });
}

function deleteJSON<T>(path: string): Promise<T> {
  return requestJSON<T>(path, { method: "DELETE" });
}

function patchJSON<T>(path: string, body: unknown): Promise<T> {
  return requestJSON<T>(path, {
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
    method: "PATCH",
  });
}

function scopedPath(userPath: string, adminPath: string): string {
  return hasUserPrincipal() ? userPath : adminPath;
}

export type TokenAPIScope = "auto" | "self" | "admin";

function tokenScopedPath(userPath: string, adminPath: string, scope: TokenAPIScope = "auto"): string {
  if (scope === "self") {
    return userPath;
  }
  if (scope === "admin") {
    return adminPath;
  }
  return currentAuth?.user?.id ? userPath : scopedPath(userPath, adminPath);
}

function oauthBase(): string {
  return hasUserPrincipal() ? "/api/oauth/openai" : "/admin/oauth/openai";
}

export type ImportAPIScope = "self" | "admin";

export const api = {
  health: () => requestJSON<HealthResponse>("/healthz"),
  me: () => requestJSON<MeResponse>("/api/me"),
  register: (payload: Record<string, unknown>) =>
    postJSON<{ user?: PlatformUser; api_key?: CreatedAPIKey }>("/api/auth/register", payload),
  login: (payload: Record<string, unknown>) =>
    postJSON<{ user?: PlatformUser; api_key?: CreatedAPIKey }>("/api/auth/login", payload),
  myAPIKeys: () => requestJSON<{ items?: APIKeyItem[] }>("/api/me/api-keys"),
  createMyAPIKey: (payload: Record<string, unknown>) =>
    postJSON<{ api_key?: CreatedAPIKey }>("/api/me/api-keys", payload),
  revokeMyAPIKey: (id: number) => deleteJSON<Record<string, unknown>>(`/api/me/api-keys/${id}`),
  myUsage: (hours = 24) => requestJSON<{ usage?: UsageSummary }>(`/api/me/usage?hours=${hours}`),
  myPoolSummary: () => requestJSON<PoolSummaryResponse>("/api/me/pool-summary"),
  mySettings: () => requestJSON<{ items?: SettingItem[] }>("/api/me/settings"),
  updateMySetting: (key: string, value: unknown) =>
    postJSON<SettingItem>(`/api/me/settings/${encodeURIComponent(key)}`, value),
  runtime: () => requestJSON<Record<string, unknown>>("/admin/runtime"),
  tokenSelection: () => requestJSON<Record<string, unknown>>("/admin/token-selection"),
  updateTokenSelection: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>("/admin/token-selection", payload),
  listTokens: (params: URLSearchParams, scope: TokenAPIScope = "auto") =>
    requestJSON<TokenListResponse>(`${tokenScopedPath("/api/tokens", "/admin/tokens", scope)}?${params.toString()}`),
  getToken: (id: number, includeQuota = false, scope: TokenAPIScope = "auto") => {
    const params = new URLSearchParams();
    if (includeQuota) {
      params.set("include_quota", "true");
    }
    const query = params.toString();
    return requestJSON<TokenItem>(`${tokenScopedPath(`/api/tokens/${id}`, `/admin/tokens/${id}`, scope)}${query ? `?${query}` : ""}`);
  },
  tokenRefreshToken: (id: number, scope: TokenAPIScope = "auto") =>
    requestJSON<{ refresh_token?: string }>(tokenScopedPath(`/api/tokens/${id}/refresh-token`, `/admin/token-refresh-tokens/${id}`, scope)),
  tokenQuotaResetCredits: (id: number, scope: TokenAPIScope = "auto") =>
    requestJSON<TokenQuotaResetCreditsResponse>(tokenScopedPath(`/api/tokens/${id}/quota-reset-credits`, `/admin/token-quota-reset-credits/${id}`, scope)),
  resetTokenQuota: (id: number, scope: TokenAPIScope = "auto") =>
    postJSON<TokenQuotaResetResult>(tokenScopedPath(`/api/tokens/${id}/quota-reset`, `/admin/token-quota-reset/${id}`, scope), {}),
  tokenCosts: (ids: number[]) =>
    requestJSON<{ items?: TokenObservedCostItem[] }>(`/admin/tokens/costs?ids=${ids.join(",")}`),
  updateActivation: (id: number, payload: Record<string, unknown>, scope: TokenAPIScope = "auto") => {
    if (tokenScopedPath("/api", "/admin", scope) === "/api") {
      return patchJSON<Record<string, unknown>>(`/api/tokens/${id}`, { is_active: Boolean(payload.active) });
    }
    return postJSON<Record<string, unknown>>(`/admin/tokens/${id}/activation`, payload);
  },
  updateRemark: (id: number, remark: string, scope: TokenAPIScope = "auto") => {
    if (tokenScopedPath("/api", "/admin", scope) === "/api") {
      return patchJSON<Record<string, unknown>>(`/api/tokens/${id}`, { remark });
    }
    return postJSON<Record<string, unknown>>(`/admin/tokens/${id}/remark`, { remark });
  },
  probeToken: (id: number, payload: Record<string, unknown> = {}, scope: TokenAPIScope = "auto") =>
    postJSON<TokenProbeResponse>(tokenScopedPath(`/api/tokens/${id}/probe`, `/admin/tokens/${id}/probe`, scope), payload),
  deleteToken: (id: number, scope: TokenAPIScope = "auto") =>
    deleteJSON<Record<string, unknown>>(tokenScopedPath(`/api/tokens/${id}`, `/admin/tokens/${id}`, scope)),
  batchTokens: async (payload: Record<string, unknown>, scope: TokenAPIScope = "auto") => {
    if (tokenScopedPath("/api", "/admin", scope) === "/admin") {
      return postJSON<Record<string, unknown>>("/admin/tokens/batch", payload);
    }
    const ids = Array.isArray(payload.token_ids) ? payload.token_ids.map(Number).filter(Boolean) : [];
    const action = String(payload.action || "");
    if (action === "delete") {
      await Promise.all(ids.map((id) => api.deleteToken(id, "self")));
    } else if (action === "enable" || action === "disable") {
      await Promise.all(ids.map((id) => api.updateActivation(id, { active: action === "enable" }, "self")));
    }
    return { ok: true, count: ids.length };
  },
  parseImport: (payload: Record<string, unknown>) =>
    postJSON<ImportParseResponse>(scopedPath("/api/import/parse", "/admin/import/parse"), payload),
  uploadImport: (body: FormData) =>
    postForm<ImportParseResponse>(scopedPath("/api/import/upload", "/admin/import/upload"), body),
  createImportJob: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>(scopedPath("/api/import/jobs", "/admin/import/jobs"), payload),
  importTokens: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>("/admin/tokens/import", payload),
  startOpenAIOAuth: (payload: Record<string, unknown>) =>
    postJSON<OpenAIOAuthStartResponse>(`${oauthBase()}/sessions`, payload),
  exchangeOpenAIOAuth: (payload: Record<string, unknown>) => {
    const sessionID = String(payload.session_id || "");
    return postJSON<OpenAIOAuthExchangeResponse>(`${oauthBase()}/sessions/${encodeURIComponent(sessionID)}/exchange`, payload);
  },
  importJobs: (limit = 50, scope: ImportAPIScope = "self") =>
    requestJSON<{ items?: ImportBatch[] }>(
      scope === "admin" || !hasUserPrincipal() ? `/admin/tokens/import-batches?limit=${limit}` : `/api/import/jobs?limit=${limit}`,
    ),
  importJob: async (id: number, scope: ImportAPIScope = "self") => {
    if (scope === "admin" || !hasUserPrincipal()) {
      return requestJSON<ImportBatchDetail>(`/admin/tokens/import-jobs/${id}?include_quota=true`);
    }
    const [jobPayload, itemPayload, tokenPayload] = await Promise.all([
      requestJSON<ImportBatch | { job?: ImportBatch }>(`/api/import/jobs/${id}`),
      requestJSON<{ items?: ImportBatchItem[] }>(`/api/import/jobs/${id}/items?limit=500`),
      requestJSON<TokenListResponse>(`/api/import/jobs/${id}/tokens?include_quota=true&limit=500`),
    ]);
    return {
      job: (jobPayload as { job?: ImportBatch }).job || (jobPayload as ImportBatch),
      items: itemPayload.items || [],
      tokens: tokenPayload.items || [],
    } satisfies ImportBatchDetail;
  },
  cancelImportJob: (id: number, scope: ImportAPIScope = "self") =>
    postJSON<Record<string, unknown>>(
      scope === "admin" || !hasUserPrincipal() ? `/admin/tokens/import-jobs/${id}/cancel` : `/api/import/jobs/${id}/cancel`,
      {},
    ),
  deleteImportJob: (id: number, scope: ImportAPIScope = "self") =>
    deleteJSON<Record<string, unknown>>(scope === "admin" || !hasUserPrincipal() ? `/admin/tokens/import-jobs/${id}` : `/api/import/jobs/${id}`),
  requests: async (limit = 80) => {
    if (!hasUserPrincipal()) {
      return requestJSON<{ items?: RequestItem[]; summary?: RequestSummary }>(`/admin/requests?limit=${limit}`);
    }
    const [requestPayload, usagePayload] = await Promise.all([
      requestJSON<{ items?: RequestItem[]; pagination?: Pagination }>(`/api/requests?limit=${limit}&include_total=true`),
      api.myUsage(24),
    ]);
    const usage = usagePayload.usage || {};
    return {
      items: requestPayload.items || [],
      summary: {
        total: usage.total || 0,
        success: usage.success || 0,
        failure: usage.failure || 0,
        total_tokens: usage.total_tokens || 0,
        estimated_cost_usd: usage.estimated_cost_usd || 0,
        average_ttft_ms: usage.average_ttft_ms || 0,
      },
    };
  },
  analytics: (hours = 24) => {
    if (hasUserPrincipal()) {
      return Promise.resolve({ hours, models: [] });
    }
    return requestJSON<Record<string, unknown>>(`/admin/analytics?hours=${hours}`);
  },
  settings: () => requestJSON<{ items?: SettingItem[] }>("/admin/settings"),
  updateSetting: (key: string, value: unknown) =>
    postJSON<Record<string, unknown>>(`/admin/settings/${encodeURIComponent(key)}`, value),
  adminUsers: (params = new URLSearchParams()) =>
    requestJSON<AdminUserListResponse>(`/api/admin/users${params.toString() ? `?${params.toString()}` : ""}`),
  adminCreateUser: (payload: Record<string, unknown>) =>
    postJSON<{ user?: PlatformUser; api_key?: CreatedAPIKey }>("/api/admin/users", payload),
  adminUser: (id: number) => requestJSON<{ user?: PlatformUser }>(`/api/admin/users/${id}`),
  adminUpdateUser: (id: number, payload: Record<string, unknown>) =>
    patchJSON<{ user?: PlatformUser }>(`/api/admin/users/${id}`, payload),
  adminUserAPIKeys: (id: number) =>
    requestJSON<{ items?: APIKeyItem[] }>(`/api/admin/users/${id}/api-keys`),
  adminUserTokens: (id: number, params = new URLSearchParams({ limit: "50" })) =>
    requestJSON<TokenListResponse>(`/api/admin/users/${id}/tokens?${params.toString()}`),
  adminUserImportJobs: (id: number, params = new URLSearchParams({ limit: "50" })) =>
    requestJSON<{ items?: ImportBatch[]; pagination?: Pagination }>(`/api/admin/users/${id}/import/jobs?${params.toString()}`),
  adminUserRequests: (id: number, params = new URLSearchParams({ limit: "80", include_total: "true" })) =>
    requestJSON<{ items?: RequestItem[]; pagination?: Pagination }>(`/api/admin/users/${id}/requests?${params.toString()}`),
  adminUserUsage: (id: number, hours = 24) =>
    requestJSON<{ pool?: TokenCounts; usage?: OwnerUsageSummary }>(`/api/admin/users/${id}/usage?hours=${hours}`),
  adminCreateUserAPIKey: (id: number, payload: Record<string, unknown>) =>
    postJSON<{ api_key?: CreatedAPIKey }>(`/api/admin/users/${id}/api-keys`, payload),
  adminRevokeUserAPIKey: (userID: number, keyID: number) =>
    deleteJSON<Record<string, unknown>>(`/api/admin/users/${userID}/api-keys/${keyID}`),
  adminPoolSummary: () => requestJSON<PoolSummaryResponse>("/api/admin/pool-summary"),
  adminPoolSummaryByUser: (paramsOrLimit: URLSearchParams | number = 100) => {
    const params = paramsOrLimit instanceof URLSearchParams ? new URLSearchParams(paramsOrLimit) : new URLSearchParams({ limit: String(paramsOrLimit) });
    if (!params.has("limit")) {
      params.set("limit", "100");
    }
    return requestJSON<{ items?: Array<{ user?: PlatformUser; counts?: TokenCounts; plan_counts?: TokenPlanCount[]; usage?: OwnerUsageSummary }>; pagination?: Pagination }>(
      `/api/admin/pool-summary/by-user?${params.toString()}`,
    );
  },
  adminRequests: (paramsOrLimit: URLSearchParams | number = 120) => {
    const params = paramsOrLimit instanceof URLSearchParams ? new URLSearchParams(paramsOrLimit) : new URLSearchParams({ limit: String(paramsOrLimit) });
    if (!params.has("limit")) {
      params.set("limit", "120");
    }
    params.set("include_total", "true");
    return requestJSON<{ items?: RequestItem[]; pagination?: Pagination }>(`/api/admin/requests?${params.toString()}`);
  },
  adminRequestsExportURL: (params = new URLSearchParams()) => `/api/admin/requests/export?${params.toString()}`,
  adminAuditLogs: (limit = 100) =>
    requestJSON<{ items?: Array<Record<string, unknown>>; pagination?: Pagination }>(`/api/admin/audit-logs?limit=${limit}`),
  sub2APITargets: () => requestJSON<{ items?: Sub2APITarget[] }>("/api/admin/sub2api/targets"),
  createSub2APITarget: (payload: Record<string, unknown>) =>
    postJSON<{ target?: Sub2APITarget }>("/api/admin/sub2api/targets", payload),
  updateSub2APITarget: (id: number, payload: Record<string, unknown>) =>
    patchJSON<{ target?: Sub2APITarget }>(`/api/admin/sub2api/targets/${id}`, payload),
  deleteSub2APITarget: (id: number) =>
    deleteJSON<Record<string, unknown>>(`/api/admin/sub2api/targets/${id}`),
  sub2APITargetGroups: (id: number) =>
    requestJSON<{ items?: Sub2APIGroup[] }>(`/api/admin/sub2api/targets/${id}/groups`),
  sub2APITargetProxies: (id: number) =>
    requestJSON<{ items?: Sub2APIProxy[] }>(`/api/admin/sub2api/targets/${id}/proxies`),
  probeSub2APIGroups: (payload: Record<string, unknown>) =>
    postJSON<{ items?: Sub2APIGroup[] }>("/api/admin/sub2api/probe-groups", payload),
  probeSub2APIProxies: (payload: Record<string, unknown>) =>
    postJSON<{ items?: Sub2APIProxy[] }>("/api/admin/sub2api/probe-proxies", payload),
  checkSub2APITarget: (id: number) =>
    postJSON<{ run?: Sub2APIRun }>(`/api/admin/sub2api/targets/${id}/check`, {}),
  syncSub2APITarget: (id: number) =>
    postJSON<{ run?: Sub2APIRun }>(`/api/admin/sub2api/targets/${id}/sync`, {}),
  sub2APIRuns: (targetID?: number, limit = 80) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (targetID) params.set("target_id", String(targetID));
    return requestJSON<{ items?: Sub2APIRun[] }>(`/api/admin/sub2api/runs?${params.toString()}`);
  },
};
