const KEY_STORAGE = "oaix.serviceApiKey";
const LEGACY_KEY_STORAGE = "oaix.serviceKey";

export type TokenCounts = {
  total?: number;
  available?: number;
  active?: number;
  cooling?: number;
  disabled?: number;
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

export type TokenQuotaSnapshot = {
  fetched_at?: string | null;
  error?: string | null;
  plan_type?: string | null;
  windows?: TokenQuotaWindow[];
};

export type TokenItem = {
  id: number;
  email?: string | null;
  account_id?: string | null;
  plan_type?: string | null;
  remark?: string | null;
  source_file?: string | null;
  is_active: boolean;
  cooldown_until?: string | null;
  disabled_at?: string | null;
  last_used_at?: string | null;
  last_error?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
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
  items?: TokenItem[];
  pagination?: Pagination;
};

export type ImportBatch = {
  id: number;
  status: string;
  submitted_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  total_count?: number;
  processed_count?: number;
  created_count?: number;
  updated_count?: number;
  failed_count?: number;
  error_message?: string | null;
};

export type RequestItem = {
  id?: number;
  started_at?: string | null;
  endpoint?: string | null;
  model?: string | null;
  model_name?: string | null;
  prompt_cache_source?: string | null;
  status_code?: number | null;
  success?: boolean | null;
  ttft_ms?: number | null;
  attempt_count?: number | null;
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

export type SettingItem = {
  key: string;
  value: unknown;
  updated_at?: string | null;
};

export type HealthResponse = {
  ok?: boolean;
  degraded?: boolean;
  service_key_protected?: boolean;
  counts?: TokenCounts;
  token_pool?: Record<string, unknown>;
};

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

export const api = {
  health: () => requestJSON<HealthResponse>("/healthz"),
  runtime: () => requestJSON<Record<string, unknown>>("/admin/runtime"),
  tokenSelection: () => requestJSON<Record<string, unknown>>("/admin/token-selection"),
  updateTokenSelection: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>("/admin/token-selection", payload),
  listTokens: (params: URLSearchParams) =>
    requestJSON<TokenListResponse>(`/admin/tokens?${params.toString()}`),
  tokenCosts: (ids: number[]) =>
    requestJSON<{ items?: TokenObservedCostItem[] }>(`/admin/tokens/costs?ids=${ids.join(",")}`),
  updateActivation: (id: number, payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>(`/admin/tokens/${id}/activation`, payload),
  updateRemark: (id: number, remark: string) =>
    postJSON<Record<string, unknown>>(`/admin/tokens/${id}/remark`, { remark }),
  probeToken: (id: number, payload: Record<string, unknown> = {}) =>
    postJSON<TokenProbeResponse>(`/admin/tokens/${id}/probe`, payload),
  deleteToken: (id: number) =>
    requestJSON<Record<string, unknown>>(`/admin/tokens/${id}`, { method: "DELETE" }),
  batchTokens: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>("/admin/tokens/batch", payload),
  importTokens: (payload: Record<string, unknown>) =>
    postJSON<Record<string, unknown>>("/admin/tokens/import", payload),
  importJobs: (limit = 50) =>
    requestJSON<{ items?: ImportBatch[] }>(`/admin/tokens/import-batches?limit=${limit}`),
  cancelImportJob: (id: number) =>
    postJSON<Record<string, unknown>>(`/admin/tokens/import-jobs/${id}/cancel`, {}),
  requests: (limit = 80) =>
    requestJSON<{ items?: RequestItem[]; summary?: RequestSummary }>(`/admin/requests?limit=${limit}`),
  analytics: (hours = 24) =>
    requestJSON<Record<string, unknown>>(`/admin/analytics?hours=${hours}`),
  settings: () => requestJSON<{ items?: SettingItem[] }>("/admin/settings"),
  updateSetting: (key: string, value: unknown) =>
    postJSON<Record<string, unknown>>(`/admin/settings/${encodeURIComponent(key)}`, value),
};
