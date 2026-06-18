import { getServiceKey } from "./api";

type AdminRequestOptions = {
  body?: unknown;
  headers?: HeadersInit;
  method?: string;
  query?: Record<string, string | number | boolean | null | undefined>;
};

export async function adminRequest<T>(path: string, options: AdminRequestOptions = {}): Promise<T> {
  const url = new URL(path, window.location.origin);
  for (const [key, value] of Object.entries(options.query || {})) {
    if (value !== undefined && value !== null && String(value) !== "") {
      url.searchParams.set(key, String(value));
    }
  }
  const headers = new Headers(options.headers || {});
  const serviceKey = getServiceKey();
  if (serviceKey) {
    headers.set("Authorization", `Bearer ${serviceKey}`);
  }
  let body: BodyInit | undefined;
  if (options.body instanceof FormData) {
    body = options.body;
  } else if (options.body !== undefined) {
    body = JSON.stringify(options.body);
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(url.pathname + url.search, {
    body,
    headers,
    method: options.method || "GET",
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const message = payload?.error?.message || payload?.detail || response.statusText || "请求失败";
    throw Object.assign(new Error(String(message)), { payload, status: response.status });
  }
  return payload as T;
}

export const generatedAdminAPI = {
  openapi: () => adminRequest<Record<string, unknown>>("/admin/openapi.json"),
  options: () => adminRequest<Record<string, unknown>>("/admin/options"),
  listTokens: (query?: AdminRequestOptions["query"]) => adminRequest<Record<string, unknown>>("/admin/tokens", { query }),
  exportTokens: (query?: AdminRequestOptions["query"]) => adminRequest<unknown>("/admin/tokens/export", { query }),
  patchToken: (id: number, body: Record<string, unknown>) => adminRequest<Record<string, unknown>>(`/admin/tokens/${id}`, { body, method: "PATCH" }),
  parseImport: (body: Record<string, unknown>) => adminRequest<Record<string, unknown>>("/admin/import/parse", { body, method: "POST" }),
  uploadImport: (body: FormData) => adminRequest<Record<string, unknown>>("/admin/import/upload", { body, method: "POST" }),
  createImportJob: (body: Record<string, unknown>) => adminRequest<Record<string, unknown>>("/admin/import/jobs", { body, method: "POST" }),
  listImportJobs: (query?: AdminRequestOptions["query"]) => adminRequest<Record<string, unknown>>("/admin/import/jobs", { query }),
  listRequests: (query?: AdminRequestOptions["query"]) => adminRequest<Record<string, unknown>>("/admin/requests", { query }),
  runtime: () => adminRequest<Record<string, unknown>>("/admin/runtime"),
  settings: () => adminRequest<Record<string, unknown>>("/admin/settings"),
};
