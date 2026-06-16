const KEY_STORAGE = "oaix.serviceApiKey";

export function getServiceKey() {
  try {
    return window.localStorage.getItem(KEY_STORAGE) || "";
  } catch {
    return "";
  }
}

export function setServiceKey(value) {
  try {
    if (value) {
      window.localStorage.setItem(KEY_STORAGE, value);
    } else {
      window.localStorage.removeItem(KEY_STORAGE);
    }
  } catch {}
}

export async function getJSON(path) {
  return requestJSON(path, { method: "GET" });
}

export async function postJSON(path, body = {}) {
  return requestJSON(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function deleteJSON(path) {
  return requestJSON(path, { method: "DELETE" });
}

export async function requestJSON(path, init = {}) {
  const headers = new Headers(init.headers || {});
  const key = getServiceKey();
  if (key) {
    headers.set("Authorization", `Bearer ${key}`);
  }
  const response = await fetch(path, { ...init, headers });
  const text = await response.text();
  const payload = text ? parseJSON(text) : {};
  if (!response.ok) {
    const message = payload?.error?.message || payload?.error || response.statusText || "请求失败";
    const error = new Error(message);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
}

export const api = {
  health: () => getJSON("/healthz"),
  runtime: () => getJSON("/admin/runtime"),
  tokenSelection: () => getJSON("/admin/token-selection"),
  updateTokenSelection: (payload) => postJSON("/admin/token-selection", payload),
  listTokens: (params) => getJSON(`/admin/tokens?${params.toString()}`),
  getToken: (id) => getJSON(`/admin/tokens/${id}`),
  updateActivation: (id, payload) => postJSON(`/admin/tokens/${id}/activation`, payload),
  updateRemark: (id, remark) => postJSON(`/admin/tokens/${id}/remark`, { remark }),
  deleteToken: (id) => deleteJSON(`/admin/tokens/${id}`),
  batchTokens: (payload) => postJSON("/admin/tokens/batch", payload),
  importTokens: (payload) => postJSON("/admin/tokens/import", payload),
  importJobs: (limit = 20) => getJSON(`/admin/tokens/import-batches?limit=${limit}`),
  cancelImportJob: (id) => postJSON(`/admin/tokens/import-jobs/${id}/cancel`, {}),
  requests: (limit = 80) => getJSON(`/admin/requests?limit=${limit}`),
  analytics: (hours = 24) => getJSON(`/admin/analytics?hours=${hours}`),
  settings: () => getJSON("/admin/settings"),
  updateSetting: (key, value) =>
    requestJSON(`/admin/settings/${encodeURIComponent(key)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(value),
    }),
};

function parseJSON(text) {
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}
