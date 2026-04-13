const STORAGE_KEY = "oaix.serviceKey";
const REFRESH_INTERVAL_MS = 15000;

const state = {
  serviceKey: localStorage.getItem(STORAGE_KEY) || "",
  protected: false,
  timer: null,
};

const elements = {
  availableCount: document.getElementById("available-count"),
  coolingCount: document.getElementById("cooling-count"),
  disabledCount: document.getElementById("disabled-count"),
  totalCount: document.getElementById("total-count"),
  barAvailable: document.getElementById("bar-available"),
  barCooling: document.getElementById("bar-cooling"),
  barDisabled: document.getElementById("bar-disabled"),
  legendAvailable: document.getElementById("legend-available"),
  legendCooling: document.getElementById("legend-cooling"),
  legendDisabled: document.getElementById("legend-disabled"),
  serviceKey: document.getElementById("service-key"),
  protectionChip: document.getElementById("protection-chip"),
  syncChip: document.getElementById("sync-chip"),
  refreshButton: document.getElementById("refresh-button"),
  saveKeyButton: document.getElementById("save-key-button"),
  clearKeyButton: document.getElementById("clear-key-button"),
  tokenJson: document.getElementById("token-json"),
  tokenFiles: document.getElementById("token-files"),
  importButton: document.getElementById("import-button"),
  resetImportButton: document.getElementById("reset-import-button"),
  importFeedback: document.getElementById("import-feedback"),
  tokenList: document.getElementById("token-list"),
  listNote: document.getElementById("list-note"),
  requestList: document.getElementById("request-list"),
  requestNote: document.getElementById("request-note"),
  requestTotalCount: document.getElementById("request-total-count"),
  requestSuccessCount: document.getElementById("request-success-count"),
  requestFailedCount: document.getElementById("request-failed-count"),
  requestAvgTtft: document.getElementById("request-avg-ttft"),
};

elements.serviceKey.value = state.serviceKey;

function authHeaders(needsJson = false) {
  const headers = {};
  if (needsJson) {
    headers["Content-Type"] = "application/json";
  }
  if (state.serviceKey) {
    headers.Authorization = `Bearer ${state.serviceKey}`;
  }
  return headers;
}

function setFeedback(message, tone = "") {
  elements.importFeedback.textContent = message;
  elements.importFeedback.classList.remove("is-error", "is-success");
  if (tone) {
    elements.importFeedback.classList.add(tone);
  }
}

function formatDate(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatPercent(value, total) {
  if (!total) {
    return "0%";
  }
  return `${Math.round((value / total) * 100)}%`;
}

function formatDurationMs(value) {
  if (value == null || Number.isNaN(Number(value))) {
    return "—";
  }
  const ms = Number(value);
  if (ms < 1000) {
    return `${ms} ms`;
  }
  if (ms < 10000) {
    return `${(ms / 1000).toFixed(1)} s`;
  }
  return `${Math.round(ms / 1000)} s`;
}

function clampWidth(value, total) {
  if (!total || value <= 0) {
    return "0%";
  }
  return `${Math.max(6, Math.round((value / total) * 100))}%`;
}

function deriveStatus(item) {
  if (!item.is_active) {
    return { label: "已禁用", tone: "disabled" };
  }
  if (item.cooldown_until) {
    const cooldownAt = new Date(item.cooldown_until);
    if (!Number.isNaN(cooldownAt.getTime()) && cooldownAt.getTime() > Date.now()) {
      return { label: "冷却中", tone: "cooling" };
    }
  }
  return { label: "可用", tone: "available" };
}

function renderCounts(counts) {
  const total = counts.total || 0;
  const available = counts.available || 0;
  const cooling = counts.cooling || 0;
  const disabled = counts.disabled ?? Math.max(0, total - (counts.active || 0));

  elements.availableCount.textContent = available;
  elements.coolingCount.textContent = cooling;
  elements.disabledCount.textContent = disabled;
  elements.totalCount.textContent = total;

  elements.legendAvailable.textContent = formatPercent(available, total);
  elements.legendCooling.textContent = formatPercent(cooling, total);
  elements.legendDisabled.textContent = formatPercent(disabled, total);

  elements.barAvailable.style.width = clampWidth(available, total);
  elements.barCooling.style.width = clampWidth(cooling, total);
  elements.barDisabled.style.width = clampWidth(disabled, total);
}

function deriveRequestStatus(item) {
  const statusCode = Number(item.status_code || 0);
  if (statusCode >= 200 && statusCode < 300 && item.success !== false) {
    return { label: String(statusCode || 200), tone: "success" };
  }
  if (statusCode > 0) {
    return { label: String(statusCode), tone: "failed" };
  }
  if (item.success === true) {
    return { label: "OK", tone: "success" };
  }
  return { label: "ERR", tone: "failed" };
}

function renderRequestSummary(summary) {
  const total = summary?.total || 0;
  const successful = summary?.successful || 0;
  const failed = summary?.failed || 0;
  const avgTtft = summary?.avg_ttft_ms;

  elements.requestTotalCount.textContent = total;
  elements.requestSuccessCount.textContent = successful;
  elements.requestFailedCount.textContent = failed;
  elements.requestAvgTtft.textContent = formatDurationMs(avgTtft);
}

function renderTokenList(items) {
  if (!Array.isArray(items) || items.length === 0) {
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>没有最近记录。先导入一批 key，列表会在这里展开。</p>
      </article>
    `;
    return;
  }

  elements.tokenList.innerHTML = items
    .map((item) => {
      const status = deriveStatus(item);
      const account = item.email || item.account_id || "未命名账号";
      const meta = item.account_id && item.email ? item.account_id : item.source_file || "无来源记录";
      return `
        <article class="token-row">
          <div class="token-row__primary">
            <span class="token-row__account">${escapeHtml(account)}</span>
            <span class="token-row__meta">${escapeHtml(meta)}</span>
          </div>
          <div>
            <span class="status-pill status-pill--${status.tone}">${status.label}</span>
          </div>
          <div class="token-row__time" data-label="最近使用">${escapeHtml(formatDate(item.last_used_at))}</div>
          <div class="token-row__time" data-label="冷却截止">${escapeHtml(formatDate(item.cooldown_until))}</div>
          <div class="token-row__error" data-label="错误摘要">${escapeHtml(item.last_error || "—")}</div>
        </article>
      `;
    })
    .join("");
}

function renderRequestList(items) {
  if (!Array.isArray(items) || items.length === 0) {
    elements.requestList.innerHTML = `
      <article class="empty-state">
        <p>还没有请求记录。等外部开始请求 /v1/responses 或 /v1/responses/compact，日志会出现在这里。</p>
      </article>
    `;
    return;
  }

  elements.requestList.innerHTML = items
    .map((item) => {
      const status = deriveRequestStatus(item);
      const model = item.model || "未带 model";
      const mode = item.is_stream ? "Stream" : "JSON";
      const attemptLabel = item.attempt_count ? `${item.attempt_count} 次` : "—";
      const endpoint = item.endpoint || "—";
      return `
        <article class="request-row">
          <div class="request-row__primary">
            <span class="request-row__title">${escapeHtml(formatDate(item.started_at))}</span>
            <span class="request-row__meta">${escapeHtml(`${model} · ${mode}`)}</span>
          </div>
          <div class="request-row__endpoint" data-label="端点">${escapeHtml(endpoint)}</div>
          <div class="request-row__status" data-label="状态码">
            <span class="status-pill status-pill--${status.tone}">${escapeHtml(status.label)}</span>
          </div>
          <div class="request-row__time" data-label="首 Token">${escapeHtml(formatDurationMs(item.ttft_ms))}</div>
          <div class="request-row__time" data-label="尝试次数">${escapeHtml(attemptLabel)}</div>
          <div class="request-row__error" data-label="错误摘要">${escapeHtml(item.error_message || "—")}</div>
        </article>
      `;
    })
    .join("");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!response.ok) {
    const detail =
      typeof data === "string"
        ? data
        : data?.detail || data?.message || `Request failed with ${response.status}`;
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }
  return data;
}

async function loadHealth() {
  const response = await fetch("/healthz");
  const health = await response.json();
  renderCounts(health.counts || {});
  state.protected = Boolean(health.service_key_protected);
  if (response.ok) {
    elements.protectionChip.textContent = state.protected ? "管理接口需要 Service Key" : "管理接口未开启鉴权";
  } else {
    elements.protectionChip.textContent = state.protected
      ? "当前池已退化，管理接口仍需要 Service Key"
      : "当前池已退化，但仍可查看状态";
  }
  elements.syncChip.textContent = `上次同步 ${new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date())}`;
  return health;
}

async function loadTokens() {
  try {
    const data = await fetchJson("/admin/tokens?limit=80", {
      headers: authHeaders(),
    });
    elements.listNote.textContent = "显示最近 80 条记录";
    renderTokenList(data.items || []);
    if (data.counts) {
      renderCounts(data.counts);
    }
  } catch (error) {
    if (error.status === 401) {
      elements.listNote.textContent = "需要输入 Service API Key 才能查看明细和导入";
      elements.tokenList.innerHTML = `
        <article class="empty-state">
          <p>管理接口已加锁。填入 Service API Key 后可查看明细和导入 key。</p>
        </article>
      `;
      return;
    }
    elements.listNote.textContent = `列表加载失败：${error.message}`;
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(error.message)}</p>
      </article>
    `;
  }
}

async function loadRequests() {
  try {
    const data = await fetchJson("/admin/requests?limit=80", {
      headers: authHeaders(),
    });
    elements.requestNote.textContent = "显示最近 80 条请求";
    renderRequestSummary(data.summary || {});
    renderRequestList(data.items || []);
  } catch (error) {
    if (error.status === 401) {
      elements.requestNote.textContent = "需要输入 Service API Key 才能查看请求日志";
      renderRequestSummary({});
      elements.requestList.innerHTML = `
        <article class="empty-state">
          <p>请求日志已加锁。填入 Service API Key 后可查看请求次数、端点、状态码和首 token 时间。</p>
        </article>
      `;
      return;
    }
    elements.requestNote.textContent = `请求日志加载失败：${error.message}`;
    renderRequestSummary({});
    elements.requestList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(error.message)}</p>
      </article>
    `;
  }
}

async function refreshDashboard() {
  elements.refreshButton.disabled = true;
  try {
    await loadHealth();
    await Promise.all([loadTokens(), loadRequests()]);
  } catch (error) {
    elements.listNote.textContent = `面板同步失败：${error.message}`;
    elements.requestNote.textContent = `面板同步失败：${error.message}`;
  } finally {
    elements.refreshButton.disabled = false;
  }
}

function normalizePayload(raw) {
  if (Array.isArray(raw)) {
    return raw.flatMap(normalizePayload);
  }
  if (raw && typeof raw === "object" && Array.isArray(raw.tokens)) {
    return raw.tokens.flatMap(normalizePayload);
  }
  if (raw && typeof raw === "object") {
    return [raw];
  }
  throw new Error("token JSON 必须是对象、数组，或 { tokens: [...] }");
}

function collectLogicalKeyLines(text) {
  const physicalLines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const logicalLines = [];
  for (const line of physicalLines) {
    if (!logicalLines.length) {
      logicalLines.push(line);
      continue;
    }

    const lastIndex = logicalLines.length - 1;
    const current = logicalLines[lastIndex];
    const currentHasComma = current.includes(",");
    const nextHasComma = line.includes(",");

    // Some paste sources hard-wrap a single key across multiple lines.
    // Keep joining until we see a new record that clearly starts with its own comma pair.
    if (currentHasComma && nextHasComma) {
      logicalLines.push(line);
      continue;
    }

    logicalLines[lastIndex] = `${current}${line}`;
  }

  return logicalLines;
}

function parseAccountRefreshLines(text, sourceLabel) {
  const payloads = [];
  const lines = collectLogicalKeyLines(text);

  lines.forEach((line, index) => {
    const commaIndex = line.indexOf(",");
    if (commaIndex <= 0 || commaIndex === line.length - 1) {
      throw new Error(`${sourceLabel} 第 ${index + 1} 条记录格式错误，应为 account_id,refresh_token`);
    }

    const accountId = line.slice(0, commaIndex).trim();
    const refreshToken = line.slice(commaIndex + 1).trim();
    if (!accountId || !refreshToken) {
      throw new Error(`${sourceLabel} 第 ${index + 1} 条记录缺少 account_id 或 refresh_token`);
    }

    payloads.push({
      account_id: accountId,
      refresh_token: refreshToken,
      type: "codex",
    });
  });

  return payloads;
}

function parseImportText(text, sourceLabel) {
  const trimmed = text.trim();
  if (!trimmed) {
    return [];
  }

  const firstChar = trimmed[0];
  if (firstChar === "{" || firstChar === "[") {
    try {
      return normalizePayload(JSON.parse(trimmed));
    } catch (error) {
      throw new Error(`${sourceLabel} JSON 解析失败：${error.message}`);
    }
  }

  return parseAccountRefreshLines(trimmed, sourceLabel);
}

async function collectImportPayloads() {
  const payloads = [];
  const textareaValue = elements.tokenJson.value.trim();
  if (textareaValue) {
    payloads.push(...parseImportText(textareaValue, "粘贴内容"));
  }

  const files = Array.from(elements.tokenFiles.files || []);
  for (const file of files) {
    const text = await file.text();
    payloads.push(...parseImportText(text, `文件 ${file.name}`));
  }

  if (payloads.length === 0) {
    throw new Error("请先粘贴 JSON / account_id,refresh_token 文本，或选择至少一个文件");
  }
  return payloads;
}

async function importTokens() {
  elements.importButton.disabled = true;
  setFeedback("正在整理并提交导入批次…");
  try {
    const payloads = await collectImportPayloads();
    const data = await fetchJson("/admin/tokens/import", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify(payloads),
    });

    const message = [
      `导入完成：成功 ${data.imported_count || 0} 条，失败 ${data.failed_count || 0} 条。`,
      data.failed_count ? `失败原因：${(data.failed || []).slice(0, 3).map((item) => item.error).join("；")}` : "",
    ]
      .filter(Boolean)
      .join(" ");

    setFeedback(message, "is-success");
    await refreshDashboard();
  } catch (error) {
    setFeedback(`导入失败：${error.message}`, "is-error");
  } finally {
    elements.importButton.disabled = false;
  }
}

function resetImportForm() {
  elements.tokenJson.value = "";
  elements.tokenFiles.value = "";
  setFeedback("等待导入。");
}

function saveServiceKey() {
  state.serviceKey = elements.serviceKey.value.trim();
  if (state.serviceKey) {
    localStorage.setItem(STORAGE_KEY, state.serviceKey);
    setFeedback("Service API Key 已保存到本地浏览器。", "is-success");
  } else {
    localStorage.removeItem(STORAGE_KEY);
    setFeedback("Service API Key 已清空。");
  }
  refreshDashboard();
}

function clearServiceKey() {
  elements.serviceKey.value = "";
  state.serviceKey = "";
  localStorage.removeItem(STORAGE_KEY);
  setFeedback("Service API Key 已清空。");
  refreshDashboard();
}

elements.refreshButton.addEventListener("click", refreshDashboard);
elements.saveKeyButton.addEventListener("click", saveServiceKey);
elements.clearKeyButton.addEventListener("click", clearServiceKey);
elements.importButton.addEventListener("click", importTokens);
elements.resetImportButton.addEventListener("click", resetImportForm);

refreshDashboard();
state.timer = window.setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
