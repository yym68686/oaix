const STORAGE_KEY = "oaix.serviceKey";
const THEME_STORAGE_KEY = "oaix.themePreference";
const IMPORT_JOB_STORAGE_KEY = "oaix.importJobId";
const REFRESH_INTERVAL_MS = 30000;
const IMPORT_JOB_POLL_INTERVAL_MS = 1500;
const TOAST_DURATION_MS = 5200;
const TOAST_EXIT_DURATION_MS = 220;
const TOKEN_PAGE_SIZE = 80;
const TOKEN_LIST_BASE_NOTE = `每页 ${TOKEN_PAGE_SIZE} 条记录 · 搜索、筛选、排序均作用于全池`;
const THEME_OPTIONS = new Set(["auto", "light", "dark"]);
const PROBE_MODEL_OPTIONS = ["gpt-5.5", "gpt-5.4-mini"];
const DEFAULT_PROBE_MODEL = PROBE_MODEL_OPTIONS[0];
const PLAN_TYPE_OPTIONS = ["free", "plus", "team", "pro"];
const DEFAULT_PLAN_ORDER = [...PLAN_TYPE_OPTIONS];
const TOKEN_STATUS_FILTERS = new Set(["all", "available", "cooling", "disabled"]);
const TOKEN_VIEW_MODES = new Set(["keys", "import_batches"]);
const DEFAULT_TOKEN_STATUS_FILTER = "all";
const AVAILABLE_PLAN_FILTER_ALL = "all";
const TOKEN_PLAN_FILTER_UNKNOWN = "unknown";
const TOKEN_SORT_OPTIONS = new Set([
  "-created_at",
  "created_at",
  "-last_used_at",
  "last_used_at",
  "status",
  "plan_type",
  "account",
]);
const DEFAULT_TOKEN_SORT = "-created_at";
const IMPORT_JOB_ACTIVE_STATUSES = new Set(["queued", "running"]);
const IMPORT_QUEUE_POSITION_OPTIONS = new Set(["front", "back"]);
const DEFAULT_IMPORT_QUEUE_POSITION = "front";
const TOKEN_QUERY_DEBOUNCE_MS = 240;
const TOKEN_ACTIVE_STREAM_CAP_MIN = 1;
const TOKEN_ACTIVE_STREAM_CAP_MAX = 10;
const DEFAULT_TOKEN_ACTIVE_STREAM_CAP = 10;

function readStoredImportJobId() {
  const raw = localStorage.getItem(IMPORT_JOB_STORAGE_KEY) || "";
  const value = Number.parseInt(raw, 10);
  return Number.isInteger(value) && value > 0 ? value : null;
}

function normalizeThemePreference(value) {
  return THEME_OPTIONS.has(value) ? value : "auto";
}

function normalizeProbeModel(value) {
  return PROBE_MODEL_OPTIONS.includes(value) ? value : DEFAULT_PROBE_MODEL;
}

function normalizeImportQueuePosition(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replaceAll("-", "_");
  return IMPORT_QUEUE_POSITION_OPTIONS.has(normalized) ? normalized : DEFAULT_IMPORT_QUEUE_POSITION;
}

const initialThemePreference = normalizeThemePreference(
  document.documentElement.dataset.themePreference || localStorage.getItem(THEME_STORAGE_KEY) || "auto",
);
const initialResolvedTheme = document.documentElement.dataset.colorScheme === "dark" ? "dark" : "light";

const state = {
  serviceKey: localStorage.getItem(STORAGE_KEY) || "",
  importJobId: readStoredImportJobId(),
  importJobPollTimer: null,
  importJobPolling: false,
  toastHideTimer: null,
  toastCleanupTimer: null,
  protected: false,
  timer: null,
  tokenSearchDebounceTimer: null,
  tokenActionPendingKinds: new Map(),
  tokenDeleteTargetId: null,
  tokenDetailsOpenStates: Object.create(null),
  tokenViewMode: "keys",
  tokenSearchTerm: "",
  tokenStatusFilter: DEFAULT_TOKEN_STATUS_FILTER,
  tokenPlanFilter: AVAILABLE_PLAN_FILTER_ALL,
  tokenSort: DEFAULT_TOKEN_SORT,
  selectedImportBatchId: null,
  tokenPage: 1,
  tokenPageSize: TOKEN_PAGE_SIZE,
  tokenPageLoading: false,
  tokenPagination: {
    total: 0,
    limit: TOKEN_PAGE_SIZE,
    offset: 0,
    returned: 0,
    page: 1,
    totalPages: 1,
    hasPrevious: false,
    hasNext: false,
  },
  tokenItems: [],
  tokenImportBatches: [],
  tokenFilteredCounts: {
    total: 0,
    active: 0,
    available: 0,
    cooling: 0,
    disabled: 0,
  },
  tokenPlanCounts: {
    free: 0,
    plus: 0,
    team: 0,
    pro: 0,
    unknown: 0,
  },
  healthLoaded: false,
  tokensLoaded: false,
  requestsLoaded: false,
  tokenSelectionSaving: false,
  tokenSelectionStrategy: "least_recently_used",
  tokenSelectionOrder: [],
  tokenPlanOrderEnabled: false,
  tokenPlanOrder: [...DEFAULT_PLAN_ORDER],
  tokenPlanOrderSaving: false,
  tokenPlanOrderDragType: "",
  tokenActiveStreamCap: DEFAULT_TOKEN_ACTIVE_STREAM_CAP,
  tokenActiveStreamCapDraft: DEFAULT_TOKEN_ACTIVE_STREAM_CAP,
  tokenActiveStreamCapSaving: false,
  tokenOrderSaving: false,
  tokenDragTokenId: null,
  importQueuePosition: DEFAULT_IMPORT_QUEUE_POSITION,
  refreshing: false,
  probeModel: DEFAULT_PROBE_MODEL,
  themePreference: initialThemePreference,
  resolvedTheme: initialResolvedTheme,
  themeMedia: window.matchMedia("(prefers-color-scheme: dark)"),
};

const elements = {
  themeSummary: document.getElementById("theme-summary"),
  themeButtons: Array.from(document.querySelectorAll("[data-theme-option]")),
  themeColorMeta: document.querySelector('meta[name="theme-color"]'),
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
  importQueuePositionSummary: document.getElementById("import-queue-position-summary"),
  importQueuePositionButtons: Array.from(document.querySelectorAll("[data-import-queue-position]")),
  importButton: document.getElementById("import-button"),
  resetImportButton: document.getElementById("reset-import-button"),
  importFeedback: document.getElementById("import-feedback"),
  tokenList: document.getElementById("token-list"),
  listNote: document.getElementById("list-note"),
  tokenViewButtons: Array.from(document.querySelectorAll("[data-token-view-mode]")),
  tokenQueryBar: document.getElementById("token-querybar"),
  tokenSearchInput: document.getElementById("token-search-input"),
  tokenSearchSummary: document.getElementById("token-search-summary"),
  tokenStatusFilters: document.getElementById("token-status-filters"),
  tokenPlanFilters: document.getElementById("token-plan-filters"),
  tokenSortSelect: document.getElementById("token-sort-select"),
  tokenPagination: document.getElementById("token-pagination"),
  tokenPageFirst: document.getElementById("token-page-first"),
  tokenPagePrev: document.getElementById("token-page-prev"),
  tokenPageNext: document.getElementById("token-page-next"),
  tokenPageLast: document.getElementById("token-page-last"),
  tokenPageInput: document.getElementById("token-page-input"),
  tokenPaginationSummary: document.getElementById("token-pagination-summary"),
  dispatchSettings: document.getElementById("dispatch-settings"),
  tokenSelectionSummary: document.getElementById("token-selection-summary"),
  tokenSelectionButtons: Array.from(document.querySelectorAll("[data-selection-strategy]")),
  tokenConcurrencySummary: document.getElementById("token-concurrency-summary"),
  tokenConcurrencyInput: document.getElementById("token-active-stream-cap"),
  tokenConcurrencyRange: document.getElementById("token-active-stream-cap-range"),
  tokenConcurrencySave: document.getElementById("token-active-stream-cap-save"),
  tokenPlanOrderSummary: document.getElementById("token-plan-order-summary"),
  tokenPlanOrderButtons: Array.from(document.querySelectorAll("[data-plan-order-enabled]")),
  tokenPlanOrderList: document.getElementById("token-plan-order-list"),
  requestList: document.getElementById("request-list"),
  requestNote: document.getElementById("request-note"),
  requestTotalCount: document.getElementById("request-total-count"),
  requestTotalTokens: document.getElementById("request-total-tokens"),
  requestEstimatedCost: document.getElementById("request-estimated-cost"),
  requestSuccessCount: document.getElementById("request-success-count"),
  requestFailedCount: document.getElementById("request-failed-count"),
  requestAvgTtft: document.getElementById("request-avg-ttft"),
  requestTokenChart: document.getElementById("request-token-chart"),
  requestTokenChartTotal: document.getElementById("request-token-chart-total"),
  requestTokenChartNote: document.getElementById("request-token-chart-note"),
  requestCostChart: document.getElementById("request-cost-chart"),
  requestCostChartTotal: document.getElementById("request-cost-chart-total"),
  requestCostChartNote: document.getElementById("request-cost-chart-note"),
  toastStack: document.getElementById("toast-stack"),
  tokenDeleteDialog: document.getElementById("token-delete-dialog"),
  tokenDeleteDescription: document.getElementById("token-delete-description"),
  tokenDeleteTarget: document.getElementById("token-delete-target"),
  tokenDeleteMeta: document.getElementById("token-delete-meta"),
  tokenDeleteFeedback: document.getElementById("token-delete-feedback"),
  tokenDeleteCancel: document.getElementById("token-delete-cancel"),
  tokenDeleteClose: document.getElementById("token-delete-close"),
  tokenDeleteConfirm: document.getElementById("token-delete-confirm"),
};

elements.serviceKey.value = state.serviceKey;
const supportsTokenDeleteDialog =
  typeof HTMLDialogElement !== "undefined" && elements.tokenDeleteDialog instanceof HTMLDialogElement;
const DEFAULT_IMPORT_BUTTON_LABEL = elements.importButton.textContent.trim() || "导入";

function resolveTheme(preference) {
  if (preference === "dark" || preference === "light") {
    return preference;
  }
  return state.themeMedia.matches ? "dark" : "light";
}

function updateThemeSummary() {
  if (!elements.themeSummary) {
    return;
  }
  const currentLabel = state.resolvedTheme === "dark" ? "暗色" : "亮色";
  elements.themeSummary.textContent =
    state.themePreference === "auto" ? `自动 · 当前${currentLabel}` : `手动 · ${currentLabel}`;
}

function updateThemeMeta() {
  if (!elements.themeColorMeta) {
    return;
  }
  elements.themeColorMeta.setAttribute("content", state.resolvedTheme === "dark" ? "#111827" : "#f5efe7");
}

function applyTheme(preference, { persist = true } = {}) {
  state.themePreference = normalizeThemePreference(preference);
  state.resolvedTheme = resolveTheme(state.themePreference);
  document.documentElement.dataset.themePreference = state.themePreference;
  document.documentElement.dataset.colorScheme = state.resolvedTheme;
  document.documentElement.style.colorScheme = state.resolvedTheme;

  elements.themeButtons.forEach((button) => {
    const active = button.dataset.themeOption === state.themePreference;
    button.setAttribute("aria-pressed", String(active));
  });

  updateThemeSummary();
  updateThemeMeta();

  if (persist) {
    localStorage.setItem(THEME_STORAGE_KEY, state.themePreference);
  }
}

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

function clearToastTimers() {
  if (state.toastHideTimer) {
    window.clearTimeout(state.toastHideTimer);
    state.toastHideTimer = null;
  }
  if (state.toastCleanupTimer) {
    window.clearTimeout(state.toastCleanupTimer);
    state.toastCleanupTimer = null;
  }
}

function dismissToast() {
  if (!elements.toastStack) {
    return;
  }
  clearToastTimers();
  const toast = elements.toastStack.querySelector(".toast");
  if (!(toast instanceof HTMLElement)) {
    return;
  }
  toast.classList.remove("is-visible");
  state.toastCleanupTimer = window.setTimeout(() => {
    if (elements.toastStack.contains(toast)) {
      elements.toastStack.replaceChildren();
    }
    state.toastCleanupTimer = null;
  }, TOAST_EXIT_DURATION_MS);
}

function showToast(message, tone = "default", title = "Key 测试") {
  if (!elements.toastStack) {
    return;
  }
  clearToastTimers();
  elements.toastStack.replaceChildren();

  const toast = document.createElement("article");
  toast.className = `toast toast--${tone}`;
  toast.setAttribute("role", tone === "error" ? "alert" : "status");
  toast.setAttribute("aria-live", tone === "error" ? "assertive" : "polite");

  const body = document.createElement("div");
  body.className = "toast__body";

  const titleElement = document.createElement("strong");
  titleElement.className = "toast__title";
  titleElement.textContent = title;

  const messageElement = document.createElement("p");
  messageElement.className = "toast__message";
  messageElement.textContent = String(message || "").trim() || "操作已完成。";

  const closeButton = document.createElement("button");
  closeButton.className = "toast__close";
  closeButton.type = "button";
  closeButton.dataset.toastDismiss = "true";
  closeButton.setAttribute("aria-label", "关闭提示");
  closeButton.textContent = "×";

  body.append(titleElement, messageElement);
  toast.append(body, closeButton);
  elements.toastStack.appendChild(toast);

  window.requestAnimationFrame(() => {
    toast.classList.add("is-visible");
  });
  state.toastHideTimer = window.setTimeout(dismissToast, TOAST_DURATION_MS);
}

function resolveProbeToastTone(result) {
  switch (String(result?.outcome || "").trim()) {
    case "reactivated":
      return "success";
    case "cooling":
      return "warning";
    case "disabled":
      return "error";
    default:
      return "default";
  }
}

function setImportButtonBusy(disabled, label = DEFAULT_IMPORT_BUTTON_LABEL) {
  elements.importButton.disabled = disabled;
  elements.importButton.textContent = label;
}

function persistImportJobId(jobId) {
  const normalized = Number.parseInt(String(jobId), 10);
  if (!Number.isInteger(normalized) || normalized <= 0) {
    state.importJobId = null;
    localStorage.removeItem(IMPORT_JOB_STORAGE_KEY);
    return;
  }
  state.importJobId = normalized;
  localStorage.setItem(IMPORT_JOB_STORAGE_KEY, String(normalized));
}

function stopImportJobPolling() {
  if (state.importJobPollTimer) {
    window.clearTimeout(state.importJobPollTimer);
    state.importJobPollTimer = null;
  }
}

function clearImportJobTracking() {
  stopImportJobPolling();
  state.importJobId = null;
  localStorage.removeItem(IMPORT_JOB_STORAGE_KEY);
}

function queueImportJobPoll(jobId, refreshOnFinish) {
  stopImportJobPolling();
  state.importJobPollTimer = window.setTimeout(() => {
    void pollImportJob(jobId, { refreshOnFinish });
  }, IMPORT_JOB_POLL_INTERVAL_MS);
}

function buildImportJobProgressMessage(job) {
  const totalCount = Number(job?.total_count || 0);
  const processedCount = Number(job?.processed_count || 0);
  const createdCount = Number(job?.created_count || 0);
  const updatedCount = Number(job?.updated_count || 0);
  const skippedCount = Number(job?.skipped_count || 0);
  const failedCount = Number(job?.failed_count || 0);
  const queueLabel =
    job?.status === "queued"
      ? `导入任务 #${job?.id} 已入队，等待后台 worker 领取。`
      : `后台并行导入中：已处理 ${processedCount}/${totalCount} 条。`;
  const trafficNote = Number(job?.yielded_to_response_traffic_count || 0)
    ? `期间主动给 /v1/responses 流量让路 ${job.yielded_to_response_traffic_count} 次。`
    : "";
  const timeoutNote = Number(job?.response_traffic_timeout_count || 0)
    ? `有 ${job.response_traffic_timeout_count} 条在响应流量持续繁忙时改为继续执行。`
    : "";
  return [
    queueLabel,
    `当前结果：新增 ${createdCount} 条，更新 ${updatedCount} 条，跳过 ${skippedCount} 条，失败 ${failedCount} 条。`,
    trafficNote,
    timeoutNote,
  ]
    .filter(Boolean)
    .join(" ");
}

function buildImportJobCompletedMessage(job) {
  const failedMessages = Array.isArray(job?.failed)
    ? job.failed
        .slice(0, 3)
        .map((item) => item?.error)
        .filter(Boolean)
    : [];
  return [
    `导入完成：新增 ${job?.created_count || 0} 条，更新 ${job?.updated_count || 0} 条，跳过重复 ${job?.skipped_count || 0} 条，失败 ${job?.failed_count || 0} 条。`,
    job?.response_traffic_timeout_count
      ? `导入期间有 ${job.response_traffic_timeout_count} 条记录在 /v1/responses 持续繁忙时改为继续执行，没有再因等待排空而整批失败。`
      : "",
    failedMessages.length ? `失败原因：${failedMessages.join("；")}` : "",
    job?.skipped_count ? "重复判定基于当前 RT 和历史 RT 链，不会再按 account_id 合并不同账号。" : "",
  ]
    .filter(Boolean)
    .join(" ");
}

function buildImportJobFailureMessage(job) {
  return `导入任务 #${job?.id || "?"} 失败：${job?.last_error || "后台 worker 执行失败"}`;
}

async function pollImportJob(jobId, { refreshOnFinish = true } = {}) {
  if (state.importJobPolling && Number.parseInt(String(jobId), 10) === state.importJobId) {
    return;
  }
  state.importJobPolling = true;
  persistImportJobId(jobId);
  setImportButtonBusy(true, "后台导入中…");
  try {
    const data = await fetchJson(`/admin/tokens/import-jobs/${encodeURIComponent(jobId)}`, {
      headers: authHeaders(),
    });
    const job = data?.job;
    if (!job) {
      throw new Error("导入任务状态缺失");
    }

    if (IMPORT_JOB_ACTIVE_STATUSES.has(String(job.status || ""))) {
      setFeedback(buildImportJobProgressMessage(job));
      queueImportJobPoll(job.id, refreshOnFinish);
      return;
    }

    clearImportJobTracking();
    setImportButtonBusy(false);

    if (job.status === "completed") {
      setFeedback(buildImportJobCompletedMessage(job), "is-success");
      if (refreshOnFinish) {
        await refreshDashboard();
      }
      return;
    }

    setFeedback(buildImportJobFailureMessage(job), "is-error");
    if (refreshOnFinish) {
      await refreshDashboard();
    }
  } catch (error) {
    const status = Number(error?.status || 0);
    if (status === 404) {
      clearImportJobTracking();
      setImportButtonBusy(false);
      setFeedback("后台导入任务不存在，可能已被清理或迁移。", "is-error");
      return;
    }
    if (status === 401 || status === 403) {
      stopImportJobPolling();
      setImportButtonBusy(true, "等待鉴权…");
      setFeedback("后台导入任务仍在执行，但当前缺少有效 Service API Key，恢复鉴权后会继续显示进度。", "is-error");
      return;
    }

    setFeedback(`后台导入任务状态同步失败：${error.message}`, "is-error");
    queueImportJobPoll(jobId, refreshOnFinish);
  } finally {
    state.importJobPolling = false;
  }
}

function resumeTrackedImportJob() {
  if (!state.importJobId || state.importJobPollTimer || state.importJobPolling) {
    return;
  }
  void pollImportJob(state.importJobId, { refreshOnFinish: false });
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

function deriveTtftTone(value) {
  if (value == null) {
    return "neutral";
  }
  const ms = Number(value);
  if (!Number.isFinite(ms)) {
    return "neutral";
  }
  if (ms <= 5000) {
    return "available";
  }
  if (ms <= 10000) {
    return "cooling";
  }
  return "disabled";
}

function renderTtftBadge(value) {
  const tone = deriveTtftTone(value);
  return `<span class="status-pill status-pill--${tone}">${escapeHtml(formatDurationMs(value))}</span>`;
}

function formatInteger(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "0";
  }
  return new Intl.NumberFormat("zh-CN").format(Math.round(number));
}

function formatCompactNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "0";
  }
  return new Intl.NumberFormat("zh-CN", {
    notation: "compact",
    maximumFractionDigits: number >= 1000 ? 1 : 0,
  }).format(number);
}

function formatUsd(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "—";
  }
  if (number === 0) {
    return "$0.00";
  }
  if (number >= 100) {
    return `$${number.toFixed(2)}`;
  }
  if (number >= 1) {
    return `$${number.toFixed(2)}`;
  }
  if (number >= 0.01) {
    return `$${number.toFixed(3)}`;
  }
  return `$${number.toFixed(4)}`;
}

function normalizeTokenSelectionStrategy(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replaceAll("-", "_");
  return normalized === "fill_first" ? "fill_first" : "least_recently_used";
}

function normalizeTokenActiveStreamCap(value, fallback = DEFAULT_TOKEN_ACTIVE_STREAM_CAP) {
  const fallbackCap = Number.isInteger(Number(fallback)) ? Number(fallback) : DEFAULT_TOKEN_ACTIVE_STREAM_CAP;
  const number = Number.parseInt(value, 10);
  const resolved = Number.isInteger(number) ? number : fallbackCap;
  return Math.min(TOKEN_ACTIVE_STREAM_CAP_MAX, Math.max(TOKEN_ACTIVE_STREAM_CAP_MIN, resolved));
}

function normalizeTokenSelectionOrder(value) {
  const seen = new Set();
  const order = [];
  if (!Array.isArray(value)) {
    return order;
  }
  value.forEach((item) => {
    const tokenId = Number(item);
    if (!Number.isInteger(tokenId) || tokenId <= 0 || seen.has(tokenId)) {
      return;
    }
    seen.add(tokenId);
    order.push(tokenId);
  });
  return order;
}

function normalizePlanType(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/^chatgpt_/, "");
  if (PLAN_TYPE_OPTIONS.includes(normalized)) {
    return normalized;
  }
  return "unknown";
}

function normalizeTokenPlanOrder(value) {
  const seen = new Set();
  const order = [];
  const source = Array.isArray(value) ? value : [];
  source.forEach((item) => {
    const planType = normalizePlanType(item);
    if (!PLAN_TYPE_OPTIONS.includes(planType) || seen.has(planType)) {
      return;
    }
    seen.add(planType);
    order.push(planType);
  });
  DEFAULT_PLAN_ORDER.forEach((planType) => {
    if (seen.has(planType)) {
      return;
    }
    order.push(planType);
  });
  return order;
}

function normalizeAvailablePlanFilter(value) {
  const planType = normalizePlanType(value);
  if (value === AVAILABLE_PLAN_FILTER_ALL || planType === AVAILABLE_PLAN_FILTER_ALL) {
    return AVAILABLE_PLAN_FILTER_ALL;
  }
  if (PLAN_TYPE_OPTIONS.includes(planType) || planType === "unknown") {
    return planType;
  }
  return AVAILABLE_PLAN_FILTER_ALL;
}

function normalizeTokenStatusFilter(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replaceAll("-", "_");
  return TOKEN_STATUS_FILTERS.has(normalized) ? normalized : DEFAULT_TOKEN_STATUS_FILTER;
}

function normalizeTokenViewMode(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replaceAll("-", "_");
  return TOKEN_VIEW_MODES.has(normalized) ? normalized : "keys";
}

function normalizeTokenSort(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return TOKEN_SORT_OPTIONS.has(normalized) ? normalized : DEFAULT_TOKEN_SORT;
}

function normalizePositiveInteger(value, fallback = 1) {
  const number = Number.parseInt(value, 10);
  return Number.isInteger(number) && number > 0 ? number : fallback;
}

function normalizeNonNegativeInteger(value, fallback = 0) {
  const number = Number.parseInt(value, 10);
  return Number.isInteger(number) && number >= 0 ? number : fallback;
}

function normalizeTokenPagination(raw = {}, itemCount = 0, fallbackPage = state.tokenPage) {
  const limit = Math.max(1, Math.min(normalizePositiveInteger(raw?.limit, state.tokenPageSize), 500));
  const total = normalizeNonNegativeInteger(raw?.total, Math.max(0, Number(itemCount) || 0));
  const offset = normalizeNonNegativeInteger(raw?.offset, (normalizePositiveInteger(fallbackPage, 1) - 1) * limit);
  const returned = normalizeNonNegativeInteger(raw?.returned, Math.max(0, Number(itemCount) || 0));
  const inferredTotalPages = Math.max(1, Math.ceil(total / limit));
  const totalPages = Math.max(1, normalizePositiveInteger(raw?.total_pages, inferredTotalPages));
  const inferredPage = Math.floor(offset / limit) + 1;
  const rawPage = normalizePositiveInteger(raw?.page, inferredPage);
  const page = total > 0 ? Math.min(Math.max(1, rawPage), totalPages) : 1;
  const firstItem = total > 0 && returned > 0 ? offset + 1 : 0;
  const lastItem = total > 0 && returned > 0 ? Math.min(offset + returned, total) : 0;

  return {
    total,
    limit,
    offset,
    returned,
    page,
    totalPages,
    hasPrevious: Boolean(raw?.has_previous ?? raw?.has_prev ?? page > 1),
    hasNext: Boolean(raw?.has_next ?? page < totalPages),
    firstItem,
    lastItem,
  };
}

function describeTokenSelectionStrategy(strategy) {
  if (normalizeTokenSelectionStrategy(strategy) === "fill_first") {
    return "首个可用优先";
  }
  return "最久未用优先";
}

function describeTokenConcurrency(cap = state.tokenActiveStreamCapDraft) {
  return `每 key ${formatInteger(normalizeTokenActiveStreamCap(cap))} 并发`;
}

function describeTokenPlanOrder(enabled = state.tokenPlanOrderEnabled, planOrder = state.tokenPlanOrder) {
  if (!enabled) {
    return "默认请求顺序";
  }
  return normalizeTokenPlanOrder(planOrder).map(formatPlanType).join(" → ");
}

function describeImportQueuePosition(position) {
  if (normalizeImportQueuePosition(position) === "back") {
    return "放在队列最后";
  }
  return "放在队列开头";
}

function renderImportQueuePosition(position) {
  const resolvedPosition = normalizeImportQueuePosition(position);
  state.importQueuePosition = resolvedPosition;
  elements.importQueuePositionButtons.forEach((button) => {
    const active = button.dataset.importQueuePosition === resolvedPosition;
    button.setAttribute("aria-pressed", String(active));
  });
  if (elements.importQueuePositionSummary) {
    elements.importQueuePositionSummary.textContent = describeImportQueuePosition(resolvedPosition);
  }
}

function setTokenSelectionDisabled(disabled) {
  elements.tokenSelectionButtons.forEach((button) => {
    button.disabled = disabled;
  });
}

function setTokenConcurrencyDisabled(disabled) {
  [
    elements.tokenConcurrencyInput,
    elements.tokenConcurrencyRange,
    elements.tokenConcurrencySave,
  ].forEach((control) => {
    if (!control) {
      return;
    }
    control.disabled = disabled;
  });
}

function setTokenPlanOrderDisabled(disabled) {
  elements.tokenPlanOrderButtons.forEach((button) => {
    button.disabled = disabled;
  });
  if (!elements.tokenPlanOrderList) {
    return;
  }
  elements.tokenPlanOrderList.querySelectorAll("button").forEach((button) => {
    button.disabled = disabled || !state.tokenPlanOrderEnabled;
  });
}

function setTokenSearchDisabled(disabled) {
  if (!elements.tokenSearchInput) {
    return;
  }
  elements.tokenSearchInput.disabled = disabled;
}

function setTokenPaginationDisabled(disabled) {
  [
    elements.tokenPageFirst,
    elements.tokenPagePrev,
    elements.tokenPageNext,
    elements.tokenPageLast,
    elements.tokenPageInput,
  ].forEach((control) => {
    if (!control) {
      return;
    }
    control.disabled = disabled;
  });
}

function renderLoadingState(message, className = "") {
  const classes = ["loading-state", className].filter(Boolean).join(" ");
  return `
    <article class="${classes}" role="status" aria-live="polite">
      <span class="loading-state__spinner" aria-hidden="true"></span>
      <p class="loading-state__label">${escapeHtml(message)}</p>
    </article>
  `;
}

function renderCountsLoading() {
  elements.availableCount.textContent = "—";
  elements.coolingCount.textContent = "—";
  elements.disabledCount.textContent = "—";
  elements.totalCount.textContent = "—";
  elements.legendAvailable.textContent = "—";
  elements.legendCooling.textContent = "—";
  elements.legendDisabled.textContent = "—";
  elements.barAvailable.style.width = "0%";
  elements.barCooling.style.width = "0%";
  elements.barDisabled.style.width = "0%";
}

function renderRequestSummaryUnavailable() {
  elements.requestTotalCount.textContent = "—";
  elements.requestTotalTokens.textContent = "—";
  elements.requestEstimatedCost.textContent = "—";
  elements.requestSuccessCount.textContent = "—";
  elements.requestFailedCount.textContent = "—";
  elements.requestAvgTtft.textContent = "—";
  elements.requestTokenChartTotal.textContent = "—";
  elements.requestCostChartTotal.textContent = "—";
}

function renderTokenListLoading() {
  renderTokenSearchSummary(0, 0, "等待载入");
  elements.tokenList.innerHTML = renderLoadingState("正在加载 key 池…");
}

function renderRequestListLoading() {
  elements.requestList.innerHTML = renderLoadingState("正在加载请求日志…");
}

function renderChartLoading(element, message) {
  element.innerHTML = renderLoadingState(message, "loading-state--chart");
}

function renderRequestAnalyticsLoading() {
  elements.requestTokenChartNote.textContent = "正在汇总最近 24 小时 token 用量…";
  elements.requestCostChartNote.textContent = "正在汇总最近 24 小时模型金额…";
  renderChartLoading(elements.requestTokenChart, "正在汇总 token 用量…");
  renderChartLoading(elements.requestCostChart, "正在汇总模型金额…");
}

function renderInitialLoadingStates() {
  if (!state.healthLoaded) {
    renderCountsLoading();
    elements.protectionChip.textContent = "正在检测服务状态";
    elements.syncChip.textContent = "正在首次同步";
  }
  if (!state.tokensLoaded) {
    elements.listNote.textContent = "正在载入 key 池…";
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent = "正在同步策略";
    }
    if (elements.tokenPlanOrderSummary) {
      elements.tokenPlanOrderSummary.textContent = "正在同步顺序";
    }
    if (elements.tokenConcurrencySummary) {
      elements.tokenConcurrencySummary.textContent = "正在同步上限";
    }
    setTokenSelectionDisabled(true);
    setTokenConcurrencyDisabled(true);
    setTokenPlanOrderDisabled(true);
    setTokenSearchDisabled(true);
    setTokenPaginationDisabled(true);
    renderTokenPagination("正在载入分页");
    renderTokenListLoading();
  }
  if (!state.requestsLoaded) {
    elements.requestNote.textContent = "正在载入最近 80 条请求…";
    renderRequestSummaryUnavailable();
    renderRequestAnalyticsLoading();
    renderRequestListLoading();
  }
}

function syncTokenConcurrencyControls(value, { updateDraft = true } = {}) {
  const cap = normalizeTokenActiveStreamCap(value, state.tokenActiveStreamCap);
  if (updateDraft) {
    state.tokenActiveStreamCapDraft = cap;
  }
  if (elements.tokenConcurrencyInput) {
    elements.tokenConcurrencyInput.value = String(cap);
  }
  if (elements.tokenConcurrencyRange) {
    elements.tokenConcurrencyRange.value = String(cap);
  }
  if (elements.tokenConcurrencySummary && !state.tokenActiveStreamCapSaving) {
    elements.tokenConcurrencySummary.textContent = describeTokenConcurrency(cap);
  }
}

function renderTokenConcurrency(selection) {
  const hasCap = Object.prototype.hasOwnProperty.call(selection || {}, "active_stream_cap");
  const cap = normalizeTokenActiveStreamCap(
    hasCap ? selection.active_stream_cap : state.tokenActiveStreamCap,
    state.tokenActiveStreamCap,
  );
  state.tokenActiveStreamCap = cap;
  state.tokenActiveStreamCapDraft = cap;
  syncTokenConcurrencyControls(cap);
}

function renderTokenSelection(selection) {
  const hasStrategy = Object.prototype.hasOwnProperty.call(selection || {}, "strategy");
  const strategy = hasStrategy ? normalizeTokenSelectionStrategy(selection?.strategy) : state.tokenSelectionStrategy;
  state.tokenSelectionStrategy = strategy;
  if (Array.isArray(selection?.token_order)) {
    state.tokenSelectionOrder = normalizeTokenSelectionOrder(selection.token_order);
  }
  if (Object.prototype.hasOwnProperty.call(selection || {}, "plan_order_enabled")) {
    state.tokenPlanOrderEnabled = Boolean(selection?.plan_order_enabled);
  }
  if (Array.isArray(selection?.plan_order)) {
    state.tokenPlanOrder = normalizeTokenPlanOrder(selection.plan_order);
  } else {
    state.tokenPlanOrder = normalizeTokenPlanOrder(state.tokenPlanOrder);
  }

  elements.tokenSelectionButtons.forEach((button) => {
    const active = button.dataset.selectionStrategy === strategy;
    button.setAttribute("aria-pressed", String(active));
  });
  elements.tokenPlanOrderButtons.forEach((button) => {
    const active = (button.dataset.planOrderEnabled === "true") === state.tokenPlanOrderEnabled;
    button.setAttribute("aria-pressed", String(active));
  });

  if (elements.tokenSelectionSummary) {
    elements.tokenSelectionSummary.textContent = describeTokenSelectionStrategy(strategy);
  }
  if (elements.tokenPlanOrderSummary) {
    elements.tokenPlanOrderSummary.textContent = describeTokenPlanOrder();
  }
  renderTokenConcurrency(selection || {});
  renderTokenPlanOrderList();
}

function describeTokenStatusFilter(status) {
  switch (normalizeTokenStatusFilter(status)) {
    case "available":
      return "可用";
    case "cooling":
      return "冷却";
    case "disabled":
      return "已禁用";
    default:
      return "全部";
  }
}

function renderTokenViewMode(mode = state.tokenViewMode) {
  state.tokenViewMode = normalizeTokenViewMode(mode);
  elements.tokenViewButtons.forEach((button) => {
    const active = normalizeTokenViewMode(button.dataset.tokenViewMode) === state.tokenViewMode;
    button.setAttribute("aria-pressed", String(active));
  });
  if (elements.tokenQueryBar) {
    elements.tokenQueryBar.hidden = state.tokenViewMode !== "keys";
  }
  if (elements.tokenPagination) {
    elements.tokenPagination.hidden = state.tokenViewMode !== "keys";
  }
}

function statusFilterCount(status, counts = state.tokenFilteredCounts) {
  if (status === "available") {
    return Number(counts?.available || 0);
  }
  if (status === "cooling") {
    return Number(counts?.cooling || 0);
  }
  if (status === "disabled") {
    return Number(counts?.disabled || 0);
  }
  return Number(counts?.total || 0);
}

function renderTokenStatusFilters(counts = state.tokenFilteredCounts) {
  if (!elements.tokenStatusFilters) {
    return;
  }
  const statuses = ["all", "available", "cooling", "disabled"];
  const activeStatus = normalizeTokenStatusFilter(state.tokenStatusFilter);
  elements.tokenStatusFilters.innerHTML = statuses
    .map(
      (status) => `
        <button
          class="token-filter-chip token-filter-chip--${escapeHtml(status)}"
          type="button"
          data-token-status-filter="${escapeHtml(status)}"
          aria-pressed="${activeStatus === status ? "true" : "false"}"
        >
          <span>${escapeHtml(describeTokenStatusFilter(status))}</span>
          <strong>${escapeHtml(formatInteger(statusFilterCount(status, counts)))}</strong>
        </button>
      `,
    )
    .join("");
}

function planFilterCount(planType, counts = state.tokenPlanCounts) {
  if (planType === AVAILABLE_PLAN_FILTER_ALL) {
    return PLAN_TYPE_OPTIONS.reduce((sum, item) => sum + Number(counts?.[item] || 0), 0) + Number(counts?.unknown || 0);
  }
  return Number(counts?.[planType] || 0);
}

function renderTokenPlanFilters(counts = state.tokenPlanCounts) {
  if (!elements.tokenPlanFilters) {
    return;
  }
  const filters = [AVAILABLE_PLAN_FILTER_ALL, ...PLAN_TYPE_OPTIONS];
  if (Number(counts?.unknown || 0) > 0 || state.tokenPlanFilter === TOKEN_PLAN_FILTER_UNKNOWN) {
    filters.push(TOKEN_PLAN_FILTER_UNKNOWN);
  }
  const activeFilter = normalizeAvailablePlanFilter(state.tokenPlanFilter);
  elements.tokenPlanFilters.innerHTML = filters
    .map(
      (planType) => `
        <button
          class="token-filter-chip"
          type="button"
          data-token-plan-filter="${escapeHtml(planType)}"
          aria-pressed="${activeFilter === planType ? "true" : "false"}"
        >
          <span>${escapeHtml(planType === AVAILABLE_PLAN_FILTER_ALL ? "全部计划" : formatPlanType(planType))}</span>
          <strong>${escapeHtml(formatInteger(planFilterCount(planType, counts)))}</strong>
        </button>
      `,
    )
    .join("");
}

function renderTokenSort() {
  if (!elements.tokenSortSelect) {
    return;
  }
  elements.tokenSortSelect.value = normalizeTokenSort(state.tokenSort);
}

function renderTokenQueryControls() {
  renderTokenViewMode(state.tokenViewMode);
  renderTokenStatusFilters();
  renderTokenPlanFilters();
  renderTokenSort();
}

function renderTokenPlanOrderList() {
  if (!elements.tokenPlanOrderList) {
    return;
  }
  const planOrder = normalizeTokenPlanOrder(state.tokenPlanOrder);
  const disabled = state.tokenPlanOrderSaving || !state.tokenPlanOrderEnabled;
  elements.tokenPlanOrderList.classList.toggle("plan-order-list--editable", state.tokenPlanOrderEnabled);
  elements.tokenPlanOrderList.innerHTML = planOrder
    .map((planType, index) => {
      const label = formatPlanType(planType);
      const draggable = state.tokenPlanOrderEnabled && !state.tokenPlanOrderSaving;
      return `
        <div
          class="plan-order-item${disabled ? " plan-order-item--disabled" : ""}"
          data-plan-order-item="${escapeHtml(planType)}"
          draggable="${draggable ? "true" : "false"}"
          role="listitem"
        >
          <span class="plan-order-item__rank">${index + 1}</span>
          <span class="plan-order-item__label">${escapeHtml(label)}</span>
          <span class="plan-order-item__actions" aria-label="${escapeHtml(label)} 顺序操作">
            <button
              class="plan-order-item__move"
              type="button"
              data-plan-order-move="up"
              data-plan-type="${escapeHtml(planType)}"
              aria-label="${escapeHtml(`${label} 上移`)}"
              ${disabled || index === 0 ? "disabled" : ""}
            >↑</button>
            <button
              class="plan-order-item__move"
              type="button"
              data-plan-order-move="down"
              data-plan-type="${escapeHtml(planType)}"
              aria-label="${escapeHtml(`${label} 下移`)}"
              ${disabled || index === planOrder.length - 1 ? "disabled" : ""}
            >↓</button>
          </span>
        </div>
      `;
    })
    .join("");
}

function normalizeTokenSearchQuery(value) {
  return normalizeWhitespace(value).toLowerCase();
}

function getTokenSearchTerms() {
  return normalizeTokenSearchQuery(state.tokenSearchTerm)
    .split(/\s+/)
    .filter(Boolean);
}

function buildTokenSearchIndex(item) {
  const status = deriveStatus(item);
  const workspaceAccountId = resolveWorkspaceAccountId(item);
  const primaryError = normalizeWhitespace(item?.last_error || "");
  const quotaError = normalizeWhitespace(item?.quota?.error || "");
  const fields = [
    item?.id == null ? "" : `token ${item.id}`,
    item?.id == null ? "" : `token #${item.id}`,
    normalizeWhitespace(item?.email || ""),
    workspaceAccountId,
    normalizeWhitespace(item?.source_file || ""),
    normalizeWhitespace(item?.plan_type || ""),
    formatPlanType(item?.plan_type),
    status.label,
    summarizeTokenError(primaryError),
    summarizeTokenError(quotaError),
    primaryError,
    quotaError,
  ];
  return fields
    .filter(Boolean)
    .join("\n")
    .toLowerCase();
}

function filterTokenItems(items) {
  const list = Array.isArray(items) ? items : [];
  const searchTerms = getTokenSearchTerms();
  if (!searchTerms.length) {
    return list;
  }
  return list.filter((item) => {
    const haystack = buildTokenSearchIndex(item);
    return searchTerms.every((term) => haystack.includes(term));
  });
}

function canDragAvailableTokens() {
  return (
    state.tokenViewMode === "keys" &&
    state.tokenSelectionStrategy === "fill_first" &&
    !state.tokenOrderSaving &&
    state.tokenStatusFilter === "available" &&
    state.tokenPlanFilter === AVAILABLE_PLAN_FILTER_ALL &&
    getTokenSearchTerms().length === 0
  );
}

function getTokenSelectionOrderRanks() {
  const ranks = new Map();
  state.tokenSelectionOrder.forEach((tokenId, index) => {
    ranks.set(Number(tokenId), index);
  });
  return ranks;
}

function sortAvailableTokenItems(items) {
  const list = Array.isArray(items) ? [...items] : [];
  const ranks = getTokenSelectionOrderRanks();
  const fallbackRank = ranks.size;
  const planRanks = new Map();
  if (state.tokenPlanOrderEnabled) {
    normalizeTokenPlanOrder(state.tokenPlanOrder).forEach((planType, index) => {
      planRanks.set(planType, index);
    });
  }
  return list.sort((left, right) => {
    if (state.tokenPlanOrderEnabled) {
      const leftPlanRank = planRanks.has(normalizePlanType(left?.plan_type))
        ? planRanks.get(normalizePlanType(left?.plan_type))
        : planRanks.size;
      const rightPlanRank = planRanks.has(normalizePlanType(right?.plan_type))
        ? planRanks.get(normalizePlanType(right?.plan_type))
        : planRanks.size;
      if (leftPlanRank !== rightPlanRank) {
        return leftPlanRank - rightPlanRank;
      }
    }
    if (state.tokenSelectionStrategy !== "fill_first") {
      return 0;
    }
    const leftId = Number(left?.id);
    const rightId = Number(right?.id);
    const leftRank = ranks.has(leftId) ? ranks.get(leftId) : fallbackRank;
    const rightRank = ranks.has(rightId) ? ranks.get(rightId) : fallbackRank;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return leftId - rightId;
  });
}

function mergeVisibleTokenOrder(visibleTokenIds) {
  const visibleOrder = normalizeTokenSelectionOrder(visibleTokenIds);
  const visibleIds = new Set(visibleOrder);
  return [
    ...visibleOrder,
    ...state.tokenSelectionOrder.filter((tokenId) => !visibleIds.has(Number(tokenId))),
  ];
}

function hasActiveTokenQuery() {
  return (
    getTokenSearchTerms().length > 0 ||
    normalizeTokenStatusFilter(state.tokenStatusFilter) !== DEFAULT_TOKEN_STATUS_FILTER ||
    normalizeAvailablePlanFilter(state.tokenPlanFilter) !== AVAILABLE_PLAN_FILTER_ALL
  );
}

function renderTokenSearchSummary(totalCount, visibleCount, customMessage = "") {
  if (!elements.tokenSearchSummary) {
    return;
  }
  if (customMessage) {
    elements.tokenSearchSummary.textContent = customMessage;
    return;
  }
  if (!hasActiveTokenQuery()) {
    elements.tokenSearchSummary.textContent = totalCount > 0 ? `全池 ${formatInteger(totalCount)} 条` : "等待导入或刷新";
    return;
  }
  elements.tokenSearchSummary.textContent = `匹配 ${formatInteger(visibleCount)} / ${formatInteger(totalCount)} 条`;
}

function buildTokenListNote() {
  const pagination = state.tokenPagination;
  const viewPrefix =
    state.selectedImportBatchId != null ? `导入批次 #${state.selectedImportBatchId}` : "Key Explorer";
  if (pagination.total > 0 && pagination.returned > 0) {
    return `${viewPrefix} · 每页 ${formatInteger(pagination.limit)} 条 · 当前 ${formatInteger(pagination.firstItem)}-${formatInteger(
      pagination.lastItem,
    )} / ${formatInteger(pagination.total)} · 搜索、筛选、排序均为全局结果`;
  }
  return TOKEN_LIST_BASE_NOTE;
}

function renderTokenPagination(customMessage = "") {
  if (!elements.tokenPagination) {
    return;
  }

  const pagination = state.tokenPagination;
  const page = pagination.page || 1;
  const totalPages = pagination.totalPages || 1;
  const hasTokens = pagination.total > 0;
  const disabled = state.refreshing || state.tokenPageLoading || !state.tokensLoaded || !hasTokens;
  const previousDisabled = disabled || !pagination.hasPrevious || page <= 1;
  const nextDisabled = disabled || !pagination.hasNext || page >= totalPages;

  if (elements.tokenPageFirst) {
    elements.tokenPageFirst.disabled = previousDisabled;
  }
  if (elements.tokenPagePrev) {
    elements.tokenPagePrev.disabled = previousDisabled;
  }
  if (elements.tokenPageNext) {
    elements.tokenPageNext.disabled = nextDisabled;
  }
  if (elements.tokenPageLast) {
    elements.tokenPageLast.disabled = nextDisabled;
  }
  if (elements.tokenPageInput) {
    elements.tokenPageInput.disabled = disabled;
    elements.tokenPageInput.min = "1";
    elements.tokenPageInput.max = String(totalPages);
    elements.tokenPageInput.value = String(page);
  }
  if (!elements.tokenPaginationSummary) {
    return;
  }
  if (customMessage) {
    elements.tokenPaginationSummary.textContent = customMessage;
    return;
  }
  if (!hasTokens) {
    elements.tokenPaginationSummary.textContent = state.tokensLoaded ? "暂无 key" : "等待载入";
    return;
  }
  elements.tokenPaginationSummary.textContent = `第 ${formatInteger(page)} / ${formatInteger(
    totalPages,
  )} 页 · ${formatInteger(pagination.firstItem)}-${formatInteger(pagination.lastItem)} / ${formatInteger(
    pagination.total,
  )}`;
}

function getTokenActionPendingKind(tokenId) {
  return state.tokenActionPendingKinds.get(Number(tokenId)) || "";
}

function isTokenActionPending(tokenId) {
  return state.tokenActionPendingKinds.has(Number(tokenId));
}

function clearTokenActionPending(tokenId) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId)) {
    return;
  }
  if (!state.tokenActionPendingKinds.delete(resolvedTokenId)) {
    return;
  }
  renderTokenList(state.tokenItems);
}

function renderTokenActionButtons(item) {
  const tokenId = Number(item?.id);
  const status = deriveStatus(item);
  const pendingKind = getTokenActionPendingKind(tokenId);
  const pending = Boolean(pendingKind);
  const probeLabel = pendingKind === "probe" ? "测试中" : "测试";
  const probeModel = normalizeProbeModel(state.probeModel);
  const probeModelOptions = PROBE_MODEL_OPTIONS.map(
    (model) =>
      `<option value="${escapeHtml(model)}"${model === probeModel ? " selected" : ""}>${escapeHtml(model)}</option>`,
  ).join("");
  const probeModelSelect = `
    <label class="token-probe-model">
      <span class="token-probe-model__label">测试模型</span>
      <select
        class="token-probe-model__select"
        data-token-probe-model="true"
        data-token-id="${tokenId}"
        aria-label="测试模型"
        ${pending ? "disabled" : ""}
      >
        ${probeModelOptions}
      </select>
    </label>
  `;
  const enableLabel = pendingKind === "enable" ? "处理中" : "启用";
  const makeAvailableLabel = pendingKind === "make-available" ? "处理中" : "转为可用";
  const disableLabel = pendingKind === "disable" ? "处理中" : "停用";
  const deleteLabel = pendingKind === "delete" ? "删除中" : "删除";
  const probeButton = `
    <button
      class="token-action-button token-action-button--probe"
      type="button"
      data-token-probe="true"
      data-token-id="${tokenId}"
      ${pending ? "disabled" : ""}
    >
      ${probeLabel}
    </button>
  `;
  const actionButtons = [];

  if (status.tone === "cooling") {
    actionButtons.push(`
      <button
        class="token-action-button token-action-button--enable"
        type="button"
        data-token-toggle="true"
        data-token-id="${tokenId}"
        data-next-active="true"
        data-clear-cooldown="true"
        data-pending-kind="make-available"
        ${pending ? "disabled" : ""}
      >
        ${makeAvailableLabel}
      </button>
    `);
    actionButtons.push(`
      <button
        class="token-action-button token-action-button--disable"
        type="button"
        data-token-toggle="true"
        data-token-id="${tokenId}"
        data-next-active="false"
        data-clear-cooldown="false"
        data-pending-kind="disable"
        ${pending ? "disabled" : ""}
      >
        ${disableLabel}
      </button>
    `);
  } else {
    const nextActive = !Boolean(item?.is_active);
    const tone = nextActive ? "enable" : "disable";
    const toggleLabel = nextActive ? enableLabel : disableLabel;
    actionButtons.push(`
      <button
        class="token-action-button token-action-button--${tone}"
        type="button"
        data-token-toggle="true"
        data-token-id="${tokenId}"
        data-next-active="${nextActive ? "true" : "false"}"
        data-clear-cooldown="${nextActive ? "true" : "false"}"
        data-pending-kind="${nextActive ? "enable" : "disable"}"
        ${pending ? "disabled" : ""}
      >
        ${toggleLabel}
      </button>
    `);
  }

  actionButtons.push(`
      <button
        class="token-action-button token-action-button--delete"
        type="button"
        data-token-delete="true"
        data-token-id="${tokenId}"
        ${pending ? "disabled" : ""}
      >
        ${deleteLabel}
      </button>
    `);

  return `
    <div class="token-card__actions">
      ${probeModelSelect}
      ${probeButton}
      ${actionButtons.join("")}
    </div>
  `;
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

function formatPercentValue(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "—";
  }
  if (number <= 0 || number >= 100) {
    return `${Math.round(number)}%`;
  }
  if (number >= 10) {
    return `${Math.round(number)}%`;
  }
  return `${number.toFixed(1)}%`;
}

function clampPercentNumber(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return 0;
  }
  return Math.max(0, Math.min(100, number));
}

function formatPlanType(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "未识别计划";
  }

  const labels = {
    free: "Free",
    plus: "Plus",
    team: "Team",
    pro: "Pro",
    business: "Business",
    enterprise: "Enterprise",
    edu: "Edu",
  };
  if (normalized === "unknown" || normalized === "unrecognized") {
    return "未识别计划";
  }
  return labels[normalized] || normalized.toUpperCase();
}

function derivePlanTone(value) {
  switch (String(value || "").trim().toLowerCase()) {
    case "plus":
    case "pro":
    case "team":
    case "business":
    case "enterprise":
      return "available";
    case "free":
      return "cooling";
    default:
      return "pending";
  }
}

function formatCooldownUntil(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  if (date.getTime() <= Date.now()) {
    return "—";
  }
  return formatDate(value);
}

function getQuotaWindow(item, windowId) {
  const windows = item?.quota?.windows;
  if (!Array.isArray(windows)) {
    return null;
  }
  return windows.find((window) => window?.id === windowId) || null;
}

function deriveQuotaTone(window) {
  const remaining = Number(window?.remaining_percent);
  if (!Number.isFinite(remaining)) {
    return window?.exhausted ? "disabled" : "pending";
  }
  if (remaining <= 0) {
    return "disabled";
  }
  if (remaining <= 30) {
    return "disabled";
  }
  if (remaining <= 70) {
    return "cooling";
  }
  return "available";
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function coerceHttpStatus(value) {
  const status = Number(value);
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : null;
}

function resolveWorkspaceAccountId(item) {
  return normalizeWhitespace(item?.chatgpt_account_id || item?.account_id || "");
}

function buildWorkspaceTokenCounts(items) {
  const counts = Object.create(null);
  if (!Array.isArray(items)) {
    return counts;
  }
  items.forEach((item) => {
    const workspaceAccountId = resolveWorkspaceAccountId(item);
    if (!workspaceAccountId) {
      return;
    }
    counts[workspaceAccountId] = (counts[workspaceAccountId] || 0) + 1;
  });
  return counts;
}

function buildTokenPresentation(item, workspaceTokenCounts) {
  const workspaceAccountId = resolveWorkspaceAccountId(item);
  const email = normalizeWhitespace(item?.email || "");
  const sourceFile = normalizeWhitespace(item?.source_file || "");
  const sharedWorkspaceCount = workspaceAccountId ? workspaceTokenCounts?.[workspaceAccountId] || 0 : 0;
  const hasSharedWorkspace = sharedWorkspaceCount > 1;
  const titleIncludesTokenId = (!email && hasSharedWorkspace && workspaceAccountId) || (!email && !workspaceAccountId);

  let account = email || workspaceAccountId || `Token #${item?.id ?? "—"}`;
  if (!email && hasSharedWorkspace && workspaceAccountId) {
    account = `${workspaceAccountId} · Token #${item.id}`;
  }

  const metaParts = [];
  if (email && workspaceAccountId) {
    metaParts.push(workspaceAccountId);
  }
  if (!titleIncludesTokenId) {
    metaParts.push(`Token #${item?.id ?? "—"}`);
  }
  if (hasSharedWorkspace) {
    metaParts.push(`同工作区共 ${sharedWorkspaceCount} 把`);
  }
  if (sourceFile) {
    metaParts.push(sourceFile);
  } else {
    metaParts.push("无来源记录");
  }

  return {
    account,
    meta: metaParts.join(" · "),
    workspaceAccountId,
    sharedWorkspaceCount,
    hasSharedWorkspace,
  };
}

function truncateText(value, maxLength = 160) {
  const text = normalizeWhitespace(value);
  if (!text || text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
}

function looksLikeHtmlError(value, contentType = "") {
  const text = String(value || "").trim();
  if (!text) {
    return false;
  }
  if (String(contentType || "").toLowerCase().includes("text/html")) {
    return true;
  }
  return (
    /^<!doctype\s+html\b/i.test(text) ||
    /^<html[\s>]/i.test(text) ||
    /<head[\s>]/i.test(text) ||
    /<body[\s>]/i.test(text) ||
    /<title[\s>][\s\S]*<\/title>/i.test(text)
  );
}

function extractHtmlTitle(value) {
  const raw = String(value || "");
  if (!raw) {
    return "";
  }
  try {
    const doc = new DOMParser().parseFromString(raw, "text/html");
    const title = normalizeWhitespace(doc.querySelector("title")?.textContent || "");
    if (title) {
      return title;
    }
  } catch {
    // DOMParser is available in browsers; the regex fallback keeps tests and older runtimes safe.
  }
  const titleMatch = raw.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return normalizeWhitespace(titleMatch?.[1] || "");
}

function stripHtmlErrorTitleNoise(value) {
  return normalizeWhitespace(value).replace(/^[^|]{1,120}\|\s*/, "");
}

function inferHtmlErrorStatus(value, fallbackStatus = null) {
  const status = coerceHttpStatus(fallbackStatus);
  if (status) {
    return status;
  }
  const titleStatus = extractHtmlTitle(value).match(/\b(5\d{2})\b/);
  if (titleStatus) {
    return coerceHttpStatus(titleStatus[1]);
  }
  const explicitStatus = normalizeWhitespace(value).match(/\bError(?:\s+code)?\s+(5\d{2})\b/i);
  return explicitStatus ? coerceHttpStatus(explicitStatus[1]) : null;
}

function cloudflareErrorLabel(status) {
  const labels = {
    520: "Cloudflare 返回未知错误，请稍后重试",
    521: "源站拒绝连接，请稍后重试",
    522: "源站连接超时，请稍后重试",
    523: "源站不可达，请稍后重试",
    524: "源站响应超时，请稍后重试",
    525: "SSL 握手失败，请检查源站证书",
    526: "SSL 证书无效，请检查源站证书",
    527: "边缘服务执行失败，请稍后重试",
  };
  return labels[status] || "";
}

function summarizeHtmlError(value, { status = null, contentType = "" } = {}) {
  if (!looksLikeHtmlError(value, contentType)) {
    return "";
  }

  const resolvedStatus = inferHtmlErrorStatus(value, status);
  const statusLabel = resolvedStatus ? `HTTP ${resolvedStatus}` : "请求失败";
  const cloudflareLabel = cloudflareErrorLabel(resolvedStatus);
  if (cloudflareLabel) {
    return `${statusLabel} · ${cloudflareLabel}`;
  }

  const title = stripHtmlErrorTitleNoise(extractHtmlTitle(value));
  if (title) {
    return truncateText(`${statusLabel} · ${title}`, 180);
  }
  if (resolvedStatus && resolvedStatus >= 500) {
    return `${statusLabel} · 服务暂时不可用，请稍后重试`;
  }
  return `${statusLabel} · 服务返回了 HTML 错误页，请稍后重试`;
}

function extractErrorPayload(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.error && typeof parsed.error === "object") {
      return { raw, data: parsed.error };
    }
    if (parsed?.detail && typeof parsed.detail === "object") {
      return { raw, data: parsed.detail };
    }
    return { raw, data: parsed };
  } catch {
    return { raw, data: null };
  }
}

function humanizeErrorType(value) {
  const type = normalizeWhitespace(value).toLowerCase();
  const labels = {
    usage_limit_reached: "额度限制",
    invalid_request_error: "请求无效",
    rate_limit_exceeded: "速率限制",
    account_deactivated: "账号停用",
    deactivated_workspace: "工作区停用",
    auth_error: "鉴权失败",
  };
  return labels[type] || "";
}

function summarizeTokenError(value) {
  const payload = extractErrorPayload(value);
  if (!payload) {
    return "";
  }

  const raw = payload.raw;
  const rawHtmlSummary = summarizeHtmlError(raw);
  if (rawHtmlSummary) {
    return rawHtmlSummary;
  }

  const source = payload.data && typeof payload.data === "object" ? payload.data : {};
  const type = normalizeWhitespace(source.type || source.code || "");
  const message = normalizeWhitespace(source.message || source.detail || raw);
  const messageHtmlSummary = summarizeHtmlError(message);
  if (messageHtmlSummary) {
    return messageHtmlSummary;
  }

  const haystack = `${type} ${message}`.toLowerCase();

  if (type === "usage_limit_reached" || haystack.includes("usage limit")) {
    return "额度已用尽，等待窗口重置";
  }
  if (haystack.includes("refresh token") && (haystack.includes("signing in again") || haystack.includes("invalid"))) {
    return "Refresh token 已失效，需要重新登录";
  }
  if (haystack.includes("already been used to generate a new access token")) {
    return "Refresh token 已失效，需要重新登录";
  }
  if (haystack.includes("account_deactivated") || haystack.includes("deactivated_workspace")) {
    return "账号已停用，不能继续请求";
  }
  if (haystack.includes("401") && haystack.includes("codex token refresh failed")) {
    return "Codex 刷新失败，请重新登录账号";
  }

  const typeLabel = humanizeErrorType(type);
  if (typeLabel && message && !message.toLowerCase().includes(type.toLowerCase())) {
    return truncateText(`${typeLabel} · ${message}`, 148);
  }
  if (message && message !== raw) {
    return truncateText(message, 148);
  }
  if (typeLabel) {
    return typeLabel;
  }
  return truncateText(raw, 148);
}

function renderQuotaWindow(window, fallbackLabel) {
  const label = window?.label || fallbackLabel;
  const remainingLabel = formatPercentValue(window?.remaining_percent);
  const resetLabel = formatDate(window?.reset_at);
  const tone = deriveQuotaTone(window);
  const fillWidth = clampPercentNumber(window?.remaining_percent);

  if (!window) {
    return `
      <div class="quota-band quota-band--pending">
        <div class="quota-band__head">
          <div class="quota-band__title">
            <span class="quota-band__label">${escapeHtml(label)}</span>
            <span class="quota-band__meta">未返回窗口</span>
          </div>
          <span class="quota-band__value">—</span>
        </div>
        <div class="quota-band__track"><span class="quota-band__fill" style="width: 0%"></span></div>
      </div>
    `;
  }

  let metaText = "等待窗口";
  if (resetLabel !== "—") {
    metaText = `重置 ${resetLabel}`;
  } else if (window.exhausted) {
    metaText = "已耗尽";
  }

  return `
    <div class="quota-band quota-band--${tone}">
      <div class="quota-band__head">
        <div class="quota-band__title">
          <span class="quota-band__label">${escapeHtml(label)}</span>
          <span class="quota-band__meta">${escapeHtml(metaText)}</span>
        </div>
        <span class="quota-band__value">${escapeHtml(remainingLabel)}</span>
      </div>
      <div class="quota-band__track"><span class="quota-band__fill" style="width: ${fillWidth}%"></span></div>
    </div>
  `;
}

function renderQuotaSection(item, presentation) {
  const quota = item.quota;
  const notes = [];
  if (presentation?.hasSharedWorkspace) {
    notes.push(
      `同工作区共 ${presentation.sharedWorkspaceCount} 把 token；这里只表示它们共享工作区标识，5H / 7D 配额窗口仍按每把 token 实时读取，可能不同。`,
    );
  }
  if (quota?.error) {
    notes.push(
      `配额读取失败 · ${summarizeTokenError(quota.error) || "请稍后再试"}${
        quota?.fetched_at ? ` · 最近尝试 ${formatDate(quota.fetched_at)}` : ""
      }`,
    );
  }
  return `
    <div class="token-card__quota">
      <div class="token-card__quota-grid">
        ${renderQuotaWindow(getQuotaWindow(item, "code-5h"), "5h")}
        ${renderQuotaWindow(getQuotaWindow(item, "code-7d"), "7d")}
      </div>
      ${notes.map((note) => `<p class="token-card__quota-note">${escapeHtml(note)}</p>`).join("")}
    </div>
  `;
}

function renderTokenErrorSection(item) {
  const primaryRaw = normalizeWhitespace(item?.last_error);
  const quotaRaw = normalizeWhitespace(item?.quota?.error);
  if (!primaryRaw && !quotaRaw) {
    return "";
  }

  const primarySummary = primaryRaw ? summarizeTokenError(primaryRaw) : "";
  const quotaSummary = quotaRaw ? summarizeTokenError(quotaRaw) : "";
  const label = primaryRaw ? "最近错误" : "配额读取";
  const message = primarySummary || quotaSummary;
  const secondary =
    primaryRaw && quotaSummary && quotaSummary !== primarySummary ? `配额读取 · ${quotaSummary}` : "";
  const rawTitle = primaryRaw || quotaRaw;

  return `
    <div class="token-card__error" title="${escapeHtml(rawTitle)}">
      <span class="token-card__error-label">${label}</span>
      <p class="token-card__error-message">${escapeHtml(message || "请求失败，请稍后再试")}</p>
      ${secondary ? `<p class="token-card__error-meta">${escapeHtml(secondary)}</p>` : ""}
    </div>
  `;
}

function resolveTokenDetailsOpenState(tokenId, fallbackOpen = false) {
  const key = String(tokenId);
  return Object.prototype.hasOwnProperty.call(state.tokenDetailsOpenStates, key)
    ? Boolean(state.tokenDetailsOpenStates[key])
    : Boolean(fallbackOpen);
}

function renderQuotaPreview(item) {
  const window5h = getQuotaWindow(item, "code-5h");
  const window7d = getQuotaWindow(item, "code-7d");
  const formatPreview = (window, label) => `${label} ${formatPercentValue(window?.remaining_percent)}`;
  return `
    <div class="token-result__quota-preview" data-label="配额">
      <span>${escapeHtml(formatPreview(window5h, "5h"))}</span>
      <span>${escapeHtml(formatPreview(window7d, "7d"))}</span>
    </div>
  `;
}

function renderTokenResultRow(item, workspaceTokenCounts) {
  const status = deriveStatus(item);
  const tokenId = Number(item?.id);
  const draggable = status.tone === "available" && canDragAvailableTokens();
  const presentation = buildTokenPresentation(item, workspaceTokenCounts);
  const planLabel = formatPlanType(item.plan_type);
  const planTone = derivePlanTone(item.plan_type);
  const cooldownValue = formatCooldownUntil(item.cooldown_until);
  const lastUsedValue = formatDate(item.last_used_at);
  const activeStreams = Math.max(0, Number(item.active_streams || 0));
  const activeStreamCap = normalizeTokenActiveStreamCap(state.tokenActiveStreamCap, item.active_stream_cap);
  const activeStreamsValue = `${formatInteger(activeStreams)}/${formatInteger(activeStreamCap)}`;
  const observedCostValue = item.observed_cost_usd == null ? "—" : formatUsd(item.observed_cost_usd);
  const errorSection = renderTokenErrorSection(item);
  const open = resolveTokenDetailsOpenState(tokenId, false);

  return `
    <details
      class="token-result token-result--${status.tone}${draggable ? " token-result--draggable" : ""}"
      data-token-card-id="${Number.isFinite(tokenId) ? tokenId : ""}"
      data-token-draggable="${draggable ? "true" : "false"}"
      draggable="${draggable ? "true" : "false"}"
      aria-grabbed="false"
      ${open ? "open" : ""}
    >
      <summary class="token-result__summary">
        <span class="token-result__identity">
          <span class="token-result__account">${escapeHtml(presentation.account)}</span>
          <span class="token-result__meta">${escapeHtml(presentation.meta)}</span>
        </span>
        <span class="token-result__status" data-label="状态">
          <span class="status-pill status-pill--${status.tone}">${status.label}</span>
          <span class="status-pill status-pill--${planTone}">${escapeHtml(planLabel)}</span>
        </span>
        ${renderQuotaPreview(item)}
        <span class="token-result__lifecycle" data-label="生命周期">
          <span>冷却 ${escapeHtml(cooldownValue)}</span>
          <span>最近 ${escapeHtml(lastUsedValue)}</span>
        </span>
        <span class="token-result__usage" data-label="并发/金额">
          <span>${escapeHtml(activeStreamsValue)}</span>
          <span>${escapeHtml(observedCostValue)}</span>
        </span>
        <span class="token-result__expand" aria-hidden="true">详情</span>
      </summary>
      <div class="token-result__details">
        <div class="token-result__detail-grid">
          ${renderQuotaSection(item, presentation)}
          ${errorSection || `<div class="token-result__quiet-note">当前没有最近错误。</div>`}
        </div>
        ${renderTokenActionButtons(item)}
      </div>
    </details>
  `;
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
  const statusCode = Number(item.status_code);
  if (Number.isFinite(statusCode) && statusCode > 0) {
    if (statusCode >= 200 && statusCode < 300 && item.success !== false) {
      return { label: String(statusCode), tone: "success" };
    }
    return { label: String(statusCode), tone: "failed" };
  }
  if (item.success === true) {
    return { label: "OK", tone: "success" };
  }
  if (!item.finished_at) {
    return { label: "处理中", tone: "pending" };
  }
  return { label: "ERR", tone: "failed" };
}

function renderRequestSummary(summary) {
  const total = summary?.total || 0;
  const totalTokens = summary?.total_tokens || 0;
  const estimatedCost = summary?.estimated_cost_usd || 0;
  const successful = summary?.successful || 0;
  const failed = summary?.failed || 0;
  const avgTtft = summary?.avg_ttft_ms;

  elements.requestTotalCount.textContent = total;
  elements.requestTotalTokens.textContent = formatCompactNumber(totalTokens);
  elements.requestEstimatedCost.textContent = formatUsd(estimatedCost);
  elements.requestSuccessCount.textContent = successful;
  elements.requestFailedCount.textContent = failed;
  elements.requestAvgTtft.textContent = formatDurationMs(avgTtft);
}

function renderChartEmpty(element, message) {
  element.innerHTML = `
    <article class="empty-state chart-empty">
      <p>${escapeHtml(message)}</p>
    </article>
  `;
}

function formatBucketLabel(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function truncateLabel(value, maxLength = 16) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1)}…`;
}

function renderRequestTokenChart(analytics) {
  const buckets = Array.isArray(analytics?.buckets) ? analytics.buckets : [];
  const periodHours = Number(analytics?.period_hours) || 24;
  const totalTokens = buckets.reduce((sum, bucket) => sum + (Number(bucket?.total_tokens) || 0), 0);

  elements.requestTokenChartTotal.textContent = formatCompactNumber(totalTokens);
  elements.requestTokenChartNote.textContent = `最近 ${periodHours} 小时按小时汇总输入与输出 token。`;

  if (!buckets.length || totalTokens <= 0) {
    renderChartEmpty(elements.requestTokenChart, "最近 24 小时还没有 token 用量数据。");
    return;
  }

  const width = 680;
  const height = 224;
  const padding = { top: 14, right: 12, bottom: 34, left: 12 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const maxTotal = Math.max(...buckets.map((bucket) => Number(bucket?.total_tokens) || 0), 1);
  const slotWidth = plotWidth / buckets.length;
  const barWidth = Math.max(4, slotWidth * 0.64);
  const labelStep = Math.max(1, Math.ceil(buckets.length / 6));
  const gridLines = 4;

  const gridMarkup = Array.from({ length: gridLines }, (_, index) => {
    const y = padding.top + (plotHeight / (gridLines - 1)) * index;
    return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="chart-grid-line" />`;
  }).join("");

  const barsMarkup = buckets
    .map((bucket, index) => {
      const inputTokens = Math.max(0, Number(bucket?.input_tokens) || 0);
      const outputTokens = Math.max(0, Number(bucket?.output_tokens) || 0);
      const total = Math.max(0, Number(bucket?.total_tokens) || 0);
      const x = padding.left + index * slotWidth + (slotWidth - barWidth) / 2;
      const totalHeight = total > 0 ? (total / maxTotal) * plotHeight : 0;
      const inputHeight = total > 0 ? (inputTokens / maxTotal) * plotHeight : 0;
      const outputHeight = totalHeight - inputHeight;
      const baseY = padding.top + plotHeight;
      const outputY = baseY - outputHeight;
      const inputY = outputY - inputHeight;
      const label = formatBucketLabel(bucket?.bucket_start);

      return `
        <g class="chart-bar-group">
          <title>${escapeHtml(`${label} · 输入 ${formatInteger(inputTokens)} · 输出 ${formatInteger(outputTokens)} · 总计 ${formatInteger(total)}`)}</title>
          ${
            inputHeight > 0
              ? `<rect x="${x}" y="${inputY}" width="${barWidth}" height="${inputHeight}" rx="5" class="chart-bar chart-bar--input" />`
              : ""
          }
          ${
            outputHeight > 0
              ? `<rect x="${x}" y="${outputY}" width="${barWidth}" height="${outputHeight}" rx="5" class="chart-bar chart-bar--output" />`
              : ""
          }
          ${
            index % labelStep === 0
              ? `<text x="${x + barWidth / 2}" y="${height - 10}" text-anchor="middle" class="chart-axis-label">${escapeHtml(label)}</text>`
              : ""
          }
        </g>
      `;
    })
    .join("");

  elements.requestTokenChart.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="最近 24 小时 token 趋势图">
      ${gridMarkup}
      ${barsMarkup}
    </svg>
  `;
}

function renderRequestCostChart(analytics) {
  const models = Array.isArray(analytics?.models) ? analytics.models : [];
  const periodHours = Number(analytics?.period_hours) || 24;
  const totalCost = models.reduce((sum, item) => sum + (Number(item?.estimated_cost_usd) || 0), 0);
  const maxCost = Math.max(...models.map((item) => Number(item?.estimated_cost_usd) || 0), 0);

  elements.requestCostChartTotal.textContent = formatUsd(totalCost);
  elements.requestCostChartNote.textContent =
    maxCost > 0
      ? `最近 ${periodHours} 小时按模型汇总估算金额。`
      : `最近 ${periodHours} 小时暂无可估算金额，可能模型未命中单价表。`;

  if (!models.length) {
    renderChartEmpty(elements.requestCostChart, "最近 24 小时还没有模型金额分布数据。");
    return;
  }

  const width = 680;
  const rowHeight = 38;
  const height = Math.max(220, 44 + models.length * rowHeight);
  const padding = { top: 16, right: 12, bottom: 18, left: 196 };
  const plotWidth = width - padding.left - padding.right;
  const resolvedMaxCost = Math.max(maxCost, 0.0001);

  const rowsMarkup = models
    .map((item, index) => {
      const modelName = item?.model_name || "未识别模型";
      const visibleLabel = truncateLabel(modelName, 18);
      const cost = Math.max(0, Number(item?.estimated_cost_usd) || 0);
      const totalTokens = Math.max(0, Number(item?.total_tokens) || 0);
      const y = padding.top + index * rowHeight;
      const barY = y + 12;
      const barHeight = 10;
      const barWidth = (cost / resolvedMaxCost) * plotWidth;
      const textY = y + 8;

      return `
        <g class="chart-model-row">
          <title>${escapeHtml(`${modelName} · ${formatUsd(cost)} · ${formatInteger(totalTokens)} tokens`)}</title>
          <text x="${padding.left - 12}" y="${textY}" text-anchor="end" class="chart-model-label">${escapeHtml(visibleLabel)}</text>
          <rect x="${padding.left}" y="${barY}" width="${plotWidth}" height="${barHeight}" rx="999" class="chart-track" />
          ${barWidth > 0 ? `<rect x="${padding.left}" y="${barY}" width="${barWidth}" height="${barHeight}" rx="999" class="chart-bar chart-bar--cost" />` : ""}
          <text x="${width - padding.right}" y="${textY}" text-anchor="end" class="chart-model-value">${escapeHtml(formatUsd(cost))}</text>
          <text x="${padding.left - 12}" y="${y + 26}" text-anchor="end" class="chart-model-meta">${escapeHtml(`${formatCompactNumber(totalTokens)} tokens`)}</text>
        </g>
      `;
    })
    .join("");

  elements.requestCostChart.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="最近 24 小时模型金额分布图">
      ${rowsMarkup}
    </svg>
  `;
}

function renderRequestAnalytics(analytics) {
  renderRequestTokenChart(analytics);
  renderRequestCostChart(analytics);
}

function buildImportBatchCounts(batch) {
  return {
    available: Math.max(0, Number(batch?.available || 0)),
    cooling: Math.max(0, Number(batch?.cooling || 0)),
    disabled: Math.max(0, Number(batch?.disabled || 0)),
    missing: Math.max(0, Number(batch?.missing || 0)),
    tokenCount: Math.max(0, Number(batch?.token_count || 0)),
    totalCount: Math.max(0, Number(batch?.total_count || 0)),
    processedCount: Math.max(0, Number(batch?.processed_count || 0)),
    failedCount: Math.max(0, Number(batch?.failed_count || 0)),
  };
}

function formatImportBatchTitle(batch) {
  const batchId = Number(batch?.id);
  return Number.isInteger(batchId) && batchId > 0 ? `导入批次 #${batchId}` : "导入批次";
}

function formatImportBatchStatus(batch) {
  const status = normalizeWhitespace(batch?.status || "");
  const labels = {
    queued: "排队中",
    running: "导入中",
    completed: "已完成",
    failed: "失败",
  };
  return labels[status] || status || "未知状态";
}

function renderImportBatchList() {
  const batches = Array.isArray(state.tokenImportBatches) ? [...state.tokenImportBatches].sort(compareImportBatches) : [];
  if (!batches.length) {
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>还没有可展示的导入批次。新的后台导入任务完成后会显示每批当前状态数量。</p>
      </article>
    `;
    return;
  }

  elements.tokenList.innerHTML = `
    <div class="import-batch-list" role="list" aria-label="导入批次">
      ${batches
        .map((batch) => {
          const counts = buildImportBatchCounts(batch);
          const batchId = Number(batch?.id);
          const selected = state.selectedImportBatchId === batchId;
          return `
            <article class="import-batch-card${selected ? " import-batch-card--selected" : ""}" role="listitem">
              <div class="import-batch-card__main">
                <div>
                  <h3>${escapeHtml(formatImportBatchTitle(batch))}</h3>
                  <p>${escapeHtml(formatImportBatchStatus(batch))} · 提交 ${escapeHtml(formatDate(batch?.submitted_at))}</p>
                </div>
                <button
                  class="ghost-button import-batch-card__button"
                  type="button"
                  data-import-batch-view="${escapeHtml(batchId)}"
                >
                  查看 Key
                </button>
              </div>
              <div class="import-batch-card__metrics" aria-label="批次结果">
                <span><strong>${escapeHtml(formatInteger(counts.totalCount))}</strong><small>提交</small></span>
                <span><strong>${escapeHtml(formatInteger(counts.processedCount))}</strong><small>已处理</small></span>
                <span><strong>${escapeHtml(formatInteger(counts.available))}</strong><small>可用</small></span>
                <span><strong>${escapeHtml(formatInteger(counts.cooling))}</strong><small>冷却</small></span>
                <span><strong>${escapeHtml(formatInteger(counts.disabled))}</strong><small>禁用</small></span>
                <span><strong>${escapeHtml(formatInteger(counts.failedCount))}</strong><small>失败</small></span>
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function compareImportBatches(left, right) {
  const leftTime = new Date(left?.submitted_at || 0).getTime();
  const rightTime = new Date(right?.submitted_at || 0).getTime();
  if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
    return rightTime - leftTime;
  }
  return Number(right?.id || 0) - Number(left?.id || 0);
}

function renderTokenList(items) {
  const list = Array.isArray(items) ? items : [];
  const totalCount = Number(state.tokenPagination.total || list.length || 0);
  renderTokenSearchSummary(Number(state.tokenFilteredCounts?.total || totalCount), totalCount);

  if (state.tokenViewMode === "import_batches") {
    renderImportBatchList();
    return;
  }

  if (list.length === 0) {
    const queryParts = [
      normalizeWhitespace(state.tokenSearchTerm) ? `搜索“${normalizeWhitespace(state.tokenSearchTerm)}”` : "",
      state.tokenStatusFilter !== DEFAULT_TOKEN_STATUS_FILTER ? describeTokenStatusFilter(state.tokenStatusFilter) : "",
      state.tokenPlanFilter !== AVAILABLE_PLAN_FILTER_ALL ? `${formatPlanType(state.tokenPlanFilter)} 计划` : "",
    ].filter(Boolean);
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(queryParts.length ? `没有匹配 ${queryParts.join(" · ")} 的 key。` : "没有 key 记录。先导入一批 key，列表会在这里展开。")}</p>
      </article>
    `;
    return;
  }

  const workspaceTokenCounts = buildWorkspaceTokenCounts(list);
  elements.tokenList.innerHTML = `
    <div class="token-result-list" role="list" aria-label="Key 查询结果">
      ${list.map((item) => renderTokenResultRow(item, workspaceTokenCounts)).join("")}
    </div>
  `;
}

function getTokenItemById(tokenId) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId)) {
    return null;
  }
  return state.tokenItems.find((item) => Number(item?.id) === resolvedTokenId) || null;
}

function buildTokenDeleteContext(item) {
  const workspaceTokenCounts = buildWorkspaceTokenCounts(state.tokenItems);
  const presentation = buildTokenPresentation(item, workspaceTokenCounts);
  const status = deriveStatus(item);
  const tokenLabel = `Token #${item.id}`;
  const noteParts = [
    "删除后会从调度池和最近列表移除",
    "如果这把 key 合并过历史 refresh token，关联历史记录也会一起删除",
    presentation.hasSharedWorkspace ? "同工作区的其它 token 不会受影响" : "",
    "此操作不可撤销",
  ].filter(Boolean);
  const metaParts = [status.label];
  if (!presentation.meta || !presentation.meta.includes(tokenLabel)) {
    metaParts.unshift(tokenLabel);
  }
  if (presentation.meta) {
    metaParts.push(presentation.meta);
  }

  return {
    title: presentation.account,
    description: noteParts.join(" · "),
    meta: metaParts.filter(Boolean).join(" · "),
  };
}

function clearTokenDeleteFeedback() {
  if (!elements.tokenDeleteFeedback) {
    return;
  }
  elements.tokenDeleteFeedback.textContent = "";
  elements.tokenDeleteFeedback.classList.remove("is-error");
}

function setTokenDeleteFeedback(message, tone = "") {
  if (!elements.tokenDeleteFeedback) {
    return;
  }
  elements.tokenDeleteFeedback.textContent = message;
  elements.tokenDeleteFeedback.classList.toggle("is-error", tone === "error");
}

function setTokenDeleteDialogBusy(busy) {
  const disabled = Boolean(busy);
  if (elements.tokenDeleteCancel) {
    elements.tokenDeleteCancel.disabled = disabled;
  }
  if (elements.tokenDeleteClose) {
    elements.tokenDeleteClose.disabled = disabled;
  }
  if (elements.tokenDeleteConfirm) {
    elements.tokenDeleteConfirm.disabled = disabled || !Number.isFinite(Number(state.tokenDeleteTargetId));
    elements.tokenDeleteConfirm.textContent = disabled ? "删除中…" : "删除 Key";
  }
}

function closeTokenDeleteDialog({ force = false } = {}) {
  const dialog = elements.tokenDeleteDialog;
  if (!supportsTokenDeleteDialog) {
    state.tokenDeleteTargetId = null;
    return;
  }
  if (!force && state.tokenDeleteTargetId != null && getTokenActionPendingKind(state.tokenDeleteTargetId) === "delete") {
    return;
  }
  if (dialog.open) {
    dialog.close();
    return;
  }
  state.tokenDeleteTargetId = null;
  setTokenDeleteDialogBusy(false);
  clearTokenDeleteFeedback();
}

function openTokenDeleteDialog(tokenId) {
  const item = getTokenItemById(tokenId);
  if (!item) {
    return;
  }

  const context = buildTokenDeleteContext(item);
  state.tokenDeleteTargetId = Number(item.id);

  if (elements.tokenDeleteTarget) {
    elements.tokenDeleteTarget.textContent = context.title;
  }
  if (elements.tokenDeleteDescription) {
    elements.tokenDeleteDescription.textContent = context.description;
  }
  if (elements.tokenDeleteMeta) {
    elements.tokenDeleteMeta.textContent = context.meta;
  }
  clearTokenDeleteFeedback();
  setTokenDeleteDialogBusy(false);

  const dialog = elements.tokenDeleteDialog;
  if (!supportsTokenDeleteDialog || typeof dialog.showModal !== "function") {
    if (window.confirm(`删除 ${context.title}？\n${context.description}`)) {
      void deleteToken(item.id);
      return;
    }
    state.tokenDeleteTargetId = null;
    return;
  }

  if (!dialog.open) {
    dialog.showModal();
  }
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
      const mode = item.is_stream ? "Stream" : "JSON";
      const attemptLabel = item.attempt_count ? `${item.attempt_count} 次` : "—";
      const endpoint = item.endpoint || "—";
      const startedAt = item.started_at || "";
      const rawErrorMessage = item.error_message || "";
      const errorMessage = rawErrorMessage ? summarizeTokenError(rawErrorMessage) || truncateText(rawErrorMessage, 180) : "—";
      const errorTitle = rawErrorMessage ? truncateText(rawErrorMessage, 500) : "—";
      const requestedModel = item.model || "";
      const modelName = item.model_name || requestedModel || "未带 model";
      const requestedModelMeta =
        requestedModel && requestedModel !== modelName ? `<span class="request-row__meta">${escapeHtml(`请求: ${requestedModel}`)}</span>` : "";
      return `
        <article class="request-row">
          <div class="request-row__primary">
            <span class="request-row__title" title="${escapeHtml(startedAt)}">${escapeHtml(formatDate(item.started_at))}</span>
            <span class="request-row__meta">${escapeHtml(mode)}</span>
          </div>
          <div class="request-row__model" data-label="模型名" title="${escapeHtml(modelName)}">
            <span class="request-row__model-name">${escapeHtml(modelName)}</span>
            ${requestedModelMeta}
          </div>
          <div class="request-row__endpoint" data-label="端点" title="${escapeHtml(endpoint)}">${escapeHtml(endpoint)}</div>
          <div class="request-row__status" data-label="状态">
            <span class="status-pill status-pill--${status.tone}">${escapeHtml(status.label)}</span>
          </div>
          <div class="request-row__time" data-label="首字时间">${renderTtftBadge(item.ttft_ms)}</div>
          <div class="request-row__time" data-label="尝试">${escapeHtml(attemptLabel)}</div>
          <div class="request-row__error" data-label="错误" title="${escapeHtml(errorTitle)}">${escapeHtml(errorMessage)}</div>
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

async function readResponsePayload(response) {
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { data, text };
}

function stringifyFetchErrorDetail(value) {
  if (value == null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => stringifyFetchErrorDetail(item))
      .filter(Boolean)
      .join("; ");
  }
  if (typeof value === "object") {
    const source = value.error && typeof value.error === "object" ? value.error : value;
    const candidate = source.message || source.detail || source.error || source.code || source.type;
    if (candidate != null && candidate !== value) {
      return stringifyFetchErrorDetail(candidate);
    }
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function formatFetchErrorDetail(response, data) {
  const status = coerceHttpStatus(response?.status);
  const statusLabel = status ? `HTTP ${status}` : "请求失败";
  const statusText = normalizeWhitespace(response?.statusText || "");
  const contentType = response?.headers?.get?.("content-type") || "";

  const htmlSummary = typeof data === "string" ? summarizeHtmlError(data, { status, contentType }) : "";
  if (htmlSummary) {
    return htmlSummary;
  }

  const detail = stringifyFetchErrorDetail(data);
  const detailHtmlSummary = summarizeHtmlError(detail, { status, contentType });
  if (detailHtmlSummary) {
    return detailHtmlSummary;
  }
  if (detail) {
    return truncateText(detail, 240);
  }
  return statusText ? `${statusLabel} · ${statusText}` : statusLabel;
}

function createFetchError(response, data) {
  const error = new Error(formatFetchErrorDetail(response, data));
  error.status = response.status;
  error.statusText = response.statusText;
  return error;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const { data } = await readResponsePayload(response);
  if (!response.ok) {
    throw createFetchError(response, data);
  }
  return data;
}

async function loadHealth() {
  const response = await fetch("/healthz");
  const { data } = await readResponsePayload(response);
  if (!data || typeof data !== "object") {
    throw createFetchError(response, data);
  }
  const health = data;
  state.healthLoaded = true;
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

async function loadTokens({ page = state.tokenPage, allowPageAdjust = true } = {}) {
  const requestedPage = normalizePositiveInteger(page, 1);
  const limit = state.tokenPageSize;
  const offset = (requestedPage - 1) * limit;
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    include_quota: "1",
    q: normalizeWhitespace(state.tokenSearchTerm),
    status: normalizeTokenStatusFilter(state.tokenStatusFilter),
    plan_type: normalizeAvailablePlanFilter(state.tokenPlanFilter),
    sort: normalizeTokenSort(state.tokenSort),
  });
  if (state.selectedImportBatchId != null) {
    params.set("import_batch_id", String(state.selectedImportBatchId));
  }

  state.tokenPage = requestedPage;
  state.tokenPageLoading = true;
  renderTokenPagination("正在载入当前页");

  try {
    const data = await fetchJson(`/admin/tokens?${params.toString()}`, {
      headers: authHeaders(),
    });
    state.tokensLoaded = true;
    state.tokenItems = Array.isArray(data.items) ? data.items : [];
    state.tokenImportBatches = Array.isArray(data.import_batches) ? data.import_batches : [];
    state.tokenFilteredCounts = data.filtered_counts || data.counts || state.tokenFilteredCounts;
    state.tokenPlanCounts = data.plan_counts || state.tokenPlanCounts;
    const pagination = normalizeTokenPagination(data.pagination || {}, state.tokenItems.length, requestedPage);
    if (allowPageAdjust && pagination.total > 0 && state.tokenItems.length === 0 && requestedPage > pagination.totalPages) {
      state.tokenPage = pagination.totalPages;
      return loadTokens({ page: pagination.totalPages, allowPageAdjust: false });
    }
    state.tokenPagination = pagination;
    state.tokenPage = pagination.page;
    state.tokenPageLoading = false;
    elements.listNote.textContent = buildTokenListNote();
    renderTokenSelection(data.selection || {});
    setTokenSelectionDisabled(state.tokenSelectionSaving);
    setTokenConcurrencyDisabled(state.tokenActiveStreamCapSaving);
    setTokenPlanOrderDisabled(state.tokenPlanOrderSaving);
    setTokenSearchDisabled(false);
    renderTokenQueryControls();
    renderTokenPagination();
    renderTokenList(state.tokenItems);
    if (data.counts) {
      renderCounts(data.counts);
    }
  } catch (error) {
    state.tokensLoaded = true;
    state.tokenItems = [];
    state.tokenImportBatches = [];
    state.tokenFilteredCounts = { total: 0, active: 0, available: 0, cooling: 0, disabled: 0 };
    state.tokenPlanCounts = { free: 0, plus: 0, team: 0, pro: 0, unknown: 0 };
    state.tokenPagination = normalizeTokenPagination({}, 0, 1);
    state.tokenPage = 1;
    state.tokenPageLoading = false;
    if (error.status === 401) {
      elements.listNote.textContent = "需要输入 Service API Key 才能查看明细和导入";
      renderTokenSearchSummary(0, 0, "需要 Service API Key");
      renderTokenPagination("需要 Service API Key");
      if (elements.tokenSelectionSummary) {
        elements.tokenSelectionSummary.textContent = "需要 Service API Key";
      }
      if (elements.tokenPlanOrderSummary) {
        elements.tokenPlanOrderSummary.textContent = "需要 Service API Key";
      }
      if (elements.tokenConcurrencySummary) {
        elements.tokenConcurrencySummary.textContent = "需要 Service API Key";
      }
      setTokenSelectionDisabled(true);
      setTokenConcurrencyDisabled(true);
      setTokenPlanOrderDisabled(true);
      setTokenSearchDisabled(true);
      setTokenPaginationDisabled(true);
      renderTokenQueryControls();
      elements.tokenList.innerHTML = `
        <article class="empty-state">
          <p>管理接口已加锁。填入 Service API Key 后可查看明细和导入 key。</p>
        </article>
      `;
      return;
    }
    elements.listNote.textContent = `列表加载失败：${error.message}`;
    renderTokenSearchSummary(0, 0, "列表加载失败");
    renderTokenPagination("列表加载失败");
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent = "切换器同步失败";
    }
    if (elements.tokenPlanOrderSummary) {
      elements.tokenPlanOrderSummary.textContent = "顺序同步失败";
    }
    if (elements.tokenConcurrencySummary) {
      elements.tokenConcurrencySummary.textContent = "上限同步失败";
    }
    setTokenSelectionDisabled(true);
    setTokenConcurrencyDisabled(true);
    setTokenPlanOrderDisabled(true);
    setTokenSearchDisabled(true);
    setTokenPaginationDisabled(true);
    renderTokenQueryControls();
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(error.message)}</p>
      </article>
    `;
  }
}

async function goToTokenPage(page) {
  if (state.tokenPageLoading) {
    return;
  }
  const requestedPage = normalizePositiveInteger(page, state.tokenPage);
  const totalPages = state.tokenPagination.totalPages || 1;
  const nextPage = Math.min(Math.max(1, requestedPage), totalPages);
  if (nextPage === state.tokenPage && state.tokensLoaded) {
    renderTokenPagination();
    return;
  }
  await loadTokens({ page: nextPage });
}

async function applyTokenQuery(updates = {}) {
  if (Object.prototype.hasOwnProperty.call(updates, "search")) {
    state.tokenSearchTerm = String(updates.search || "");
  }
  if (Object.prototype.hasOwnProperty.call(updates, "status")) {
    state.tokenStatusFilter = normalizeTokenStatusFilter(updates.status);
  }
  if (Object.prototype.hasOwnProperty.call(updates, "planType")) {
    state.tokenPlanFilter = normalizeAvailablePlanFilter(updates.planType);
  }
  if (Object.prototype.hasOwnProperty.call(updates, "sort")) {
    state.tokenSort = normalizeTokenSort(updates.sort);
  }
  state.tokenPage = 1;
  await loadTokens({ page: 1 });
}

function queueTokenSearch(value) {
  state.tokenSearchTerm = String(value || "");
  if (state.tokenSearchDebounceTimer) {
    window.clearTimeout(state.tokenSearchDebounceTimer);
  }
  state.tokenSearchDebounceTimer = window.setTimeout(() => {
    state.tokenSearchDebounceTimer = null;
    void applyTokenQuery({ search: state.tokenSearchTerm });
  }, TOKEN_QUERY_DEBOUNCE_MS);
}

async function setTokenViewMode(mode) {
  const nextMode = normalizeTokenViewMode(mode);
  if (nextMode === state.tokenViewMode) {
    if (nextMode === "keys" && state.selectedImportBatchId != null) {
      state.selectedImportBatchId = null;
      await loadTokens({ page: 1 });
      return;
    }
    renderTokenViewMode(nextMode);
    return;
  }
  state.tokenViewMode = nextMode;
  state.selectedImportBatchId = null;
  state.tokenPage = 1;
  renderTokenViewMode(nextMode);
  if (nextMode === "import_batches") {
    renderImportBatchList();
    elements.listNote.textContent = "导入批次独立展示；点击批次可查看关联 key。";
    return;
  }
  await loadTokens({ page: 1 });
}

async function viewImportBatch(batchId) {
  const resolvedBatchId = normalizePositiveInteger(batchId, 0);
  if (!resolvedBatchId) {
    return;
  }
  state.selectedImportBatchId = resolvedBatchId;
  state.tokenViewMode = "keys";
  state.tokenStatusFilter = DEFAULT_TOKEN_STATUS_FILTER;
  state.tokenPlanFilter = AVAILABLE_PLAN_FILTER_ALL;
  state.tokenSearchTerm = "";
  if (elements.tokenSearchInput) {
    elements.tokenSearchInput.value = "";
  }
  renderTokenViewMode("keys");
  await loadTokens({ page: 1 });
}

async function saveTokenSelection(strategy) {
  const nextStrategy = normalizeTokenSelectionStrategy(strategy);
  if (state.tokenSelectionSaving || nextStrategy === state.tokenSelectionStrategy) {
    return;
  }

  const previousStrategy = state.tokenSelectionStrategy;
  let disableAfterSave = false;
  state.tokenSelectionSaving = true;
  renderTokenSelection({ strategy: nextStrategy });
  if (elements.tokenSelectionSummary) {
    elements.tokenSelectionSummary.textContent = `正在切换到${nextStrategy === "fill_first" ? " Fill-first" : "最旧未使用优先"}`;
  }
  setTokenSelectionDisabled(true);

  try {
    const data = await fetchJson("/admin/token-selection", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({ strategy: nextStrategy }),
    });
    renderTokenSelection(data);
    elements.listNote.textContent = buildTokenListNote();
  } catch (error) {
    renderTokenSelection({ strategy: previousStrategy });
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent =
        error.status === 401 ? "需要 Service API Key" : "切换失败，请重试";
    }
    disableAfterSave = error.status === 401;
    throw error;
  } finally {
    state.tokenSelectionSaving = false;
    setTokenSelectionDisabled(disableAfterSave);
  }
}

async function saveTokenConcurrency(value = state.tokenActiveStreamCapDraft) {
  const nextCap = normalizeTokenActiveStreamCap(value, state.tokenActiveStreamCap);
  syncTokenConcurrencyControls(nextCap);
  if (state.tokenActiveStreamCapSaving || nextCap === state.tokenActiveStreamCap) {
    return;
  }

  const previousCap = state.tokenActiveStreamCap;
  let disableAfterSave = false;
  let saved = false;
  state.tokenActiveStreamCapSaving = true;
  if (elements.tokenConcurrencySummary) {
    elements.tokenConcurrencySummary.textContent = "正在保存上限";
  }
  setTokenConcurrencyDisabled(true);

  try {
    const data = await fetchJson("/admin/token-selection/concurrency", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({ active_stream_cap: nextCap }),
    });
    renderTokenSelection(data);
    state.tokenItems = state.tokenItems.map((item) => ({
      ...item,
      active_stream_cap: state.tokenActiveStreamCap,
    }));
    renderTokenList(state.tokenItems);
    elements.listNote.textContent = buildTokenListNote();
    saved = true;
  } catch (error) {
    renderTokenConcurrency({ active_stream_cap: previousCap });
    if (elements.tokenConcurrencySummary) {
      elements.tokenConcurrencySummary.textContent =
        error.status === 401 ? "需要 Service API Key" : "保存失败，请重试";
    }
    disableAfterSave = error.status === 401;
    throw error;
  } finally {
    state.tokenActiveStreamCapSaving = false;
    if (saved) {
      syncTokenConcurrencyControls(state.tokenActiveStreamCap);
    }
    setTokenConcurrencyDisabled(disableAfterSave);
  }
}

async function saveTokenOrder(visibleTokenIds) {
  const nextOrder = mergeVisibleTokenOrder(visibleTokenIds);
  if (!nextOrder.length || state.tokenOrderSaving) {
    return;
  }
  if (JSON.stringify(nextOrder) === JSON.stringify(state.tokenSelectionOrder)) {
    return;
  }

  const previousOrder = [...state.tokenSelectionOrder];
  state.tokenOrderSaving = true;
  state.tokenSelectionOrder = nextOrder;
  elements.listNote.textContent = "正在保存 Fill-first 顺序…";

  try {
    const data = await fetchJson("/admin/token-selection/order", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({ token_ids: nextOrder }),
    });
    renderTokenSelection(data);
    elements.listNote.textContent = "Fill-first 顺序已保存";
  } catch (error) {
    state.tokenSelectionOrder = previousOrder;
    elements.listNote.textContent =
      error.status === 401 ? "需要输入 Service API Key 才能保存 Fill-first 顺序" : `顺序保存失败：${error.message}`;
    renderTokenList(state.tokenItems);
    throw error;
  } finally {
    state.tokenOrderSaving = false;
    renderTokenList(state.tokenItems);
  }
}

async function saveTokenPlanOrder({ enabled = state.tokenPlanOrderEnabled, planOrder = state.tokenPlanOrder } = {}) {
  const nextEnabled = Boolean(enabled);
  const nextPlanOrder = normalizeTokenPlanOrder(planOrder);
  const currentPlanOrder = normalizeTokenPlanOrder(state.tokenPlanOrder);
  if (
    state.tokenPlanOrderSaving ||
    (nextEnabled === state.tokenPlanOrderEnabled &&
      JSON.stringify(nextPlanOrder) === JSON.stringify(currentPlanOrder))
  ) {
    return;
  }

  const previousEnabled = state.tokenPlanOrderEnabled;
  const previousPlanOrder = [...state.tokenPlanOrder];
  let disableAfterSave = false;
  state.tokenPlanOrderSaving = true;
  state.tokenPlanOrderEnabled = nextEnabled;
  state.tokenPlanOrder = nextPlanOrder;
  renderTokenSelection({
    strategy: state.tokenSelectionStrategy,
    token_order: state.tokenSelectionOrder,
    plan_order_enabled: nextEnabled,
    plan_order: nextPlanOrder,
  });
  renderTokenList(state.tokenItems);
  elements.listNote.textContent = nextEnabled ? "正在保存计划优先级…" : "正在关闭计划优先级…";
  setTokenPlanOrderDisabled(true);

  try {
    const data = await fetchJson("/admin/token-selection/plan-order", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({
        enabled: nextEnabled,
        plan_order: nextPlanOrder,
      }),
    });
    renderTokenSelection(data);
    renderTokenList(state.tokenItems);
    elements.listNote.textContent = nextEnabled ? "计划优先级已保存" : buildTokenListNote();
  } catch (error) {
    state.tokenPlanOrderEnabled = previousEnabled;
    state.tokenPlanOrder = previousPlanOrder;
    renderTokenSelection({
      strategy: state.tokenSelectionStrategy,
      token_order: state.tokenSelectionOrder,
      plan_order_enabled: previousEnabled,
      plan_order: previousPlanOrder,
    });
    renderTokenList(state.tokenItems);
    if (elements.tokenPlanOrderSummary) {
      elements.tokenPlanOrderSummary.textContent =
        error.status === 401 ? "需要 Service API Key" : "保存失败，请重试";
    }
    elements.listNote.textContent =
      error.status === 401 ? "需要输入 Service API Key 才能保存计划优先级" : `计划优先级保存失败：${error.message}`;
    disableAfterSave = error.status === 401;
    throw error;
  } finally {
    state.tokenPlanOrderSaving = false;
    setTokenPlanOrderDisabled(disableAfterSave);
    renderTokenPlanOrderList();
  }
}

function movePlanOrderItem(planType, direction) {
  const resolvedPlanType = normalizePlanType(planType);
  const order = normalizeTokenPlanOrder(state.tokenPlanOrder);
  const index = order.indexOf(resolvedPlanType);
  const offset = direction === "up" ? -1 : direction === "down" ? 1 : 0;
  const nextIndex = index + offset;
  if (index < 0 || offset === 0 || nextIndex < 0 || nextIndex >= order.length) {
    return order;
  }
  const [item] = order.splice(index, 1);
  order.splice(nextIndex, 0, item);
  return order;
}

async function toggleTokenActivation(
  tokenId,
  { nextActive, clearCooldown = false, pendingKind = nextActive ? "enable" : "disable" } = {},
) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId) || isTokenActionPending(resolvedTokenId)) {
    return;
  }

  state.tokenActionPendingKinds.set(resolvedTokenId, pendingKind);
  renderTokenList(state.tokenItems);

  try {
    await fetchJson(`/admin/tokens/${resolvedTokenId}/activation`, {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({
        active: Boolean(nextActive),
        clear_cooldown: Boolean(clearCooldown),
      }),
    });
    await loadHealth();
    await loadTokens();
  } catch (error) {
    elements.listNote.textContent =
      error.status === 401 ? "需要输入 Service API Key 才能变更 key 状态" : `状态更新失败：${error.message}`;
    if (error.status === 401) {
      await loadTokens();
    }
    throw error;
  } finally {
    state.tokenActionPendingKinds.delete(resolvedTokenId);
    renderTokenList(state.tokenItems);
  }
}

async function probeToken(tokenId, model = state.probeModel) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId) || isTokenActionPending(resolvedTokenId)) {
    return;
  }

  const selectedModel = normalizeProbeModel(model);
  state.probeModel = selectedModel;
  state.tokenActionPendingKinds.set(resolvedTokenId, "probe");
  renderTokenList(state.tokenItems);

  try {
    const data = await fetchJson(`/admin/tokens/${resolvedTokenId}/probe`, {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({ model: selectedModel }),
    });
    clearTokenActionPending(resolvedTokenId);
    showToast(data?.message || "测试完成", resolveProbeToastTone(data));
    try {
      await loadHealth();
      await loadTokens();
    } catch (refreshError) {
      elements.listNote.textContent = `测试结果已返回，列表稍后刷新：${refreshError.message}`;
    }
  } catch (error) {
    clearTokenActionPending(resolvedTokenId);
    showToast(
      error.status === 401 ? "需要输入 Service API Key 才能测试 key。" : `测试失败：${error.message}`,
      "error",
    );
    if (error.status === 401) {
      await loadTokens();
    }
    throw error;
  } finally {
    clearTokenActionPending(resolvedTokenId);
  }
}

async function deleteToken(tokenId) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId) || isTokenActionPending(resolvedTokenId)) {
    return;
  }

  const item = getTokenItemById(resolvedTokenId);
  const context = item ? buildTokenDeleteContext(item) : { title: `Token #${resolvedTokenId}` };

  state.tokenActionPendingKinds.set(resolvedTokenId, "delete");
  if (state.tokenDeleteTargetId === resolvedTokenId) {
    clearTokenDeleteFeedback();
    setTokenDeleteDialogBusy(true);
  }
  renderTokenList(state.tokenItems);

  try {
    await fetchJson(`/admin/tokens/${resolvedTokenId}`, {
      method: "DELETE",
      headers: authHeaders(),
    });
    closeTokenDeleteDialog({ force: true });
    await loadHealth();
    await loadTokens();
    elements.listNote.textContent = `已删除 ${context.title}`;
  } catch (error) {
    const summary =
      error.status === 401 ? "需要输入 Service API Key 才能删除 key" : `删除失败：${error.message}`;
    elements.listNote.textContent = summary;
    if (state.tokenDeleteTargetId === resolvedTokenId) {
      setTokenDeleteFeedback(
        error.status === 401 ? "需要先填写 Service API Key，才能删除这把 key。" : `删除失败：${error.message}`,
        "error",
      );
    }
    if (error.status === 401) {
      await loadTokens();
    }
    throw error;
  } finally {
    state.tokenActionPendingKinds.delete(resolvedTokenId);
    if (state.tokenDeleteTargetId === resolvedTokenId) {
      setTokenDeleteDialogBusy(false);
    }
    renderTokenList(state.tokenItems);
  }
}

function isTokenDragGestureTarget(target) {
  return (
    target instanceof Element &&
    !target.closest("button, select, input, textarea, label, a, summary")
  );
}

function findAvailableTokenBody(target) {
  if (!(target instanceof Element)) {
    return null;
  }
  const list = target.closest(".token-result-list");
  return list instanceof HTMLElement ? list : null;
}

function findDraggedTokenCard(target) {
  if (!(target instanceof Element)) {
    return null;
  }
  const card = target.closest('[data-token-draggable="true"]');
  if (!(card instanceof HTMLElement)) {
    return null;
  }
  return card;
}

function getTokenDragAfterElement(container, pointerY) {
  const cards = Array.from(
    container.querySelectorAll('.token-result[data-token-draggable="true"]:not(.is-dragging)'),
  ).filter((card) => card instanceof HTMLElement);
  return cards.reduce(
    (closest, card) => {
      const box = card.getBoundingClientRect();
      const offset = pointerY - box.top - box.height / 2;
      if (offset < 0 && offset > closest.offset) {
        return { offset, element: card };
      }
      return closest;
    },
    { offset: Number.NEGATIVE_INFINITY, element: null },
  ).element;
}

function collectAvailableTokenOrder(container) {
  return Array.from(container.querySelectorAll("[data-token-card-id]"))
    .map((card) => Number(card.getAttribute("data-token-card-id")))
    .filter((tokenId) => Number.isInteger(tokenId) && tokenId > 0);
}

function clearTokenDragState() {
  state.tokenDragTokenId = null;
  elements.tokenList.querySelectorAll(".token-result.is-dragging").forEach((card) => {
    card.classList.remove("is-dragging");
    card.setAttribute("aria-grabbed", "false");
  });
}

function handleTokenCardDragStart(event) {
  if (!canDragAvailableTokens() || !isTokenDragGestureTarget(event.target)) {
    event.preventDefault();
    return;
  }
  const card = findDraggedTokenCard(event.target);
  if (!card) {
    event.preventDefault();
    return;
  }
  const tokenId = Number(card.dataset.tokenCardId);
  if (!Number.isInteger(tokenId) || tokenId <= 0) {
    event.preventDefault();
    return;
  }
  state.tokenDragTokenId = tokenId;
  card.classList.add("is-dragging");
  card.setAttribute("aria-grabbed", "true");
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", String(tokenId));
  }
}

function handleTokenCardDragOver(event) {
  if (!state.tokenDragTokenId || !canDragAvailableTokens()) {
    return;
  }
  const body = findAvailableTokenBody(event.target);
  const draggingCard = body?.querySelector(".token-result.is-dragging");
  if (!(body instanceof HTMLElement) || !(draggingCard instanceof HTMLElement)) {
    return;
  }
  event.preventDefault();
  if (event.dataTransfer) {
    event.dataTransfer.dropEffect = "move";
  }
  const afterElement = getTokenDragAfterElement(body, event.clientY);
  if (afterElement == null) {
    body.appendChild(draggingCard);
    return;
  }
  body.insertBefore(draggingCard, afterElement);
}

async function handleTokenCardDrop(event) {
  if (!state.tokenDragTokenId || !canDragAvailableTokens()) {
    return;
  }
  const body = findAvailableTokenBody(event.target);
  if (!(body instanceof HTMLElement)) {
    return;
  }
  event.preventDefault();
  const tokenOrder = collectAvailableTokenOrder(body);
  clearTokenDragState();
  try {
    await saveTokenOrder(tokenOrder);
  } catch {}
}

function handleTokenCardDragEnd() {
  const wasDragging = Boolean(state.tokenDragTokenId);
  clearTokenDragState();
  if (wasDragging) {
    renderTokenList(state.tokenItems);
  }
}

function findPlanOrderItem(target) {
  if (!(target instanceof Element)) {
    return null;
  }
  const item = target.closest("[data-plan-order-item]");
  return item instanceof HTMLElement ? item : null;
}

function handlePlanOrderDragStart(event) {
  if (!state.tokenPlanOrderEnabled || state.tokenPlanOrderSaving) {
    event.preventDefault();
    return;
  }
  const item = findPlanOrderItem(event.target);
  const planType = normalizePlanType(item?.dataset.planOrderItem);
  if (!item || !PLAN_TYPE_OPTIONS.includes(planType)) {
    event.preventDefault();
    return;
  }
  state.tokenPlanOrderDragType = planType;
  item.classList.add("is-dragging");
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", planType);
  }
}

function handlePlanOrderDragOver(event) {
  if (!state.tokenPlanOrderDragType || !state.tokenPlanOrderEnabled || state.tokenPlanOrderSaving) {
    return;
  }
  event.preventDefault();
  if (event.dataTransfer) {
    event.dataTransfer.dropEffect = "move";
  }
}

async function handlePlanOrderDrop(event) {
  if (!state.tokenPlanOrderDragType || !state.tokenPlanOrderEnabled || state.tokenPlanOrderSaving) {
    return;
  }
  event.preventDefault();
  const draggedType = state.tokenPlanOrderDragType;
  const targetItem = findPlanOrderItem(event.target);
  const targetType = normalizePlanType(targetItem?.dataset.planOrderItem);
  if (targetType === draggedType) {
    state.tokenPlanOrderDragType = "";
    renderTokenPlanOrderList();
    return;
  }
  const nextOrder = normalizeTokenPlanOrder(state.tokenPlanOrder).filter((planType) => planType !== draggedType);
  const targetIndex = nextOrder.indexOf(targetType);
  if (targetIndex >= 0) {
    nextOrder.splice(targetIndex, 0, draggedType);
  } else {
    nextOrder.push(draggedType);
  }
  state.tokenPlanOrderDragType = "";
  renderTokenPlanOrderList();
  try {
    await saveTokenPlanOrder({ enabled: true, planOrder: nextOrder });
  } catch {}
}

function handlePlanOrderDragEnd() {
  state.tokenPlanOrderDragType = "";
  renderTokenPlanOrderList();
}

async function loadRequests() {
  try {
    const data = await fetchJson("/admin/requests?limit=80", {
      headers: authHeaders(),
    });
    state.requestsLoaded = true;
    elements.requestNote.textContent = "显示最近 80 条请求 · 图表为最近 24 小时 token 与估算金额";
    renderRequestSummary(data.summary || {});
    renderRequestAnalytics(data.analytics || {});
    renderRequestList(data.items || []);
  } catch (error) {
    state.requestsLoaded = true;
    if (error.status === 401) {
      elements.requestNote.textContent = "需要输入 Service API Key 才能查看请求日志";
      renderRequestSummaryUnavailable();
      elements.requestTokenChartNote.textContent = "填入 Service API Key 后显示最近 24 小时 token 趋势。";
      elements.requestCostChartNote.textContent = "填入 Service API Key 后显示最近 24 小时模型金额分布。";
      renderChartEmpty(elements.requestTokenChart, "需要 Service API Key");
      renderChartEmpty(elements.requestCostChart, "需要 Service API Key");
      elements.requestList.innerHTML = `
        <article class="empty-state">
          <p>请求日志已加锁。填入 Service API Key 后可查看请求次数、token 用量、估算金额、模型名、端点、状态码和首字时间。</p>
        </article>
      `;
      return;
    }
    elements.requestNote.textContent = `请求日志加载失败：${error.message}`;
    renderRequestSummaryUnavailable();
    elements.requestTokenChartNote.textContent = "请求恢复后会显示最近 24 小时 token 趋势。";
    elements.requestCostChartNote.textContent = "请求恢复后会显示最近 24 小时模型金额分布。";
    renderChartEmpty(elements.requestTokenChart, "请求日志加载失败");
    renderChartEmpty(elements.requestCostChart, "模型金额暂时不可用");
    elements.requestList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(error.message)}</p>
      </article>
    `;
  }
}

async function refreshDashboard() {
  if (state.refreshing) {
    return;
  }
  state.refreshing = true;
  elements.refreshButton.disabled = true;
  renderInitialLoadingStates();
  try {
    await loadHealth();
    await loadRequests();
    await loadTokens();
  } catch (error) {
    elements.listNote.textContent = `面板同步失败：${error.message}`;
    elements.requestNote.textContent = `面板同步失败：${error.message}`;
  } finally {
    state.refreshing = false;
    elements.refreshButton.disabled = false;
    renderTokenPagination();
    if (state.importJobId && !state.importJobPollTimer) {
      resumeTrackedImportJob();
    }
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

    // Some paste sources hard-wrap the refresh_token part of account_id,refresh_token rows.
    // Refresh-token-only rows have no delimiter, so keep those as one record per line.
    if (!currentHasComma || nextHasComma) {
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
    if (commaIndex === -1) {
      payloads.push({
        refresh_token: line.trim(),
        type: "codex",
      });
      return;
    }

    if (commaIndex <= 0 || commaIndex === line.length - 1) {
      throw new Error(`${sourceLabel} 第 ${index + 1} 条记录格式错误，应为 refresh_token 或 account_id,refresh_token`);
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
    throw new Error("请先粘贴 JSON / refresh_token / account_id,refresh_token 文本，或选择至少一个文件");
  }
  return payloads;
}

async function importTokens() {
  setImportButtonBusy(true, "提交中…");
  setFeedback("正在整理并提交导入批次…");
  try {
    const payloads = await collectImportPayloads();
    const data = await fetchJson("/admin/tokens/import", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({
        tokens: payloads,
        import_queue_position: state.importQueuePosition,
      }),
    });
    const job = data?.job;
    if (!job?.id) {
      throw new Error("导入任务创建失败，服务端未返回 job_id");
    }
    persistImportJobId(job.id);
    setFeedback(
      `导入任务 #${job.id} 已创建，共 ${job.total_count || payloads.length} 条，后台会继续处理。`,
      "is-success",
    );
    await refreshDashboard();
    await pollImportJob(job.id, { refreshOnFinish: false });
  } catch (error) {
    setFeedback(`导入失败：${error.message}`, "is-error");
    if (!state.importJobId) {
      setImportButtonBusy(false);
    }
  }
}

function resetImportForm() {
  elements.tokenJson.value = "";
  elements.tokenFiles.value = "";
  if (state.importJobId) {
    return;
  }
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
  if (state.importJobId) {
    resumeTrackedImportJob();
  }
}

function clearServiceKey() {
  elements.serviceKey.value = "";
  state.serviceKey = "";
  localStorage.removeItem(STORAGE_KEY);
  setFeedback("Service API Key 已清空。");
  if (state.importJobId) {
    stopImportJobPolling();
    setImportButtonBusy(true, "等待鉴权…");
  }
  refreshDashboard();
}

function syncSystemTheme() {
  if (state.themePreference === "auto") {
    applyTheme("auto", { persist: false });
  }
}

applyTheme(state.themePreference, { persist: false });
renderImportQueuePosition(state.importQueuePosition);
renderTokenViewMode(state.tokenViewMode);
renderTokenQueryControls();
renderTokenSelection({ strategy: state.tokenSelectionStrategy });
renderTokenPlanOrderList();
renderInitialLoadingStates();
if (state.importJobId) {
  setImportButtonBusy(true, "后台导入中…");
  setFeedback(`检测到后台导入任务 #${state.importJobId}，正在恢复状态…`);
}

elements.themeButtons.forEach((button) => {
  button.addEventListener("click", () => applyTheme(button.dataset.themeOption));
});

elements.importQueuePositionButtons.forEach((button) => {
  button.addEventListener("click", () => renderImportQueuePosition(button.dataset.importQueuePosition));
});

elements.tokenViewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    void setTokenViewMode(button.dataset.tokenViewMode);
  });
});

elements.tokenSelectionButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await saveTokenSelection(button.dataset.selectionStrategy);
    } catch {}
  });
});

if (elements.tokenConcurrencyRange) {
  elements.tokenConcurrencyRange.addEventListener("input", (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    syncTokenConcurrencyControls(event.target.value);
  });
  elements.tokenConcurrencyRange.addEventListener("change", async (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    try {
      await saveTokenConcurrency(event.target.value);
    } catch {}
  });
}

if (elements.tokenConcurrencyInput) {
  elements.tokenConcurrencyInput.addEventListener("input", (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    if (!event.target.value.trim()) {
      if (elements.tokenConcurrencySummary) {
        elements.tokenConcurrencySummary.textContent = "范围 1-10 并发";
      }
      return;
    }
    syncTokenConcurrencyControls(event.target.value);
  });
  elements.tokenConcurrencyInput.addEventListener("change", async (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    try {
      await saveTokenConcurrency(event.target.value);
    } catch {}
  });
  elements.tokenConcurrencyInput.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter" || !(event.target instanceof HTMLInputElement)) {
      return;
    }
    event.preventDefault();
    try {
      await saveTokenConcurrency(event.target.value);
    } catch {}
  });
}

if (elements.tokenConcurrencySave) {
  elements.tokenConcurrencySave.addEventListener("click", async () => {
    try {
      await saveTokenConcurrency();
    } catch {}
  });
}

elements.tokenPlanOrderButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await saveTokenPlanOrder({
        enabled: button.dataset.planOrderEnabled === "true",
        planOrder: state.tokenPlanOrder,
      });
    } catch {}
  });
});

if (elements.tokenPlanOrderList) {
  elements.tokenPlanOrderList.addEventListener("click", async (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    const moveButton = event.target.closest("[data-plan-order-move]");
    if (!(moveButton instanceof HTMLElement)) {
      return;
    }
    const nextOrder = movePlanOrderItem(moveButton.dataset.planType, moveButton.dataset.planOrderMove);
    try {
      await saveTokenPlanOrder({ enabled: true, planOrder: nextOrder });
    } catch {}
  });
  elements.tokenPlanOrderList.addEventListener("dragstart", handlePlanOrderDragStart);
  elements.tokenPlanOrderList.addEventListener("dragover", handlePlanOrderDragOver);
  elements.tokenPlanOrderList.addEventListener("drop", (event) => {
    void handlePlanOrderDrop(event);
  });
  elements.tokenPlanOrderList.addEventListener("dragend", handlePlanOrderDragEnd);
}

if (elements.tokenSearchInput) {
  elements.tokenSearchInput.addEventListener("input", (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    queueTokenSearch(event.target.value);
  });
}

if (elements.tokenStatusFilters) {
  elements.tokenStatusFilters.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    const button = event.target.closest("[data-token-status-filter]");
    if (!(button instanceof HTMLElement)) {
      return;
    }
    void applyTokenQuery({ status: button.dataset.tokenStatusFilter });
  });
}

if (elements.tokenPlanFilters) {
  elements.tokenPlanFilters.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    const button = event.target.closest("[data-token-plan-filter]");
    if (!(button instanceof HTMLElement)) {
      return;
    }
    void applyTokenQuery({ planType: button.dataset.tokenPlanFilter });
  });
}

if (elements.tokenSortSelect) {
  elements.tokenSortSelect.addEventListener("change", (event) => {
    if (!(event.target instanceof HTMLSelectElement)) {
      return;
    }
    void applyTokenQuery({ sort: event.target.value });
  });
}

[
  [elements.tokenPageFirst, () => 1],
  [elements.tokenPagePrev, () => state.tokenPage - 1],
  [elements.tokenPageNext, () => state.tokenPage + 1],
  [elements.tokenPageLast, () => state.tokenPagination.totalPages || 1],
].forEach(([button, resolvePage]) => {
  if (!button) {
    return;
  }
  button.addEventListener("click", async () => {
    try {
      await goToTokenPage(resolvePage());
    } catch {}
  });
});

if (elements.tokenPageInput) {
  elements.tokenPageInput.addEventListener("change", async (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    try {
      await goToTokenPage(event.target.value);
    } catch {}
  });
  elements.tokenPageInput.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter" || !(event.target instanceof HTMLInputElement)) {
      return;
    }
    event.preventDefault();
    try {
      await goToTokenPage(event.target.value);
    } catch {}
  });
}

elements.tokenList.addEventListener("click", async (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const importBatchButton = event.target.closest("[data-import-batch-view]");
  if (importBatchButton instanceof HTMLElement) {
    try {
      await viewImportBatch(importBatchButton.dataset.importBatchView);
    } catch {}
    return;
  }
  const probeButton = event.target.closest("[data-token-probe]");
  if (probeButton instanceof HTMLElement) {
    const actions = probeButton.closest(".token-card__actions");
    const modelSelect = actions?.querySelector("[data-token-probe-model]");
    const selectedModel = modelSelect instanceof HTMLSelectElement ? modelSelect.value : state.probeModel;
    try {
      await probeToken(probeButton.dataset.tokenId, selectedModel);
    } catch {}
    return;
  }
  const toggleButton = event.target.closest("[data-token-toggle]");
  if (toggleButton instanceof HTMLElement) {
    try {
      await toggleTokenActivation(toggleButton.dataset.tokenId, {
        nextActive: toggleButton.dataset.nextActive === "true",
        clearCooldown: toggleButton.dataset.clearCooldown === "true",
        pendingKind: toggleButton.dataset.pendingKind,
      });
    } catch {}
    return;
  }
  const deleteButton = event.target.closest("[data-token-delete]");
  if (deleteButton instanceof HTMLElement) {
    openTokenDeleteDialog(deleteButton.dataset.tokenId);
  }
});

elements.tokenList.addEventListener("change", (event) => {
  if (!(event.target instanceof HTMLSelectElement) || !event.target.matches("[data-token-probe-model]")) {
    return;
  }
  state.probeModel = normalizeProbeModel(event.target.value);
  event.target.value = state.probeModel;
});

elements.tokenList.addEventListener("dragstart", handleTokenCardDragStart);
elements.tokenList.addEventListener("dragover", handleTokenCardDragOver);
elements.tokenList.addEventListener("drop", (event) => {
  void handleTokenCardDrop(event);
});
elements.tokenList.addEventListener("dragend", handleTokenCardDragEnd);

if (elements.toastStack) {
  elements.toastStack.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    const dismissButton = event.target.closest("[data-toast-dismiss]");
    if (dismissButton instanceof HTMLElement) {
      dismissToast();
    }
  });
}

elements.tokenList.addEventListener(
  "toggle",
  (event) => {
    const target = event.target;
    if (!(target instanceof HTMLDetailsElement)) {
      return;
    }
    const tokenId = target.dataset.tokenCardId;
    if (!tokenId) {
      return;
    }
    state.tokenDetailsOpenStates[String(tokenId)] = target.open;
  },
  true,
);

if (typeof state.themeMedia.addEventListener === "function") {
  state.themeMedia.addEventListener("change", syncSystemTheme);
} else if (typeof state.themeMedia.addListener === "function") {
  state.themeMedia.addListener(syncSystemTheme);
}

if (elements.tokenDeleteCancel) {
  elements.tokenDeleteCancel.addEventListener("click", () => closeTokenDeleteDialog());
}

if (elements.tokenDeleteClose) {
  elements.tokenDeleteClose.addEventListener("click", () => closeTokenDeleteDialog());
}

if (elements.tokenDeleteConfirm) {
  elements.tokenDeleteConfirm.addEventListener("click", async () => {
    if (state.tokenDeleteTargetId == null) {
      return;
    }
    try {
      await deleteToken(state.tokenDeleteTargetId);
    } catch {}
  });
}

if (supportsTokenDeleteDialog) {
  elements.tokenDeleteDialog.addEventListener("click", (event) => {
    if (event.target === elements.tokenDeleteDialog) {
      closeTokenDeleteDialog();
    }
  });
  elements.tokenDeleteDialog.addEventListener("cancel", (event) => {
    if (state.tokenDeleteTargetId != null && getTokenActionPendingKind(state.tokenDeleteTargetId) === "delete") {
      event.preventDefault();
    }
  });
  elements.tokenDeleteDialog.addEventListener("close", () => {
    state.tokenDeleteTargetId = null;
    setTokenDeleteDialogBusy(false);
    clearTokenDeleteFeedback();
  });
}

elements.refreshButton.addEventListener("click", refreshDashboard);
elements.saveKeyButton.addEventListener("click", saveServiceKey);
elements.clearKeyButton.addEventListener("click", clearServiceKey);
elements.importButton.addEventListener("click", importTokens);
elements.resetImportButton.addEventListener("click", resetImportForm);

refreshDashboard();
state.timer = window.setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
resumeTrackedImportJob();
