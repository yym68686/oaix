const STORAGE_KEY = "oaix.serviceKey";
const THEME_STORAGE_KEY = "oaix.themePreference";
const IMPORT_JOB_STORAGE_KEY = "oaix.importJobId";
const REFRESH_INTERVAL_MS = 15000;
const IMPORT_JOB_POLL_INTERVAL_MS = 1500;
const TOAST_DURATION_MS = 5200;
const TOAST_EXIT_DURATION_MS = 220;
const TOKEN_LIST_BASE_NOTE = "显示最近 80 条记录 · 可用 / 冷却 / 已禁用分组";
const THEME_OPTIONS = new Set(["auto", "light", "dark"]);
const PROBE_MODEL_OPTIONS = ["gpt-5.4-mini", "gpt-5.5"];
const DEFAULT_PROBE_MODEL = PROBE_MODEL_OPTIONS[0];
const IMPORT_JOB_ACTIVE_STATUSES = new Set(["queued", "running"]);

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
  tokenActionPendingKinds: new Map(),
  tokenDeleteTargetId: null,
  tokenGroupOpenStates: Object.create(null),
  tokenSearchTerm: "",
  tokenItems: [],
  healthLoaded: false,
  tokensLoaded: false,
  requestsLoaded: false,
  tokenSelectionSaving: false,
  tokenSelectionStrategy: "least_recently_used",
  tokenSelectionOrder: [],
  tokenOrderSaving: false,
  tokenDragTokenId: null,
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
  importButton: document.getElementById("import-button"),
  resetImportButton: document.getElementById("reset-import-button"),
  importFeedback: document.getElementById("import-feedback"),
  tokenList: document.getElementById("token-list"),
  listNote: document.getElementById("list-note"),
  tokenSearchInput: document.getElementById("token-search-input"),
  tokenSearchSummary: document.getElementById("token-search-summary"),
  tokenSelectionSummary: document.getElementById("token-selection-summary"),
  tokenSelectionButtons: Array.from(document.querySelectorAll("[data-selection-strategy]")),
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

function describeTokenSelectionStrategy(strategy) {
  if (normalizeTokenSelectionStrategy(strategy) === "fill_first") {
    return "首个可用优先";
  }
  return "最久未用优先";
}

function setTokenSelectionDisabled(disabled) {
  elements.tokenSelectionButtons.forEach((button) => {
    button.disabled = disabled;
  });
}

function setTokenSearchDisabled(disabled) {
  if (!elements.tokenSearchInput) {
    return;
  }
  elements.tokenSearchInput.disabled = disabled;
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
    elements.listNote.textContent = "正在载入最近 80 条 key…";
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent = "正在同步策略";
    }
    setTokenSelectionDisabled(true);
    setTokenSearchDisabled(true);
    renderTokenListLoading();
  }
  if (!state.requestsLoaded) {
    elements.requestNote.textContent = "正在载入最近 80 条请求…";
    renderRequestSummaryUnavailable();
    renderRequestAnalyticsLoading();
    renderRequestListLoading();
  }
}

function renderTokenSelection(selection) {
  const strategy = normalizeTokenSelectionStrategy(selection?.strategy);
  state.tokenSelectionStrategy = strategy;
  if (Array.isArray(selection?.token_order)) {
    state.tokenSelectionOrder = normalizeTokenSelectionOrder(selection.token_order);
  }

  elements.tokenSelectionButtons.forEach((button) => {
    const active = button.dataset.selectionStrategy === strategy;
    button.setAttribute("aria-pressed", String(active));
  });

  if (elements.tokenSelectionSummary) {
    elements.tokenSelectionSummary.textContent = describeTokenSelectionStrategy(strategy);
  }
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
    state.tokenSelectionStrategy === "fill_first" &&
    !state.tokenOrderSaving &&
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
  if (state.tokenSelectionStrategy !== "fill_first") {
    return list;
  }
  const ranks = getTokenSelectionOrderRanks();
  const fallbackRank = ranks.size;
  return list.sort((left, right) => {
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

function renderTokenSearchSummary(totalCount, visibleCount, customMessage = "") {
  if (!elements.tokenSearchSummary) {
    return;
  }
  if (customMessage) {
    elements.tokenSearchSummary.textContent = customMessage;
    return;
  }
  if (!getTokenSearchTerms().length) {
    elements.tokenSearchSummary.textContent = totalCount > 0 ? `显示 ${formatInteger(totalCount)} 条` : "等待导入或刷新";
    return;
  }
  elements.tokenSearchSummary.textContent = `匹配 ${formatInteger(visibleCount)} / ${formatInteger(totalCount)} 条`;
}

function resolveTokenGroupOpenState(groupId, fallbackOpen) {
  return Object.prototype.hasOwnProperty.call(state.tokenGroupOpenStates, groupId)
    ? Boolean(state.tokenGroupOpenStates[groupId])
    : Boolean(fallbackOpen);
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

function formatSubscriptionUntil(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  if (date.getTime() <= Date.now()) {
    return `已过期 ${formatDate(value)}`;
  }
  return formatDate(value);
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

function countDistinctWorkspaces(items, workspaceTokenCounts = null) {
  if (!Array.isArray(items) || items.length === 0) {
    return 0;
  }
  if (workspaceTokenCounts && typeof workspaceTokenCounts === "object") {
    return Object.keys(workspaceTokenCounts).length;
  }
  return Object.keys(buildWorkspaceTokenCounts(items)).length;
}

function describeTokenGroupMeta(items, presentText, emptyText, workspaceTokenCounts = null) {
  if (!Array.isArray(items) || items.length === 0) {
    return emptyText;
  }
  const distinctWorkspaceCount = countDistinctWorkspaces(items, workspaceTokenCounts);
  if (!distinctWorkspaceCount || distinctWorkspaceCount === items.length) {
    return presentText;
  }
  return `${presentText} · ${formatInteger(distinctWorkspaceCount)} 个工作区共享这 ${formatInteger(items.length)} 把 token`;
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
  const source = payload.data && typeof payload.data === "object" ? payload.data : {};
  const type = normalizeWhitespace(source.type || source.code || "");
  const message = normalizeWhitespace(source.message || source.detail || raw);
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

function renderTokenFact(label, value, tone = "default") {
  return `
    <span class="token-fact">
      <span class="token-fact__label">${escapeHtml(label)}</span>
      <span class="token-fact__value token-fact__value--${tone}">${escapeHtml(value)}</span>
    </span>
  `;
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

function renderTokenCard(item, workspaceTokenCounts) {
  const status = deriveStatus(item);
  const tokenId = Number(item?.id);
  const draggable = status.tone === "available" && canDragAvailableTokens();
  const presentation = buildTokenPresentation(item, workspaceTokenCounts);
  const planLabel = formatPlanType(item.plan_type);
  const planTone = derivePlanTone(item.plan_type);
  const subscriptionValue = formatSubscriptionUntil(item.subscription_active_until);
  const subscriptionTone = subscriptionValue.startsWith("已过期")
    ? "disabled"
    : subscriptionValue === "—"
      ? "muted"
      : "default";
  const cooldownValue = formatCooldownUntil(item.cooldown_until);
  const createdAtValue = formatDate(item.created_at);
  const lastUsedValue = formatDate(item.last_used_at);
  const observedCostValue = item.observed_cost_usd == null ? "—" : formatUsd(item.observed_cost_usd);
  const cooldownTone = cooldownValue === "—" ? "muted" : "cooling";
  const createdAtTone = createdAtValue === "—" ? "muted" : "default";
  const lastUsedTone = lastUsedValue === "—" ? "muted" : "default";
  const observedCostTone = item.observed_cost_usd == null ? "muted" : "money";
  const errorSection = renderTokenErrorSection(item);
  const observedCostLabel = "已用金额";
  return `
    <article
      class="token-card token-card--${status.tone}${draggable ? " token-card--draggable" : ""}"
      data-token-card-id="${Number.isFinite(tokenId) ? tokenId : ""}"
      data-token-draggable="${draggable ? "true" : "false"}"
      draggable="${draggable ? "true" : "false"}"
      aria-grabbed="false"
    >
      <div class="token-card__top">
        <div class="token-card__identity">
          <span class="token-card__account">${escapeHtml(presentation.account)}</span>
          <span class="token-card__meta">${escapeHtml(presentation.meta)}</span>
        </div>
        <div class="token-card__overview">
          <div class="token-card__chips">
            <span class="status-pill status-pill--${status.tone}">${status.label}</span>
            <span class="status-pill status-pill--${planTone}">${escapeHtml(planLabel)}</span>
          </div>
          <div class="token-card__facts">
            ${renderTokenFact("订阅", subscriptionValue, subscriptionTone)}
            ${renderTokenFact("冷却到", cooldownValue, cooldownTone)}
            ${renderTokenFact("入库时间", createdAtValue, createdAtTone)}
            ${renderTokenFact("最近使用", lastUsedValue, lastUsedTone)}
            ${renderTokenFact(observedCostLabel, observedCostValue, observedCostTone)}
          </div>
        </div>
        <div class="token-card__action">
          ${renderTokenActionButtons(item)}
        </div>
      </div>
      <div class="token-card__bottom${errorSection ? " token-card__bottom--with-error" : ""}">
        ${renderQuotaSection(item, presentation)}
        ${errorSection}
      </div>
    </article>
  `;
}

function renderTokenGroup({ id, title, meta, tone, items, open = false, emptyMessage, itemRenderer = renderTokenCard }) {
  const list = Array.isArray(items) ? items : [];
  return `
    <details class="token-group token-group--${escapeHtml(tone)}" data-token-group="${escapeHtml(id)}"${open ? " open" : ""}>
      <summary class="token-group__summary">
        <span class="token-group__summary-main">
          <span class="token-group__title-row">
            <span class="token-group__title">${escapeHtml(title)}</span>
            <span class="token-group__count">${escapeHtml(formatInteger(list.length))}</span>
          </span>
          <span class="token-group__meta">${escapeHtml(meta)}</span>
        </span>
        <span class="token-group__summary-action">
          <span class="token-group__toggle-label token-group__toggle-label--closed">展开</span>
          <span class="token-group__toggle-label token-group__toggle-label--open">收起</span>
          <span class="token-group__toggle-icon" aria-hidden="true"></span>
        </span>
      </summary>
      <div class="token-group__body token-group__body--fold">
        ${
          list.length > 0
            ? list.map((item) => itemRenderer(item)).join("")
            : `<article class="empty-state empty-state--inline"><p>${escapeHtml(emptyMessage)}</p></article>`
        }
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

function renderTokenList(items) {
  const list = Array.isArray(items) ? items : [];
  const filteredItems = filterTokenItems(list);
  renderTokenSearchSummary(list.length, filteredItems.length);

  if (list.length === 0) {
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>没有最近记录。先导入一批 key，列表会在这里展开。</p>
      </article>
    `;
    return;
  }

  if (filteredItems.length === 0) {
    const query = normalizeWhitespace(state.tokenSearchTerm);
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>最近 80 条记录里没有匹配“${escapeHtml(query)}”的 key。</p>
      </article>
    `;
    return;
  }

  const availableItems = [];
  const coolingItems = [];
  const disabledItems = [];
  const workspaceTokenCounts = buildWorkspaceTokenCounts(filteredItems);

  filteredItems.forEach((item) => {
    const status = deriveStatus(item);
    if (status.tone === "disabled") {
      disabledItems.push(item);
      return;
    }
    if (status.tone === "cooling") {
      coolingItems.push(item);
      return;
    }
    availableItems.push(item);
  });
  const orderedAvailableItems = sortAvailableTokenItems(availableItems);

  const availableOpen = resolveTokenGroupOpenState("available", true);
  const coolingOpen = resolveTokenGroupOpenState("cooling", orderedAvailableItems.length === 0 && coolingItems.length > 0);
  const disabledOpen = resolveTokenGroupOpenState(
    "disabled",
    orderedAvailableItems.length === 0 && coolingItems.length === 0 && disabledItems.length > 0,
  );

  elements.tokenList.innerHTML = `
    ${renderTokenGroup({
      id: "available",
      title: "可用",
      meta: describeTokenGroupMeta(
        orderedAvailableItems,
        "可立即调度",
        "当前没有可立即调度的 key",
        buildWorkspaceTokenCounts(orderedAvailableItems),
      ),
      tone: "available",
      items: orderedAvailableItems,
      itemRenderer: (item) => renderTokenCard(item, workspaceTokenCounts),
      open: availableOpen,
      emptyMessage: "当前没有可立即调度的 key。",
    })}
    ${renderTokenGroup({
      id: "cooling",
      title: "冷却",
      meta: describeTokenGroupMeta(
        coolingItems,
        "等待冷却窗口结束",
        "当前没有冷却中的 key",
        buildWorkspaceTokenCounts(coolingItems),
      ),
      tone: "cooling",
      items: coolingItems,
      itemRenderer: (item) => renderTokenCard(item, workspaceTokenCounts),
      open: coolingOpen,
      emptyMessage: "当前没有处于冷却窗口的 key。",
    })}
    ${renderTokenGroup({
      id: "disabled",
      title: "已禁用",
      meta: describeTokenGroupMeta(
        disabledItems,
        "已移出调度池",
        "当前没有已禁用 key",
        buildWorkspaceTokenCounts(disabledItems),
      ),
      tone: "disabled",
      items: disabledItems,
      itemRenderer: (item) => renderTokenCard(item, workspaceTokenCounts),
      open: disabledOpen,
      emptyMessage: "当前没有已禁用的 key。",
    })}
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
      const errorMessage = item.error_message || "—";
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
          <div class="request-row__time" data-label="首字时间">${escapeHtml(formatDurationMs(item.ttft_ms))}</div>
          <div class="request-row__time" data-label="尝试">${escapeHtml(attemptLabel)}</div>
          <div class="request-row__error" data-label="错误" title="${escapeHtml(errorMessage)}">${escapeHtml(errorMessage)}</div>
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

async function loadTokens() {
  try {
    const data = await fetchJson("/admin/tokens?limit=80&include_quota=1", {
      headers: authHeaders(),
    });
    state.tokensLoaded = true;
    state.tokenItems = Array.isArray(data.items) ? data.items : [];
    elements.listNote.textContent = TOKEN_LIST_BASE_NOTE;
    renderTokenSelection(data.selection || {});
    setTokenSelectionDisabled(state.tokenSelectionSaving);
    setTokenSearchDisabled(false);
    renderTokenList(state.tokenItems);
    if (data.counts) {
      renderCounts(data.counts);
    }
  } catch (error) {
    state.tokensLoaded = true;
    state.tokenItems = [];
    if (error.status === 401) {
      elements.listNote.textContent = "需要输入 Service API Key 才能查看明细和导入";
      renderTokenSearchSummary(0, 0, "需要 Service API Key");
      if (elements.tokenSelectionSummary) {
        elements.tokenSelectionSummary.textContent = "需要 Service API Key";
      }
      setTokenSelectionDisabled(true);
      setTokenSearchDisabled(true);
      elements.tokenList.innerHTML = `
        <article class="empty-state">
          <p>管理接口已加锁。填入 Service API Key 后可查看明细和导入 key。</p>
        </article>
      `;
      return;
    }
    elements.listNote.textContent = `列表加载失败：${error.message}`;
    renderTokenSearchSummary(0, 0, "列表加载失败");
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent = "切换器同步失败";
    }
    setTokenSelectionDisabled(true);
    setTokenSearchDisabled(true);
    elements.tokenList.innerHTML = `
      <article class="empty-state">
        <p>${escapeHtml(error.message)}</p>
      </article>
    `;
  }
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
    elements.listNote.textContent = TOKEN_LIST_BASE_NOTE;
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
  const group = target.closest('[data-token-group="available"]');
  if (!(group instanceof HTMLElement)) {
    return null;
  }
  const body = group.querySelector(".token-group__body");
  return body instanceof HTMLElement ? body : null;
}

function findDraggedTokenCard(target) {
  if (!(target instanceof Element)) {
    return null;
  }
  const card = target.closest('[data-token-draggable="true"]');
  if (!(card instanceof HTMLElement)) {
    return null;
  }
  const group = card.closest("[data-token-group]");
  if (!(group instanceof HTMLElement) || group.dataset.tokenGroup !== "available") {
    return null;
  }
  return card;
}

function getTokenDragAfterElement(container, pointerY) {
  const cards = Array.from(
    container.querySelectorAll('.token-card[data-token-draggable="true"]:not(.is-dragging)'),
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
  elements.tokenList.querySelectorAll(".token-card.is-dragging").forEach((card) => {
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
  const draggingCard = body?.querySelector(".token-card.is-dragging");
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
  elements.refreshButton.disabled = true;
  renderInitialLoadingStates();
  try {
    await loadHealth();
    await Promise.all([loadTokens(), loadRequests()]);
  } catch (error) {
    elements.listNote.textContent = `面板同步失败：${error.message}`;
    elements.requestNote.textContent = `面板同步失败：${error.message}`;
  } finally {
    elements.refreshButton.disabled = false;
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
  setImportButtonBusy(true, "提交中…");
  setFeedback("正在整理并提交导入批次…");
  try {
    const payloads = await collectImportPayloads();
    const data = await fetchJson("/admin/tokens/import", {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify(payloads),
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
renderTokenSelection({ strategy: state.tokenSelectionStrategy });
renderInitialLoadingStates();
if (state.importJobId) {
  setImportButtonBusy(true, "后台导入中…");
  setFeedback(`检测到后台导入任务 #${state.importJobId}，正在恢复状态…`);
}

elements.themeButtons.forEach((button) => {
  button.addEventListener("click", () => applyTheme(button.dataset.themeOption));
});

elements.tokenSelectionButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    try {
      await saveTokenSelection(button.dataset.selectionStrategy);
    } catch {}
  });
});

if (elements.tokenSearchInput) {
  elements.tokenSearchInput.addEventListener("input", (event) => {
    if (!(event.target instanceof HTMLInputElement)) {
      return;
    }
    state.tokenSearchTerm = event.target.value;
    renderTokenList(state.tokenItems);
  });
}

elements.tokenList.addEventListener("click", async (event) => {
  if (!(event.target instanceof Element)) {
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
    const groupId = target.dataset.tokenGroup;
    if (!groupId) {
      return;
    }
    state.tokenGroupOpenStates[groupId] = target.open;
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
