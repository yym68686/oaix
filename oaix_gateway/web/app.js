const STORAGE_KEY = "oaix.serviceKey";
const THEME_STORAGE_KEY = "oaix.themePreference";
const REFRESH_INTERVAL_MS = 15000;
const THEME_OPTIONS = new Set(["auto", "light", "dark"]);

function normalizeThemePreference(value) {
  return THEME_OPTIONS.has(value) ? value : "auto";
}

const initialThemePreference = normalizeThemePreference(
  document.documentElement.dataset.themePreference || localStorage.getItem(THEME_STORAGE_KEY) || "auto",
);
const initialResolvedTheme = document.documentElement.dataset.colorScheme === "dark" ? "dark" : "light";

const state = {
  serviceKey: localStorage.getItem(STORAGE_KEY) || "",
  protected: false,
  timer: null,
  tokenActionPendingIds: new Set(),
  tokenItems: [],
  tokenSelectionSaving: false,
  tokenSelectionStrategy: "least_recently_used",
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
};

elements.serviceKey.value = state.serviceKey;

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

function describeTokenSelectionStrategy(strategy) {
  if (normalizeTokenSelectionStrategy(strategy) === "fill_first") {
    return "优先首个可用 key";
  }
  return "最旧未使用优先";
}

function setTokenSelectionDisabled(disabled) {
  elements.tokenSelectionButtons.forEach((button) => {
    button.disabled = disabled;
  });
}

function renderTokenSelection(selection) {
  const strategy = normalizeTokenSelectionStrategy(selection?.strategy);
  state.tokenSelectionStrategy = strategy;

  elements.tokenSelectionButtons.forEach((button) => {
    const active = button.dataset.selectionStrategy === strategy;
    button.setAttribute("aria-pressed", String(active));
  });

  if (elements.tokenSelectionSummary) {
    elements.tokenSelectionSummary.textContent = describeTokenSelectionStrategy(strategy);
  }
}

function isTokenActionPending(tokenId) {
  return state.tokenActionPendingIds.has(Number(tokenId));
}

function renderTokenActionButton(item) {
  const tokenId = Number(item?.id);
  const nextActive = !Boolean(item?.is_active);
  const pending = isTokenActionPending(tokenId);
  const label = pending ? "处理中…" : nextActive ? "取消禁用" : "暂时禁用";
  const tone = nextActive ? "enable" : "disable";
  return `
    <button
      class="token-action-button token-action-button--${tone}"
      type="button"
      data-token-toggle="true"
      data-token-id="${tokenId}"
      data-next-active="${nextActive ? "true" : "false"}"
      ${pending ? "disabled" : ""}
    >
      ${label}
    </button>
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
    return "pending";
  }
  if (window?.exhausted || remaining <= 0) {
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

function renderTokenInlineMeta(label, value, tone = "default") {
  return `
    <div class="token-inline-meta">
      <span class="token-inline-meta__label">${escapeHtml(label)}</span>
      <span class="token-inline-meta__value token-inline-meta__value--${tone}">${escapeHtml(value)}</span>
    </div>
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
      <div class="quota-meter quota-meter--pending">
        <div class="quota-meter__head">
          <div class="quota-meter__title">
            <span class="quota-meter__label">${escapeHtml(label)}</span>
            <span class="quota-meter__meta">未返回窗口</span>
          </div>
          <span class="quota-meter__value">—</span>
        </div>
        <div class="quota-meter__track"><span class="quota-meter__fill" style="width: 0%"></span></div>
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
    <div class="quota-meter quota-meter--${tone}">
      <div class="quota-meter__head">
        <div class="quota-meter__title">
          <span class="quota-meter__label">${escapeHtml(label)}</span>
          <span class="quota-meter__meta">${escapeHtml(metaText)}</span>
        </div>
        <span class="quota-meter__value">${escapeHtml(remainingLabel)}</span>
      </div>
      <div class="quota-meter__track"><span class="quota-meter__fill" style="width: ${fillWidth}%"></span></div>
    </div>
  `;
}

function renderQuotaSection(item) {
  const quota = item.quota;
  if (quota?.error) {
    return `
      <div class="token-row__quota" data-label="5h / 7d 配额">
        <p class="quota-error">${escapeHtml(quota.error)}</p>
        ${quota?.fetched_at ? `<p class="quota-meta">${escapeHtml(`最近尝试 ${formatDate(quota.fetched_at)}`)}</p>` : ""}
      </div>
    `;
  }

  return `
    <div class="token-row__quota" data-label="5h / 7d 配额">
      <div class="quota-meter-list">
        ${renderQuotaWindow(getQuotaWindow(item, "code-5h"), "5h")}
        ${renderQuotaWindow(getQuotaWindow(item, "code-7d"), "7d")}
      </div>
    </div>
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
      const accountId = item.chatgpt_account_id || item.account_id || "";
      const account = item.email || accountId || "未命名账号";
      const meta = accountId && accountId !== account ? accountId : item.source_file || "无来源记录";
      const planLabel = formatPlanType(item.plan_type);
      const planTone = derivePlanTone(item.plan_type);
      const subscriptionValue = formatSubscriptionUntil(item.subscription_active_until);
      const subscriptionTone = subscriptionValue === "—" ? "muted" : "default";
      const cooldownValue = formatCooldownUntil(item.cooldown_until);
      const lastUsedValue = formatDate(item.last_used_at);
      const cooldownTone = cooldownValue === "—" ? "muted" : "cooling";
      const lastUsedTone = lastUsedValue === "—" ? "muted" : "default";
      return `
        <article class="token-row">
          <div class="token-row__primary">
            <span class="token-row__account">${escapeHtml(account)}</span>
            <span class="token-row__meta">${escapeHtml(meta)}</span>
          </div>
          <div class="token-row__status" data-label="状态 / 计划 / 订阅">
            <div class="token-row__status-top">
              <div class="token-row__status-pills">
                <span class="status-pill status-pill--${status.tone}">${status.label}</span>
                <span class="status-pill status-pill--${planTone}">${escapeHtml(planLabel)}</span>
              </div>
              ${renderTokenActionButton(item)}
            </div>
            ${renderTokenInlineMeta("订阅", subscriptionValue, subscriptionTone)}
          </div>
          <div class="token-row__lifecycle" data-label="冷却到期 / 最近使用">
            <span class="token-lifecycle-value token-lifecycle-value--${cooldownTone}">${escapeHtml(cooldownValue)}</span>
            <span class="token-lifecycle-separator" aria-hidden="true">/</span>
            <span class="token-lifecycle-value token-lifecycle-value--${lastUsedTone}">${escapeHtml(lastUsedValue)}</span>
          </div>
          ${renderQuotaSection(item)}
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
    state.tokenItems = Array.isArray(data.items) ? data.items : [];
    elements.listNote.textContent = "显示最近 80 条记录 · 含计划、订阅与 5h/7d 配额";
    renderTokenSelection(data.selection || {});
    setTokenSelectionDisabled(state.tokenSelectionSaving);
    renderTokenList(state.tokenItems);
    if (data.counts) {
      renderCounts(data.counts);
    }
  } catch (error) {
    state.tokenItems = [];
    if (error.status === 401) {
      elements.listNote.textContent = "需要输入 Service API Key 才能查看明细和导入";
      if (elements.tokenSelectionSummary) {
        elements.tokenSelectionSummary.textContent = "需要 Service API Key";
      }
      setTokenSelectionDisabled(true);
      elements.tokenList.innerHTML = `
        <article class="empty-state">
          <p>管理接口已加锁。填入 Service API Key 后可查看明细和导入 key。</p>
        </article>
      `;
      return;
    }
    elements.listNote.textContent = `列表加载失败：${error.message}`;
    if (elements.tokenSelectionSummary) {
      elements.tokenSelectionSummary.textContent = "切换器同步失败";
    }
    setTokenSelectionDisabled(true);
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
    elements.listNote.textContent = "显示最近 80 条记录 · 含计划、订阅与 5h/7d 配额";
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

async function toggleTokenActivation(tokenId, nextActive) {
  const resolvedTokenId = Number(tokenId);
  if (!Number.isFinite(resolvedTokenId) || isTokenActionPending(resolvedTokenId)) {
    return;
  }

  state.tokenActionPendingIds.add(resolvedTokenId);
  renderTokenList(state.tokenItems);

  try {
    await fetchJson(`/admin/tokens/${resolvedTokenId}/activation`, {
      method: "POST",
      headers: authHeaders(true),
      body: JSON.stringify({ active: Boolean(nextActive) }),
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
    state.tokenActionPendingIds.delete(resolvedTokenId);
    renderTokenList(state.tokenItems);
  }
}

async function loadRequests() {
  try {
    const data = await fetchJson("/admin/requests?limit=80", {
      headers: authHeaders(),
    });
    elements.requestNote.textContent = "显示最近 80 条请求 · 图表为最近 24 小时 token 与估算金额";
    renderRequestSummary(data.summary || {});
    renderRequestAnalytics(data.analytics || {});
    renderRequestList(data.items || []);
  } catch (error) {
    if (error.status === 401) {
      elements.requestNote.textContent = "需要输入 Service API Key 才能查看请求日志";
      renderRequestSummary({});
      renderRequestAnalytics({});
      elements.requestList.innerHTML = `
        <article class="empty-state">
          <p>请求日志已加锁。填入 Service API Key 后可查看请求次数、token 用量、估算金额、模型名、端点、状态码和首字时间。</p>
        </article>
      `;
      return;
    }
    elements.requestNote.textContent = `请求日志加载失败：${error.message}`;
    renderRequestSummary({});
    renderRequestAnalytics({});
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
      `导入完成：新增 ${data.created_count || 0} 条，更新 ${data.updated_count || 0} 条，跳过重复 ${data.skipped_count || 0} 条，失败 ${data.failed_count || 0} 条。`,
      data.failed_count ? `失败原因：${(data.failed || []).slice(0, 3).map((item) => item.error).join("；")}` : "",
      data.skipped_count ? "重复判定基于当前 RT 和历史 RT 链，不会再按 account_id 合并不同账号。" : "",
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

function syncSystemTheme() {
  if (state.themePreference === "auto") {
    applyTheme("auto", { persist: false });
  }
}

applyTheme(state.themePreference, { persist: false });
renderTokenSelection({ strategy: state.tokenSelectionStrategy });

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

elements.tokenList.addEventListener("click", async (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const button = event.target.closest("[data-token-toggle]");
  if (!(button instanceof HTMLElement)) {
    return;
  }
  try {
    await toggleTokenActivation(button.dataset.tokenId, button.dataset.nextActive === "true");
  } catch {}
});

if (typeof state.themeMedia.addEventListener === "function") {
  state.themeMedia.addEventListener("change", syncSystemTheme);
} else if (typeof state.themeMedia.addListener === "function") {
  state.themeMedia.addListener(syncSystemTheme);
}

elements.refreshButton.addEventListener("click", refreshDashboard);
elements.saveKeyButton.addEventListener("click", saveServiceKey);
elements.clearKeyButton.addEventListener("click", clearServiceKey);
elements.importButton.addEventListener("click", importTokens);
elements.resetImportButton.addEventListener("click", resetImportForm);

refreshDashboard();
state.timer = window.setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
