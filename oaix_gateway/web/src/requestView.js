import { api } from "./api.js";
import { escapeHTML, formatDate, formatNumber, html, setError, setLoading, text } from "./dom.js";
import { renderAnalytics } from "./charts.js";

export async function loadRequests() {
  setLoading("request-list", "正在加载请求日志...");
  try {
    const [requests, analytics] = await Promise.all([api.requests(80), api.analytics(24)]);
    renderAnalytics(analytics);
    renderRequestSummary(requests.summary || {});
    renderRequestList(requests.items || []);
  } catch (error) {
    setError("request-list", error.message);
    text("request-note", "请求日志载入失败");
  }
}

function renderRequestSummary(summary) {
  text("request-total-count", formatNumber(summary.total));
  text("request-success-count", formatNumber(summary.success));
  text("request-failed-count", formatNumber(summary.failure));
  text("request-avg-ttft", `${Math.round(summary.average_ttft_ms || 0)} ms`);
}

function renderRequestList(items) {
  text("request-note", `最近 ${items.length} 条请求`);
  html(
    "request-list",
    items.length
      ? items
          .map((item) => `<article class="request-row request-row--${item.success === false ? "failed" : "ok"}">
            <span>${formatDate(item.started_at)}</span>
            <strong>${escapeHTML(item.model_name || item.model || "-")}</strong>
            <span>${escapeHTML(item.endpoint || "-")}</span>
            <span>${escapeHTML(item.prompt_cache_source || "-")}</span>
            <span>${item.status_code || "-"}</span>
            <span>${item.ttft_ms == null ? "-" : `${item.ttft_ms} ms`}</span>
            <span>${item.attempt_count || 0}</span>
            <span title="${escapeHTML(item.error_message || "")}">${escapeHTML(item.error_message || "-")}</span>
          </article>`)
          .join("")
      : `<article class="empty-state"><p>暂无请求日志。</p></article>`,
  );
}
