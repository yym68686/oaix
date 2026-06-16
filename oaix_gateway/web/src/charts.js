import { escapeHTML, formatNumber, html, text } from "./dom.js";

export function renderAnalytics(payload) {
  const summary = payload?.summary || {};
  text("request-total-count", formatNumber(summary.total));
  text("request-success-count", formatNumber(summary.success));
  text("request-failed-count", formatNumber(summary.failure));
  text("request-avg-ttft", `${Math.round(summary.average_ttft_ms || 0)} ms`);
  const totalTokens = (summary.input_tokens || 0) + (summary.output_tokens || 0) + (summary.total_tokens || 0);
  text("request-total-tokens", formatNumber(totalTokens));
  text("request-estimated-cost", `$${Number(summary.estimated_cost_usd || 0).toFixed(4)}`);
  renderModelBars(payload?.by_model || []);
}

function renderModelBars(models) {
  const rows = models.slice(0, 10);
  const max = Math.max(1, ...rows.map((item) => item.requests || 0));
  const tokenTotal = rows.reduce((sum, item) => sum + (item.requests || 0), 0);
  text("request-token-chart-total", formatNumber(tokenTotal));
  html(
    "request-token-chart",
    rows.length
      ? rows
          .map((item) => {
            const width = Math.max(4, Math.round(((item.requests || 0) / max) * 100));
            return `<div class="chart-row">
              <span class="chart-row__label">${escapeHTML(item.model_name || "unknown")}</span>
              <span class="chart-row__bar"><i style="width:${width}%"></i></span>
              <strong>${formatNumber(item.requests)}</strong>
            </div>`;
          })
          .join("")
      : `<article class="empty-state"><p>暂无请求统计。</p></article>`,
  );
  text("request-token-chart-note", `最近 24 小时 ${formatNumber(tokenTotal)} 次模型请求。`);
  text("request-cost-chart-total", formatNumber(rows.length));
  html(
    "request-cost-chart",
    rows.length
      ? rows
          .map((item) => {
            const failures = item.failure || 0;
            const total = Math.max(1, item.requests || 0);
            const width = Math.max(4, Math.round((failures / total) * 100));
            return `<div class="chart-row chart-row--cost">
              <span class="chart-row__label">${escapeHTML(item.model_name || "unknown")}</span>
              <span class="chart-row__bar"><i style="width:${width}%"></i></span>
              <strong>${formatNumber(failures)}</strong>
            </div>`;
          })
          .join("")
      : `<article class="empty-state"><p>暂无失败统计。</p></article>`,
  );
  text("request-cost-chart-note", "条形长度表示各模型失败请求占比。");
}
