import { api } from "./api.js";
import { state, sortParam, tokenOffset } from "./state.js";
import { $, button, debounce, escapeHTML, formatDate, formatNumber, html, on, setError, setLoading, text, toast } from "./dom.js";

let refreshAll = async () => {};
let virtual = null;

export function initTokenView(options = {}) {
  refreshAll = options.refreshAll || refreshAll;
  setupFilters();
  setupPagination();
  setupBatchBar();
  setupDialogs();
  document.querySelectorAll("[data-token-view-mode]").forEach((node) => {
    on(node, "click", async () => {
      state.token.mode = node.dataset.tokenViewMode || "keys";
      document.querySelectorAll("[data-token-view-mode]").forEach((buttonNode) => {
        buttonNode.setAttribute("aria-pressed", String(buttonNode === node));
      });
      if (state.token.mode === "import_batches") {
        await loadImportJobs();
      } else {
        await loadTokens();
      }
    });
  });
}

export async function loadTokens() {
  if (state.token.mode !== "keys") {
    return loadImportJobs();
  }
  setLoading("token-list", "正在加载 key 池...");
  try {
    const params = new URLSearchParams({
      limit: String(state.token.limit),
      offset: String(tokenOffset()),
    });
    if (state.token.query) params.set("q", state.token.query);
    if (state.token.status) params.set("status", state.token.status);
    const sort = sortParam(state.token.sort);
    if (sort) params.set("sort", sort);
    const payload = await api.listTokens(params);
    state.token.items = payload.items || [];
    state.token.total = payload.pagination?.total || state.token.items.length;
    state.token.selected.clear();
    renderCounts(payload.counts || {});
    renderTokenPagination();
    renderVirtualTokens();
  } catch (error) {
    setError("token-list", error.message);
    text("list-note", "Key 池载入失败");
  }
}

export async function loadImportJobs() {
  setLoading("token-list", "正在加载导入批次...");
  try {
    const payload = await api.importJobs(50);
    const jobs = payload.items || [];
    text("list-note", `最近 ${jobs.length} 个导入批次`);
    html(
      "token-list",
      jobs.length
        ? jobs
            .map((job) => `<article class="token-card token-card--job">
              <div class="token-card__main">
                <strong>Job #${job.id}</strong>
                <span>${escapeHTML(job.status)}</span>
                <span>${formatDate(job.submitted_at)}</span>
              </div>
              <div class="token-card__meta">
                <span>总量 ${formatNumber(job.total_count)}</span>
                <span>已处理 ${formatNumber(job.processed_count)}</span>
                <span>新增 ${formatNumber(job.created_count)}</span>
                <span>更新 ${formatNumber(job.updated_count)}</span>
                <span>失败 ${formatNumber(job.failed_count)}</span>
              </div>
              <div class="token-card__actions">
                <button class="ghost-button" data-cancel-job="${job.id}" type="button">取消</button>
              </div>
            </article>`)
            .join("")
        : `<article class="empty-state"><p>暂无导入批次。</p></article>`,
    );
    document.querySelectorAll("[data-cancel-job]").forEach((node) => {
      on(node, "click", async () => {
        await api.cancelImportJob(node.dataset.cancelJob);
        toast("已发送取消请求");
        await loadImportJobs();
      });
    });
  } catch (error) {
    setError("token-list", error.message);
  }
}

function setupFilters() {
  const statusFilters = [
    ["", "全部"],
    ["available", "有效"],
    ["cooling", "冷却"],
    ["disabled", "禁用"],
  ];
  html(
    "token-status-filters",
    statusFilters
      .map(([value, label]) => `<button class="filter-chip" data-status="${value}" type="button" aria-pressed="${value === ""}">${label}</button>`)
      .join(""),
  );
  document.querySelectorAll("[data-status]").forEach((node) => {
    on(node, "click", async () => {
      state.token.status = node.dataset.status || "";
      state.token.page = 1;
      document.querySelectorAll("[data-status]").forEach((buttonNode) => {
        buttonNode.setAttribute("aria-pressed", String(buttonNode === node));
      });
      await loadTokens();
    });
  });
  html("token-plan-filters", `<button class="filter-chip" type="button" aria-pressed="true">全部计划</button>`);
  on($("token-search-input"), "input", debounce(async (event) => {
    state.token.query = event.target.value.trim();
    state.token.page = 1;
    await loadTokens();
  }));
  on($("token-sort-select"), "change", async (event) => {
    state.token.sort = event.target.value;
    state.token.page = 1;
    await loadTokens();
  });
}

function setupPagination() {
  button("token-page-first", async () => {
    state.token.page = 1;
    await loadTokens();
  });
  button("token-page-prev", async () => {
    state.token.page = Math.max(1, state.token.page - 1);
    await loadTokens();
  });
  button("token-page-next", async () => {
    const max = maxPage();
    state.token.page = Math.min(max, state.token.page + 1);
    await loadTokens();
  });
  button("token-page-last", async () => {
    state.token.page = maxPage();
    await loadTokens();
  });
  on($("token-page-input"), "change", async (event) => {
    state.token.page = clampPage(Number(event.target.value || 1));
    await loadTokens();
  });
}

function setupBatchBar() {
  const list = $("token-list");
  if (!list?.parentElement || $("token-batch-bar")) {
    return;
  }
  const bar = document.createElement("div");
  bar.className = "token-batch-bar";
  bar.id = "token-batch-bar";
  bar.innerHTML = `
    <button class="ghost-button" id="token-select-page" type="button">全选本页</button>
    <button class="ghost-button" id="token-batch-enable" type="button">启用</button>
    <button class="ghost-button" id="token-batch-disable" type="button">禁用</button>
    <button class="danger-button" id="token-batch-delete" type="button">删除</button>
    <span id="token-batch-summary">未选择</span>
  `;
  list.parentElement.insertBefore(bar, list);
  button("token-select-page", () => {
    state.token.items.forEach((item) => state.token.selected.add(item.id));
    renderVirtualTokens();
  });
  button("token-batch-enable", () => batchActivation(true));
  button("token-batch-disable", () => batchActivation(false));
  button("token-batch-delete", () => batchDelete());
}

function renderCounts(counts) {
  text("available-count", formatNumber(counts.available));
  text("cooling-count", formatNumber(counts.cooling));
  text("disabled-count", formatNumber(counts.disabled));
  text("total-count", formatNumber(counts.total));
  text("legend-available", formatNumber(counts.available));
  text("legend-cooling", formatNumber(counts.cooling));
  text("legend-disabled", formatNumber(counts.disabled));
  const total = Math.max(1, counts.total || 0);
  setBar("bar-available", counts.available, total);
  setBar("bar-cooling", counts.cooling, total);
  setBar("bar-disabled", counts.disabled, total);
  text("list-note", `显示 ${state.token.items.length} / ${formatNumber(state.token.total)} 个 key`);
  text("token-search-summary", state.token.query ? `按 "${state.token.query}" 搜索` : "全池搜索");
}

function setBar(id, value, total) {
  const node = $(id);
  if (node) {
    node.style.width = `${Math.round(((value || 0) / total) * 100)}%`;
  }
}

function renderTokenPagination() {
  const max = maxPage();
  const input = $("token-page-input");
  if (input) {
    input.value = String(state.token.page);
    input.max = String(max);
  }
  text("token-pagination-summary", `第 ${state.token.page} / ${max} 页，合计 ${formatNumber(state.token.total)} 条`);
}

function renderVirtualTokens() {
  const root = $("token-list");
  if (!root) {
    return;
  }
  if (!state.token.items.length) {
    root.innerHTML = `<article class="empty-state"><p>没有匹配的 key。</p></article>`;
    updateBatchSummary();
    return;
  }
  root.style.maxHeight = "720px";
  root.style.overflow = "auto";
  root.innerHTML = `<div class="virtual-list-spacer"></div>`;
  const spacer = root.firstElementChild;
  const rowHeight = 142;
  spacer.style.height = `${state.token.items.length * rowHeight}px`;
  virtual = () => {
    const start = Math.max(0, Math.floor(root.scrollTop / rowHeight) - 4);
    const visible = Math.ceil(root.clientHeight / rowHeight) + 8;
    const slice = state.token.items.slice(start, start + visible);
    spacer.innerHTML = `<div style="transform:translateY(${start * rowHeight}px)">
      ${slice.map(renderTokenCard).join("")}
    </div>`;
    bindTokenActions();
    updateBatchSummary();
  };
  on(root, "scroll", () => virtual?.());
  virtual();
}

function renderTokenCard(item) {
  const checked = state.token.selected.has(item.id) ? "checked" : "";
  const status = item.is_active ? (item.cooldown_until ? "cooling" : "active") : "disabled";
  const title = item.email || item.account_id || `Token #${item.id}`;
  return `<article class="token-card token-card--${status}">
    <label class="token-card__select">
      <input type="checkbox" data-token-select="${item.id}" ${checked} />
    </label>
    <div class="token-card__main">
      <strong>${escapeHTML(title)}</strong>
      <span>ID ${item.id}</span>
      <span>${escapeHTML(item.plan_type || "unknown")}</span>
    </div>
    <div class="token-card__meta">
      <span>状态 ${escapeHTML(status)}</span>
      <span>最近使用 ${formatDate(item.last_used_at)}</span>
      <span>冷却至 ${formatDate(item.cooldown_until)}</span>
      <span>${escapeHTML(item.remark || item.source_file || "")}</span>
    </div>
    <div class="token-card__actions">
      <button class="ghost-button" data-token-toggle="${item.id}" data-active="${item.is_active ? "false" : "true"}" type="button">${item.is_active ? "禁用" : "启用"}</button>
      <button class="ghost-button" data-token-remark="${item.id}" type="button">备注</button>
      <button class="danger-button" data-token-delete="${item.id}" type="button">删除</button>
    </div>
  </article>`;
}

function bindTokenActions() {
  document.querySelectorAll("[data-token-select]").forEach((node) => {
    on(node, "change", () => {
      const id = Number(node.dataset.tokenSelect);
      if (node.checked) {
        state.token.selected.add(id);
      } else {
        state.token.selected.delete(id);
      }
      updateBatchSummary();
    });
  });
  document.querySelectorAll("[data-token-toggle]").forEach((node) => {
    on(node, "click", async () => {
      await api.updateActivation(node.dataset.tokenToggle, { active: node.dataset.active === "true", clear_cooldown: true });
      toast("Key 状态已更新");
      await refreshAll();
    });
  });
  document.querySelectorAll("[data-token-delete]").forEach((node) => {
    on(node, "click", () => openDeleteDialog(Number(node.dataset.tokenDelete)));
  });
  document.querySelectorAll("[data-token-remark]").forEach((node) => {
    on(node, "click", () => openRemarkDialog(Number(node.dataset.tokenRemark)));
  });
}

function updateBatchSummary() {
  text("token-batch-summary", state.token.selected.size ? `已选择 ${state.token.selected.size} 个` : "未选择");
}

async function batchActivation(active) {
  const ids = [...state.token.selected];
  if (!ids.length) {
    toast("请先选择 key", "warn");
    return;
  }
  await api.batchTokens({
    action: "set_active",
    token_ids: ids,
    active,
    clear_cooldown: true,
  });
  toast(active ? "批量启用完成" : "批量禁用完成");
  await refreshAll();
}

async function batchDelete() {
  const ids = [...state.token.selected];
  if (!ids.length || !window.confirm(`删除 ${ids.length} 个 key？`)) {
    return;
  }
  await api.batchTokens({ action: "delete", token_ids: ids });
  toast("批量删除完成");
  await refreshAll();
}

function setupDialogs() {
  button("token-delete-close", closeDeleteDialog);
  button("token-delete-cancel", closeDeleteDialog);
  button("token-delete-confirm", async () => {
    const id = Number($("token-delete-dialog")?.dataset.tokenId);
    if (!id) return;
    await api.deleteToken(id);
    closeDeleteDialog();
    toast("Key 已删除");
    await refreshAll();
  });
  button("token-remark-close", closeRemarkDialog);
  button("token-remark-cancel", closeRemarkDialog);
  button("token-remark-clear", () => {
    const node = $("token-remark-textarea");
    if (node) node.value = "";
  });
  button("token-remark-save", async () => {
    const id = Number($("token-remark-dialog")?.dataset.tokenId);
    if (!id) return;
    await api.updateRemark(id, $("token-remark-textarea")?.value || "");
    closeRemarkDialog();
    toast("备注已保存");
    await refreshAll();
  });
}

function openDeleteDialog(id) {
  const token = state.token.items.find((item) => item.id === id);
  const dialog = $("token-delete-dialog");
  if (!dialog) return;
  dialog.dataset.tokenId = String(id);
  text("token-delete-target", token?.email || token?.account_id || `Token #${id}`);
  text("token-delete-meta", token?.remark || token?.source_file || "-");
  dialog.showModal();
}

function closeDeleteDialog() {
  $("token-delete-dialog")?.close();
}

function openRemarkDialog(id) {
  const token = state.token.items.find((item) => item.id === id);
  const dialog = $("token-remark-dialog");
  if (!dialog) return;
  dialog.dataset.tokenId = String(id);
  text("token-remark-target", token?.email || token?.account_id || `Token #${id}`);
  text("token-remark-meta", token?.source_file || "-");
  const textarea = $("token-remark-textarea");
  if (textarea) textarea.value = token?.remark || "";
  dialog.showModal();
}

function closeRemarkDialog() {
  $("token-remark-dialog")?.close();
}

function maxPage() {
  return Math.max(1, Math.ceil(state.token.total / state.token.limit));
}

function clampPage(value) {
  return Math.min(maxPage(), Math.max(1, Number.isFinite(value) ? value : 1));
}
