import { api } from "./api.js";
import { state } from "./state.js";
import { $, button, escapeHTML, on, toast } from "./dom.js";

let refreshAll = async () => {};

export function initImportView(options = {}) {
  refreshAll = options.refreshAll || refreshAll;
  document.querySelectorAll("[data-import-queue-position]").forEach((node) => {
    on(node, "click", () => {
      state.import.queuePosition = node.dataset.importQueuePosition || "front";
      document.querySelectorAll("[data-import-queue-position]").forEach((buttonNode) => {
        buttonNode.setAttribute("aria-pressed", String(buttonNode === node));
      });
      const summary = $("import-queue-position-summary");
      if (summary) {
        summary.textContent = state.import.queuePosition === "front" ? "放在队列开头" : "放在队列最后";
      }
    });
  });
  button("import-button", importTokens);
  button("reset-import-button", () => {
    const textarea = $("token-json");
    const files = $("token-files");
    if (textarea) textarea.value = "";
    if (files) files.value = "";
    feedback("等待导入。");
  });
}

async function importTokens() {
  try {
    feedback("正在解析导入内容...");
    const entries = await collectEntries();
    if (!entries.length) {
      feedback("没有可导入的 token。", true);
      return;
    }
    feedback(`正在提交 ${entries.length} 条 token...`);
    const payload = {
      tokens: entries,
      import_queue_position: state.import.queuePosition,
    };
    const result = await api.importTokens(payload);
    const job = result.job || {};
    feedback(`导入任务 #${job.id || "-"} 已提交：新增 ${result.result?.created || 0}，更新 ${result.result?.updated || 0}，跳过 ${result.result?.skipped || 0}。`);
    toast("导入完成");
    await refreshAll();
  } catch (error) {
    feedback(error.message, true);
  }
}

async function collectEntries() {
  const values = [];
  const raw = $("token-json")?.value || "";
  values.push(...parseTokenText(raw));
  const files = Array.from($("token-files")?.files || []);
  for (const file of files) {
    const text = await file.text();
    values.push(...parseTokenText(text));
  }
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function parseTokenText(raw) {
  const text = raw.trim();
  if (!text) {
    return [];
  }
  const values = [];
  try {
    const parsed = JSON.parse(text);
    collectFromJSON(parsed, values);
    if (values.length) {
      return values;
    }
  } catch {}
  for (const line of text.split(/\r?\n/)) {
    const value = line.trim();
    if (!value || value.startsWith("#")) {
      continue;
    }
    if (value.includes(",") && !value.includes(".")) {
      const parts = value.split(",").map((part) => part.trim()).filter(Boolean);
      values.push(parts.at(-1));
      continue;
    }
    values.push(value);
  }
  return values;
}

function collectFromJSON(value, output) {
  if (!value) {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectFromJSON(item, output));
    return;
  }
  if (typeof value === "string") {
    output.push(value);
    return;
  }
  if (typeof value === "object") {
    for (const key of ["access_token", "accessToken", "refresh_token", "refreshToken", "token"]) {
      if (typeof value[key] === "string") {
        output.push(value[key]);
      }
    }
    for (const nested of Object.values(value)) {
      if (Array.isArray(nested)) {
        collectFromJSON(nested, output);
      }
    }
  }
}

function feedback(message, isError = false) {
  const node = $("import-feedback");
  if (node) {
    node.innerHTML = `<span class="${isError ? "error-text" : ""}">${escapeHTML(message)}</span>`;
  }
}
