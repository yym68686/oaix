import { api, getServiceKey, setServiceKey } from "./api.js";
import { $, button, on, text, toast } from "./dom.js";
import { initImportView } from "./importView.js";
import { loadRequests } from "./requestView.js";
import { initSettingsView, loadSettings } from "./settingsView.js";
import { initTokenView, loadTokens } from "./tokenView.js";

async function refreshAll() {
  await Promise.allSettled([loadHealth(), loadTokenSelection(), loadTokens(), loadRequests(), loadSettings()]);
  text("sync-chip", `已同步 ${new Date().toLocaleTimeString("zh-CN")}`);
}

async function loadHealth() {
  try {
    const payload = await api.health();
    const counts = payload.counts || {};
    text("protection-chip", payload.service_key_protected ? "已启用 Service API Key" : "未启用服务侧凭证");
    text("available-count", counts.available ?? "-");
    text("cooling-count", counts.cooling ?? "-");
    text("disabled-count", counts.disabled ?? "-");
  } catch (error) {
    text("protection-chip", `健康检查失败：${error.message}`);
  }
}

async function loadTokenSelection() {
  try {
    const payload = await api.tokenSelection();
    text("token-selection-summary", payload.strategy || "snapshot_round_robin");
    text("token-concurrency-summary", `每 Key ${payload.active_stream_cap || "-"} 并发`);
    const range = $("token-active-stream-cap-range");
    const input = $("token-active-stream-cap");
    if (range && payload.active_stream_cap) range.value = String(payload.active_stream_cap);
    if (input && payload.active_stream_cap) input.value = String(payload.active_stream_cap);
    text("token-plan-order-summary", "Go 网关按 snapshot selector 调度");
  } catch (error) {
    text("token-selection-summary", error.message);
  }
}

function initCredentialControls() {
  const input = $("service-key");
  if (input) {
    input.value = getServiceKey();
  }
  button("save-key-button", () => {
    setServiceKey(input?.value.trim() || "");
    toast("凭证已保存");
    refreshAll();
  });
  button("clear-key-button", () => {
    if (input) input.value = "";
    setServiceKey("");
    toast("凭证已清空");
    refreshAll();
  });
}

function initThemeControls() {
  const storageKey = "oaix.themePreference";
  const apply = (preference) => {
    const resolved =
      preference === "auto"
        ? window.matchMedia("(prefers-color-scheme: dark)").matches
          ? "dark"
          : "light"
        : preference;
    document.documentElement.dataset.themePreference = preference;
    document.documentElement.dataset.colorScheme = resolved;
    document.documentElement.style.colorScheme = resolved;
    text("theme-summary", preference === "auto" ? "自动 · 跟随系统" : preference === "dark" ? "暗色" : "亮色");
    document.querySelectorAll("[data-theme-option]").forEach((node) => {
      node.setAttribute("aria-pressed", String(node.dataset.themeOption === preference));
    });
  };
  document.querySelectorAll("[data-theme-option]").forEach((node) => {
    on(node, "click", () => {
      const preference = node.dataset.themeOption || "auto";
      try {
        window.localStorage.setItem(storageKey, preference);
      } catch {}
      apply(preference);
    });
  });
  apply(document.documentElement.dataset.themePreference || "auto");
}

function initDispatchControls() {
  const sync = () => {
    const value = $("token-active-stream-cap")?.value || $("token-active-stream-cap-range")?.value || "1";
    const range = $("token-active-stream-cap-range");
    const input = $("token-active-stream-cap");
    if (range) range.value = value;
    if (input) input.value = value;
    text("token-concurrency-summary", `每 Key ${value} 并发`);
  };
  on($("token-active-stream-cap-range"), "input", sync);
  on($("token-active-stream-cap"), "input", sync);
  button("token-active-stream-cap-save", async () => {
    await api.updateTokenSelection({ active_stream_cap: Number($("token-active-stream-cap")?.value || 1) });
    toast("调度设置已提交");
    await loadTokenSelection();
  });
}

function init() {
  initThemeControls();
  initCredentialControls();
  initDispatchControls();
  initSettingsView();
  initTokenView({ refreshAll });
  initImportView({ refreshAll });
  button("refresh-button", refreshAll);
  refreshAll();
  window.setInterval(refreshAll, 30_000);
}

init();
