import { api } from "./api.js";
import { $, escapeHTML, html, on, setError, setLoading, toast } from "./dom.js";

export function initSettingsView() {
  ensureSettingsPanel();
  on($("settings-refresh"), "click", loadSettings);
  on($("settings-save"), "click", saveSetting);
}

export async function loadSettings() {
  setLoading("settings-list", "正在加载设置...");
  try {
    const payload = await api.settings();
    const items = payload.items || [];
    html(
      "settings-list",
      items.length
        ? items
            .map((item) => `<article class="settings-row">
              <strong>${escapeHTML(item.key)}</strong>
              <code>${escapeHTML(JSON.stringify(item.value ?? null))}</code>
              <span>${escapeHTML(item.updated_at || "")}</span>
            </article>`)
            .join("")
        : `<article class="empty-state"><p>暂无设置项。</p></article>`,
    );
  } catch (error) {
    setError("settings-list", error.message);
  }
}

async function saveSetting() {
  const key = $("settings-key")?.value.trim();
  const raw = $("settings-value")?.value.trim();
  if (!key) {
    toast("请输入设置 key", "warn");
    return;
  }
  let value = raw;
  if (raw) {
    try {
      value = JSON.parse(raw);
    } catch {
      value = raw;
    }
  }
  await api.updateSetting(key, value);
  toast("设置已保存");
  await loadSettings();
}

function ensureSettingsPanel() {
  if ($("settings-panel")) {
    return;
  }
  const main = document.querySelector("main.layout");
  if (!main) {
    return;
  }
  const section = document.createElement("section");
  section.className = "settings-panel reveal";
  section.id = "settings-panel";
  section.innerHTML = `
    <div class="section-head">
      <div>
        <p class="section-kicker">Settings</p>
        <h2>运行设置</h2>
      </div>
      <button class="ghost-button" id="settings-refresh" type="button">刷新</button>
    </div>
    <div class="settings-editor">
      <label class="field">
        <span class="field__label">Key</span>
        <input id="settings-key" type="text" placeholder="例如 token_selection" />
      </label>
      <label class="field">
        <span class="field__label">Value JSON</span>
        <textarea id="settings-value" rows="4" spellcheck="false" placeholder='{"enabled":true}'></textarea>
      </label>
      <button class="primary-button" id="settings-save" type="button">保存设置</button>
    </div>
    <div class="settings-list" id="settings-list"></div>
  `;
  main.appendChild(section);
}
