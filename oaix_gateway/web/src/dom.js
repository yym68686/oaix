export function $(id) {
  return document.getElementById(id);
}

export function text(id, value) {
  const node = $(id);
  if (node) {
    node.textContent = value == null || value === "" ? "-" : String(value);
  }
}

export function html(id, value) {
  const node = $(id);
  if (node) {
    node.innerHTML = value;
  }
}

export function on(node, event, handler) {
  if (node) {
    node.addEventListener(event, handler);
  }
}

export function button(id, handler) {
  on($(id), "click", handler);
}

export function formatNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString("zh-CN") : "-";
}

export function formatDate(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

export function setLoading(id, message) {
  html(
    id,
    `<article class="loading-state" role="status" aria-live="polite">
      <span class="loading-state__spinner" aria-hidden="true"></span>
      <p class="loading-state__label">${escapeHTML(message)}</p>
    </article>`,
  );
}

export function setError(id, message) {
  html(
    id,
    `<article class="empty-state empty-state--error" role="status">
      <p>${escapeHTML(message)}</p>
    </article>`,
  );
}

export function toast(message, tone = "info") {
  const stack = $("toast-stack");
  if (!stack) {
    return;
  }
  const normalized = tone === "warn" ? "warning" : tone;
  const item = document.createElement("div");
  item.className = `toast toast--${normalized}`;
  item.innerHTML = `<div class="toast__body"><p class="toast__message">${escapeHTML(message)}</p></div>`;
  stack.appendChild(item);
  window.requestAnimationFrame(() => item.classList.add("is-visible"));
  window.setTimeout(() => item.remove(), 4200);
}

export function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function debounce(fn, delay = 250) {
  let timer = 0;
  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), delay);
  };
}
