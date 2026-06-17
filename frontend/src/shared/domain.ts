import type {
  ImportBatch,
  TokenCounts,
  TokenItem,
  TokenPlanCount,
  TokenProbeResponse,
  TokenQuotaWindow,
} from "@/lib/api";
import { clamp, formatDate } from "@/lib/format";
import type { ImportEntry, ThemePreference, ToastMessage, TokenStatus } from "./types";

export const PAGE_SIZE = 100;

export const STATUS_OPTION_DEFINITIONS: Array<{ label: string; value: TokenStatus }> = [
  { label: "全部", value: "all" },
  { label: "有效", value: "available" },
  { label: "冷却", value: "cooling" },
  { label: "禁用", value: "disabled" },
];

export const SORT_OPTIONS = [
  { label: "最新入库", value: "-created_at" },
  { label: "最早入库", value: "created_at" },
  { label: "最近使用", value: "-last_used_at" },
  { label: "状态优先", value: "status" },
];

export function tokenTitle(item: TokenItem): string {
  return item.email || item.account_id || `Token #${item.id}`;
}

export function tokenPlanType(item: TokenItem): string {
  return item.quota?.plan_type || item.plan_type || "unknown";
}

export function tokenStatusOf(item: TokenItem): "active" | "cooling" | "disabled" {
  const serverStatus = String(item.status || "").trim().toLowerCase();
  if (serverStatus === "active" || serverStatus === "available") {
    return "active";
  }
  if (serverStatus === "cooling" || serverStatus === "cooldown") {
    return "cooling";
  }
  if (serverStatus === "disabled" || serverStatus === "inactive") {
    return "disabled";
  }
  if (!item.is_active || item.disabled_at) {
    return "disabled";
  }
  if (item.cooldown_until) {
    const until = new Date(item.cooldown_until).getTime();
    if (!Number.isNaN(until) && until > Date.now()) {
      return "cooling";
    }
  }
  return "active";
}

export function tokenStatusLabel(status: "active" | "cooling" | "disabled"): string {
  if (status === "active") {
    return "有效";
  }
  if (status === "cooling") {
    return "冷却";
  }
  return "禁用";
}

export function importBatchStatusLabel(status?: string): string {
  switch (String(status || "").toLowerCase()) {
    case "completed":
      return "已完成";
    case "running":
      return "运行中";
    case "queued":
      return "排队中";
    case "failed":
      return "失败";
    case "canceled":
    case "cancelled":
      return "已取消";
    default:
      return status || "未知";
  }
}

export function importBatchCancelable(status?: string): boolean {
  return ["queued", "running", "validating", "publishing"].includes(String(status || "").toLowerCase());
}

export function quotaWindowFor(
  quota: NonNullable<TokenItem["quota"]>,
  label: "5h" | "7d",
): TokenQuotaWindow | undefined {
  return quota.windows?.find((item) => item.label === label || item.id.endsWith(label));
}

export function formatPercent(value: unknown): string {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "-";
  }
  return `${Math.round(clamp(number, 0, 100))}%`;
}

export function formatUSD(value: number): string {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "-";
  }
  const fractionDigits = Math.abs(amount) > 0 && Math.abs(amount) < 1 ? 4 : 2;
  return new Intl.NumberFormat("en-US", {
    currency: "USD",
    maximumFractionDigits: fractionDigits,
    minimumFractionDigits: fractionDigits,
    style: "currency",
  }).format(amount);
}

export function formatUSDOptional(value: unknown): string {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "-";
  }
  return formatUSD(amount);
}

export function probeOutcomeLabel(outcome?: string): string {
  if (outcome === "reactivated") {
    return "通过";
  }
  if (outcome === "cooling") {
    return "冷却";
  }
  if (outcome === "disabled") {
    return "禁用";
  }
  return "未定";
}

export function probeToastVariant(outcome?: string): ToastMessage["variant"] {
  if (outcome === "reactivated") {
    return "success";
  }
  if (outcome === "cooling") {
    return "warning";
  }
  if (outcome === "disabled") {
    return "error";
  }
  return "info";
}

export function statusOptionsWithCounts(counts: TokenCounts): Array<{ label: string; value: TokenStatus }> {
  const valueByStatus: Record<TokenStatus, number> = {
    all: counts.total || 0,
    available: counts.available ?? counts.active ?? 0,
    cooling: counts.cooling || 0,
    disabled: counts.disabled || 0,
  };
  return STATUS_OPTION_DEFINITIONS.map((option) => ({
    ...option,
    label: `${option.label} ${formatNumber(valueByStatus[option.value])}`,
  }));
}

export function planOptionsWithCounts(planCounts: TokenPlanCount[]): Array<{ label: string; value: string }> {
  const total = planCounts.reduce((sum, item) => sum + (Number(item.count) || 0), 0);
  const options = [
    {
      label: `全部计划 ${formatNumber(total)}`,
      value: "all",
    },
  ];
  const seen = new Set<string>();
  for (const item of planCounts) {
    const value = normalizePlanValue(item.plan);
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    options.push({
      label: `${item.label || planLabel(value)} ${formatNumber(item.count || 0)}`,
      value,
    });
  }
  for (const value of ["free", "plus", "team", "pro", "unknown"]) {
    if (!seen.has(value)) {
      options.push({
        label: `${planLabel(value)} 0`,
        value,
      });
    }
  }
  return options;
}

export function normalizePlanValue(value: string): string {
  return String(value || "").trim().toLowerCase();
}

export function planLabel(value: string): string {
  switch (normalizePlanValue(value)) {
    case "free":
      return "Free";
    case "plus":
      return "Plus";
    case "team":
      return "Team";
    case "pro":
      return "Pro";
    case "unknown":
    case "":
      return "Unknown";
    default:
      return value;
  }
}

export function sortParam(value: string): string {
  switch (value) {
    case "created_at":
      return "oldest";
    case "-last_used_at":
      return "last_used";
    case "status":
      return "available";
    default:
      return "";
  }
}

export function readThemePreference(): ThemePreference {
  const raw = document.documentElement.dataset.themePreference || localStorage.getItem("oaix.themePreference");
  return raw === "light" || raw === "dark" || raw === "auto" ? raw : "auto";
}

export function applyTheme(preference: ThemePreference): void {
  localStorage.setItem("oaix.themePreference", preference);
  const resolved =
    preference === "auto"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"
      : preference;
  document.documentElement.classList.toggle("dark", resolved === "dark");
  document.documentElement.dataset.themePreference = preference;
  const meta = document.querySelector('meta[name="theme-color"]');
  meta?.setAttribute("content", resolved === "dark" ? "#0a0a0a" : "#ffffff");
}

export async function collectImportEntries(raw: string, files?: FileList | null): Promise<ImportEntry[]> {
  const values = [...parseTokenText(raw)];
  for (const file of Array.from(files || [])) {
    values.push(...parseTokenText(await file.text()));
  }
  const seen = new Set<string>();
  const out: ImportEntry[] = [];
  for (const value of values) {
    const key = importEntryKey(value);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(value);
  }
  return out;
}

export function parseTokenText(raw: string): ImportEntry[] {
  const text = raw.trim();
  if (!text) {
    return [];
  }
  const values: ImportEntry[] = [];
  try {
    collectFromJSON(JSON.parse(text), values);
    if (values.length) {
      return values;
    }
  } catch {}
  for (const line of text.split(/\r?\n/)) {
    const value = line.trim();
    if (!value || value.startsWith("#")) {
      continue;
    }
    if (value.includes(",")) {
      const parts = value.split(",").map((part) => part.trim()).filter(Boolean);
      if (parts.length >= 2) {
        values.push({ account_id: parts[0], refresh_token: parts[parts.length - 1] });
      } else if (parts.length) {
        values.push(parts[0]);
      }
      continue;
    }
    values.push(value);
  }
  return values;
}

function collectFromJSON(value: unknown, output: ImportEntry[]): void {
  if (!value) {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectFromJSON(item, output));
    return;
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (text) {
      output.push(text);
    }
    return;
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    const payload = importPayloadFromRecord(record);
    if (payload) {
      output.push(payload);
      return;
    }
    for (const nested of Object.values(record)) {
      if (Array.isArray(nested) || (nested && typeof nested === "object")) {
        collectFromJSON(nested, output);
      }
    }
  }
}

function importPayloadFromRecord(record: Record<string, unknown>): Record<string, unknown> | null {
  const payload: Record<string, unknown> = {};
  for (const key of [
    "access_token",
    "accessToken",
    "refresh_token",
    "refreshToken",
    "token",
    "account_id",
    "chatgpt_account_id",
    "id_token",
    "idToken",
    "email",
    "plan_type",
    "type",
    "is_active",
  ]) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      payload[key] = value.trim();
    } else if (typeof value === "boolean") {
      payload[key] = value;
    }
  }
  return Object.keys(payload).some((key) => ["access_token", "accessToken", "refresh_token", "refreshToken", "token"].includes(key))
    ? payload
    : null;
}

function importEntryKey(value: ImportEntry): string {
  if (typeof value === "string") {
    return value.trim();
  }
  const sorted = Object.keys(value)
    .sort()
    .reduce<Record<string, unknown>>((acc, key) => {
      acc[key] = value[key];
      return acc;
    }, {});
  return JSON.stringify(sorted);
}

export function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error || "请求失败");
}

export function importBatchTotal(job: ImportBatch): number {
  return Number(job.token_count || job.total_count || 0);
}

export function tokenFreshnessTitle(item: TokenItem): string {
  return `最近 ${formatDate(item.last_used_at)}，冷却 ${formatDate(item.cooldown_until)}`;
}

function formatNumber(value: unknown): string {
  const number = Number(value || 0);
  return Number.isFinite(number) ? new Intl.NumberFormat("en-US").format(number) : "0";
}
