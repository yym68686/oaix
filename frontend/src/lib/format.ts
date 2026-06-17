export function formatNumber(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return "-";
  }
  return new Intl.NumberFormat("zh-CN").format(number);
}

export function formatDate(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return date.toLocaleString("zh-CN", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "2-digit",
  });
}

export function formatCurrency(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return "$0.0000";
  }
  return `$${number.toFixed(4)}`;
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
