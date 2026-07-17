import {
  AlertCircleIcon,
  CheckCircle2Icon,
  ChevronLeftIcon,
  ChevronRightIcon,
  DatabaseIcon,
  MoonIcon,
  SunIcon,
} from "lucide-react";
import type * as React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/registry/default/ui/empty";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "@/registry/default/ui/select";
import { Spinner } from "@/registry/default/ui/spinner";
import { cn } from "@/registry/default/lib/utils";
import type { TokenItem, TokenProbeResponse, TokenQuotaWindow } from "@/lib/api";
import { clamp, formatDate, formatNumber } from "@/lib/format";
import {
  formatPercent,
  formatUSD,
  probeOutcomeLabel,
  probeResultNeedsRawInspection,
  quotaWindowFor,
  tokenStatusLabel,
} from "./domain";
import type { ThemePreference, ToastMessage } from "./types";

export function SelectField({
  label,
  onChange,
  options,
  value,
}: {
  label: string;
  onChange: (value: string) => void;
  options: Array<{ label: string; value: string }>;
  value: string;
}) {
  return (
    <div className="grid min-w-0 gap-2">
      <Label>{label}</Label>
      <Select aria-label={label} items={options} onValueChange={(next) => onChange(String(next))} value={value}>
        <SelectTrigger className="min-w-0">
          <SelectValue />
        </SelectTrigger>
        <SelectPopup>
          {options.map((item) => (
            <SelectItem key={item.value} value={item.value}>
              {item.label}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
    </div>
  );
}

export function Pagination({
  onPageChange,
  page,
  total,
  totalPages,
}: {
  onPageChange: (page: number) => void;
  page: number;
  total: number;
  totalPages: number;
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/40 p-3">
      <Badge variant="secondary">
        第 {page} / {totalPages} 页，合计 {formatNumber(total)} 条
      </Badge>
      <div className="flex items-center gap-2">
        <Button disabled={page <= 1} onClick={() => onPageChange(page - 1)} size="sm" variant="outline">
          <ChevronLeftIcon />
          上一页
        </Button>
        <Input
          className="w-24"
          max={totalPages}
          min={1}
          nativeInput
          onChange={(event) => onPageChange(Number(event.currentTarget.value || 1))}
          type="number"
          value={page}
        />
        <Button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)} size="sm" variant="outline">
          下一页
          <ChevronRightIcon />
        </Button>
      </div>
    </div>
  );
}

export function MiniMetric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0 rounded-lg border bg-muted/40 p-3">
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className="mt-1 min-w-0 truncate oaix-tabular font-medium" title={typeof value === "string" ? value : undefined}>
        {typeof value === "number" ? formatNumber(value) : value || "-"}
      </div>
    </div>
  );
}

export function ThemeButton({
  current,
  icon,
  onSelect,
  value,
}: {
  current: ThemePreference;
  icon?: React.ReactNode;
  onSelect: (value: ThemePreference) => void;
  value: ThemePreference;
}) {
  const label = value === "auto" ? "自动" : value === "light" ? "亮色" : "暗色";
  const fallbackIcon = value === "light" ? <SunIcon /> : value === "dark" ? <MoonIcon /> : undefined;
  return (
    <Button onClick={() => onSelect(value)} size="sm" variant={current === value ? "secondary" : "ghost"}>
      {icon || fallbackIcon}
      {label}
    </Button>
  );
}

export function EmptyState({
  compact = false,
  description,
  title,
}: {
  compact?: boolean;
  description: string;
  title: string;
}) {
  return (
    <Empty className={cn(compact && "py-8")}>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <DatabaseIcon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

export function ErrorAlert({ message, title }: { message: string; title: string }) {
  return (
    <Alert variant="error">
      <AlertCircleIcon />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

export function LoadingState({
  compact = false,
  label = "正在载入数据",
}: {
  compact?: boolean;
  label?: string;
}) {
  return (
    <div
      aria-live="polite"
      className={cn(
        "flex min-h-40 flex-col items-center justify-center gap-3 rounded-lg border border-dashed bg-muted/24 text-muted-foreground",
        compact ? "py-8" : "py-14",
      )}
      role="status"
    >
      <Spinner className="size-6 text-foreground" />
      <span className="text-sm">{label}</span>
    </div>
  );
}

export function LoadingRows({ rows = 3 }: { rows?: number }) {
  void rows;
  return <LoadingState />;
}

export function ToastStack({ items }: { items: ToastMessage[] }) {
  return (
    <div className="fixed bottom-4 right-4 z-50 grid w-[min(420px,calc(100vw-2rem))] gap-2">
      {items.map((item) => (
        <Alert
          key={item.id}
          variant={item.variant === "success" ? "success" : item.variant === "warning" ? "warning" : item.variant === "error" ? "error" : "info"}
        >
          {item.variant === "success" ? <CheckCircle2Icon /> : <AlertCircleIcon />}
          <AlertTitle>{item.title}</AlertTitle>
        </Alert>
      ))}
    </div>
  );
}

export function TokenQuotaStrip({ quota }: { quota?: TokenItem["quota"] | null }) {
  if (!quota) {
    return <div className="rounded-md bg-muted/64 px-2 py-0.5 text-muted-foreground">额度更新中</div>;
  }
  if (quota.disabled || String(quota.error || "").toLowerCase().includes("deactivated_workspace")) {
    return <div className="rounded-md bg-destructive/10 px-2 py-0.5 text-destructive-foreground">工作区停用，已禁用</div>;
  }
  if (quota.error) {
    return (
      <div className="max-w-52 truncate rounded-md bg-warning/8 px-2 py-0.5 text-warning-foreground" title={quota.error}>
        额度错误：{quota.error}
      </div>
    );
  }
  const fiveHour = quotaWindowFor(quota, "5h");
  const weekly = quotaWindowFor(quota, "7d");
  return (
    <>
      <QuotaMeter label="5h" window={fiveHour} />
      <QuotaMeter label="7d" window={weekly} />
    </>
  );
}

function QuotaMeter({
  label,
  window,
}: {
  label: string;
  window?: TokenQuotaWindow;
}) {
  const used = Number(window?.used_percent ?? NaN);
  const remaining = Number(window?.remaining_percent ?? (Number.isFinite(used) ? 100 - used : NaN));
  const usedWidth = Number.isFinite(used) ? clamp(used, 0, 100) : 0;
  const tone = window?.exhausted || usedWidth >= 95 ? "bg-destructive" : usedWidth >= 80 ? "bg-warning" : "bg-success";
  const title = window
    ? `${label} 已用 ${formatPercent(used)}，剩余 ${formatPercent(remaining)}，重置 ${formatDate(window.reset_at)}`
    : `${label} quota 暂无数据`;
  return (
    <div className="inline-grid w-[7.25rem] grid-cols-[1.25rem_3.25rem_1.5rem] items-center gap-1 rounded-md border bg-muted/32 px-1.5 py-0.5" title={title}>
      <span className="font-medium">{label}</span>
      <span className="oaix-tabular text-muted-foreground">余 {formatPercent(remaining)}</span>
      <div className="h-1 w-6 overflow-hidden rounded-full bg-muted">
        <div className={cn("h-full rounded-full", tone)} style={{ width: `${usedWidth}%` }} />
      </div>
    </div>
  );
}

export function TokenConcurrency({ fallbackCap, item }: { fallbackCap: number; item: TokenItem }) {
  const active = Math.max(0, Number(item.active_streams || 0));
  const cap = Math.max(0, Number(item.active_stream_cap || fallbackCap || 0));
  const used = cap > 0 ? clamp((active / cap) * 100, 0, 100) : 0;
  return (
    <div className="inline-flex min-w-[5.8rem] items-center gap-1 rounded-md border bg-muted/32 px-2 py-0.5" title={`当前并发 ${active}，上限 ${cap || "-"}`}>
      <span className="text-muted-foreground">并发</span>
      <span className="oaix-tabular font-medium">
        {formatNumber(active)}/{cap ? formatNumber(cap) : "-"}
      </span>
      <div className="h-1 w-8 overflow-hidden rounded-full bg-muted">
        <div className={cn("h-full rounded-full", used >= 90 ? "bg-warning" : "bg-info")} style={{ width: `${used}%` }} />
      </div>
    </div>
  );
}

export function TokenObservedCost({
  value,
  local,
  remote,
  syncedAt,
  stale,
}: {
  value?: number | null;
  local?: number | null;
  remote?: number | null;
  syncedAt?: string | null;
  stale?: boolean;
}) {
  const amount = value === null || value === undefined ? 0 : Number(value);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const localAmount = Number.isFinite(Number(local)) ? Number(local) : amount;
  const remoteAmount = Number.isFinite(Number(remote)) ? Number(remote) : 0;
  const syncLabel = syncedAt ? `，Sub2API 同步于 ${formatDate(syncedAt)}${stale ? "（数据可能延迟）" : ""}` : stale ? "，Sub2API 数据正在同步" : "";
  const title = `总使用金额 ${formatUSD(amount)}（OAIX ${formatUSD(localAmount)} + Sub2API ${formatUSD(remoteAmount)}）${syncLabel}`;
  return (
    <div className="inline-flex min-w-[5.6rem] items-center gap-1 rounded-md border bg-muted/32 px-2 py-0.5" title={title}>
      <span className="text-muted-foreground">已用</span>
      <span className="oaix-tabular font-medium">{formatUSD(amount)}</span>
    </div>
  );
}

export function TokenProbeResult({ result }: { result: TokenProbeResponse }) {
  const text = result.message || result.detail || "测试完成";
  const rawResponse = String(result.raw_response || "");
  const showRawResponse = rawResponse !== "" || probeResultNeedsRawInspection(result);
  return (
    <div className="grid min-w-0 gap-2">
      <div className="flex min-w-0 items-start gap-2">
        <Badge size="sm" variant={probeBadgeVariant(result.outcome)}>
          {probeOutcomeLabel(result.outcome)}
          {result.status_code ? ` ${result.status_code}` : ""}
        </Badge>
        <span className="min-w-0 break-words text-muted-foreground [overflow-wrap:anywhere]" title={result.detail || text}>
          {text}
        </span>
      </div>
      {showRawResponse && (
        <div className="grid min-w-0 gap-1.5">
          <div className="font-medium text-foreground text-xs">上游原始响应</div>
          <pre
            aria-label="上游原始响应"
            className="max-h-72 min-w-0 overflow-auto whitespace-pre-wrap break-words rounded-lg border bg-background p-3 font-mono text-foreground text-xs [overflow-wrap:anywhere] oaix-scrollbar"
          >
            {rawResponse || "（未收到上游响应正文）"}
          </pre>
        </div>
      )}
    </div>
  );
}

export function TokenStatusPlan({
  plan,
  status,
}: {
  plan: string;
  status: "active" | "cooling" | "disabled";
}) {
  const planLabel = plan || "unknown";
  return (
    <div className="inline-grid min-w-[4.75rem] max-w-full gap-1">
      <Badge className="w-fit max-w-full" size="sm" variant={statusBadge(status)}>
        {tokenStatusLabel(status)}
      </Badge>
      <Badge className="w-fit max-w-full truncate" size="sm" title={planLabel} variant="secondary">
        {planLabel}
      </Badge>
    </div>
  );
}

export function statusBadge(status: "active" | "cooling" | "disabled"): React.ComponentProps<typeof Badge>["variant"] {
  if (status === "active") {
    return "success";
  }
  if (status === "cooling") {
    return "warning";
  }
  return "error";
}

export function probeBadgeVariant(outcome?: string): React.ComponentProps<typeof Badge>["variant"] {
  if (outcome === "reactivated") {
    return "success";
  }
  if (outcome === "cooling") {
    return "warning";
  }
  if (outcome === "disabled") {
    return "error";
  }
  return "secondary";
}

export function importBatchBadge(status?: string): React.ComponentProps<typeof Badge>["variant"] {
  switch (String(status || "").toLowerCase()) {
    case "completed":
      return "success";
    case "failed":
      return "error";
    case "canceled":
    case "cancelled":
      return "secondary";
    case "queued":
    case "running":
      return "warning";
    default:
      return "outline";
  }
}
