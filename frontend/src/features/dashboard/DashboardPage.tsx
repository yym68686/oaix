import {
  ActivityIcon,
  ChartColumnBigIcon,
  CoinsIcon,
  GaugeIcon,
  LayoutDashboardIcon,
  TimerResetIcon,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import {
  api,
  getServiceKey,
  isAuthContextPending,
  type DashboardData,
  type DashboardModel,
  type DashboardPeriod,
  type DashboardRange,
  type DashboardTrendPoint,
} from "@/lib/api";
import { clamp, formatCurrency, formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";

const RANGE_OPTIONS: Array<{ label: string; value: DashboardRange }> = [
  { label: "今天", value: "today" },
  { label: "近 7 天", value: "week" },
  { label: "近 30 天", value: "month" },
  { label: "今年", value: "year" },
];

const PERIOD_LABELS: Record<DashboardRange, string> = {
  today: "今天",
  week: "近 7 天",
  month: "近 30 天",
  year: "今年",
};

export function DashboardPage({ refreshNonce }: { refreshNonce: number }) {
  const [range, setRange] = useState<DashboardRange>("month");
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [concurrency, setConcurrency] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const credential = getServiceKey().trim();
  const timezone = useMemo(() => Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC", []);

  useEffect(() => {
    if (isAuthContextPending()) {
      return;
    }
    const controller = new AbortController();
    let cancelled = false;
    const loadDashboard = async () => {
      setLoading(true);
      setError("");
      const requestCredential = credential;
      try {
        const payload = await api.dashboard(range, timezone, requestCredential, controller.signal);
        if (cancelled || getServiceKey().trim() !== requestCredential) {
          return;
        }
        setDashboard(payload.dashboard || null);
        setConcurrency(Math.max(0, Number(payload.current_concurrency || 0)));
      } catch (caught) {
        if (cancelled || getServiceKey().trim() !== requestCredential) {
          return;
        }
        setError(errorMessage(caught));
      } finally {
        if (!cancelled && getServiceKey().trim() === requestCredential) {
          setLoading(false);
        }
      }
    };
    void loadDashboard();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [credential, range, timezone, refreshNonce]);

  useEffect(() => {
    if (!credential || isAuthContextPending()) {
      return;
    }
    let cancelled = false;
    let pending = false;
    const refreshConcurrency = async () => {
      if (pending) return;
      pending = true;
      const requestCredential = credential;
      try {
        const payload = await api.myConcurrency(requestCredential);
        if (!cancelled && getServiceKey().trim() === requestCredential) {
          setConcurrency(Math.max(0, Number(payload.current_concurrency || 0)));
        }
      } catch {
        // Keep the last confirmed value; the full dashboard request surfaces errors.
      } finally {
        pending = false;
      }
    };
    const timer = window.setInterval(() => void refreshConcurrency(), 5_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [credential]);

  const selectedRangeLoaded = dashboard?.range === range;
  const selectedPeriod = dashboard?.periods?.[range] || {};
  const generatedAt = dashboard?.generated_at;

  return (
    <div className="grid gap-4">
      <Card className="overflow-hidden">
        <div className="absolute inset-x-0 top-0 h-24 bg-gradient-to-r from-primary/12 via-primary/5 to-transparent" />
        <CardHeader className="relative gap-2 sm:grid-cols-[1fr_auto]">
          <div className="grid gap-1.5">
            <CardTitle className="flex items-center gap-2">
              <LayoutDashboardIcon className="size-5" />
              使用仪表盘
            </CardTitle>
            <CardDescription>只统计当前登录账号发起的请求；金额根据请求用量与模型费率估算。</CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2 sm:justify-end">
            <Badge variant="success">
              <span className="size-1.5 rounded-full bg-current" />
              并发每 5 秒更新
            </Badge>
            <Badge variant="outline">{timezone}</Badge>
          </div>
        </CardHeader>
        <CardPanel className="relative grid gap-4">
          {error ? <ErrorAlert title="仪表盘载入失败" message={error} /> : null}
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-[1.15fr_repeat(4,minmax(0,1fr))]">
            <div className="relative overflow-hidden rounded-xl border bg-primary p-5 text-primary-foreground shadow-sm sm:col-span-2 xl:col-span-1">
              <div className="absolute -right-7 -top-7 size-28 rounded-full border border-white/15" />
              <div className="absolute -right-2 top-8 size-20 rounded-full border border-white/10" />
              <div className="relative flex items-center justify-between gap-3 text-sm text-primary-foreground/80">
                <span>当前并发</span>
                <ActivityIcon className="size-4" />
              </div>
              <div className="relative mt-5 oaix-tabular font-semibold text-4xl">{formatNumber(concurrency)}</div>
              <div className="relative mt-2 text-xs text-primary-foreground/70">正在处理的本账号 API 请求</div>
            </div>
            {RANGE_OPTIONS.map((option) => (
              <PeriodCard key={option.value} label={option.label} period={dashboard?.periods?.[option.value]} />
            ))}
          </div>
        </CardPanel>
      </Card>

      <Card>
        <CardHeader className="gap-3 sm:grid-cols-[1fr_auto]">
          <div className="grid gap-1.5">
            <CardTitle className="flex items-center gap-2">
              <GaugeIcon className="size-5" />
              用量趋势
            </CardTitle>
            <CardDescription>
              {PERIOD_LABELS[range]}缓存率曲线 · {formatNumber(selectedPeriod.request_count || 0)} 次请求 · {formatCompactNumber(selectedPeriod.total_tokens || 0)} Tokens · {formatCurrency(selectedPeriod.estimated_cost_usd || 0)}
            </CardDescription>
          </div>
          <div aria-label="仪表盘时间范围" className="flex flex-wrap gap-1 rounded-lg border bg-muted/40 p-1" role="group">
            {RANGE_OPTIONS.map((option) => (
              <Button
                aria-pressed={range === option.value}
                key={option.value}
                onClick={() => setRange(option.value)}
                size="sm"
                variant={range === option.value ? "default" : "ghost"}
              >
                {option.label}
              </Button>
            ))}
          </div>
        </CardHeader>
        <CardPanel>
          {loading && !selectedRangeLoaded ? (
            <LoadingState label="正在聚合用量趋势" />
          ) : (
            <CacheRateChart bucket={dashboard?.bucket || "day"} points={selectedRangeLoaded ? dashboard?.trend || [] : []} />
          )}
        </CardPanel>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ChartColumnBigIcon className="size-5" />
            模型消费
          </CardTitle>
          <CardDescription>{PERIOD_LABELS[range]}不同模型的预估消费金额，按金额从高到低排列。</CardDescription>
        </CardHeader>
        <CardPanel>
          {loading && !selectedRangeLoaded ? (
            <LoadingState label="正在聚合模型消费" />
          ) : (
            <ModelSpendChart models={selectedRangeLoaded ? dashboard?.models || [] : []} />
          )}
        </CardPanel>
      </Card>

      <div className="flex flex-wrap items-center justify-between gap-2 px-1 text-muted-foreground text-xs">
        <span className="flex items-center gap-1.5">
          <TimerResetIcon className="size-3.5" />
          请求用量按小时汇总，通常在请求完成后约 1 分钟内更新。
        </span>
        <span>统计更新时间 {formatDate(generatedAt)}</span>
      </div>
    </div>
  );
}

function PeriodCard({ label, period }: { label: string; period?: DashboardPeriod }) {
  return (
    <div className="rounded-xl border bg-card p-4 shadow-xs">
      <div className="flex items-center justify-between gap-2 text-muted-foreground text-xs">
        <span>{label}</span>
        <GaugeIcon className="size-3.5" />
      </div>
      <div className="mt-3 oaix-tabular font-semibold text-2xl">{formatCompactNumber(period?.request_count || 0)}</div>
      <div className="mt-1 text-muted-foreground text-xs">请求数量</div>
      <div className="mt-4 grid gap-1.5 border-t pt-3 text-xs">
        <div className="flex items-center justify-between gap-2">
          <span className="text-muted-foreground">Tokens</span>
          <span className="oaix-tabular font-medium">{formatCompactNumber(period?.total_tokens || 0)}</span>
        </div>
        <div className="flex items-center justify-between gap-2">
          <span className="text-muted-foreground">消费</span>
          <span className="oaix-tabular font-medium">{formatCompactCurrency(period?.estimated_cost_usd || 0)}</span>
        </div>
      </div>
    </div>
  );
}

function CacheRateChart({ bucket, points }: { bucket: string; points: DashboardTrendPoint[] }) {
  const plotRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(880);
  const observed = points
    .map((point, index) => ({ ...point, index, ratio: finiteNumber(point.cache_hit_ratio) }))
    .filter((point) => point.ratio != null);
  const hasData = observed.length > 0;
  useEffect(() => {
    if (!hasData || !plotRef.current) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(240, entry.contentRect.width)));
    observer.observe(plotRef.current);
    return () => observer.disconnect();
  }, [hasData]);
  if (!observed.length) {
    return <EmptyState compact title="暂无缓存率数据" description="请求返回 Token 用量后，这里会显示缓存命中率曲线。" />;
  }

  const height = 260;
  const padding = { top: 18, right: 18, bottom: 36, left: 48 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const denominator = Math.max(points.length - 1, 1);
  const coordinates = observed.map((point) => ({
    ...point,
    x: padding.left + (point.index / denominator) * plotWidth,
    y: padding.top + (1 - clamp(point.ratio || 0, 0, 1)) * plotHeight,
  }));
  const linePath = coordinates.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const areaPath = coordinates.length > 1
    ? `${linePath} L ${coordinates.at(-1)?.x.toFixed(2)} ${(padding.top + plotHeight).toFixed(2)} L ${coordinates[0].x.toFixed(2)} ${(padding.top + plotHeight).toFixed(2)} Z`
    : "";
  const labelIndexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])].filter((index) => index >= 0);

  return (
    <div className="grid gap-3">
      <div ref={plotRef} className="min-w-0 rounded-xl border bg-muted/20 p-3">
        <svg aria-label="缓存命中率曲线" className="h-[260px] w-full text-primary" role="img" viewBox={`0 0 ${width} ${height}`}>
          <defs>
            <linearGradient id="oaix-cache-rate-area" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="currentColor" stopOpacity="0.24" />
              <stop offset="100%" stopColor="currentColor" stopOpacity="0.02" />
            </linearGradient>
          </defs>
          {[0, 0.25, 0.5, 0.75, 1].map((ratio) => {
            const y = padding.top + (1 - ratio) * plotHeight;
            return (
              <g key={ratio}>
                <line className="text-border" stroke="currentColor" strokeDasharray="4 5" x1={padding.left} x2={width - padding.right} y1={y} y2={y} />
                <text className="fill-muted-foreground text-[11px]" textAnchor="end" x={padding.left - 9} y={y + 4}>{Math.round(ratio * 100)}%</text>
              </g>
            );
          })}
          {areaPath ? <path d={areaPath} fill="url(#oaix-cache-rate-area)" /> : null}
          <path d={linePath} fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" />
          {coordinates.map((point) => (
            <circle className="fill-background" cx={point.x} cy={point.y} key={point.bucket_start} r="4" stroke="currentColor" strokeWidth="2">
              <title>{formatTrendLabel(point.bucket_start, bucket)} · {formatPercent(point.ratio || 0)} · {formatNumber(point.request_count || 0)} 次请求</title>
            </circle>
          ))}
          {labelIndexes.map((index) => {
            const point = points[index];
            const x = padding.left + (index / denominator) * plotWidth;
            return <text className="fill-muted-foreground text-[11px]" key={index} textAnchor={index === 0 ? "start" : index === points.length - 1 ? "end" : "middle"} x={x} y={height - 10}>{formatTrendLabel(point?.bucket_start, bucket)}</text>;
          })}
        </svg>
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 text-muted-foreground text-xs">
        <span>缓存率 = 缓存命中输入 Tokens ÷ 输入 Tokens</span>
        <span>当前区间平均 {formatPercent(weightedCacheRate(points))}</span>
      </div>
    </div>
  );
}

function ModelSpendChart({ models }: { models: DashboardModel[] }) {
  if (!models.length) {
    return <EmptyState compact title="暂无模型消费" description="产生包含 Token 用量的请求后，这里会按模型展示消费金额。" />;
  }
  const maxCost = Math.max(...models.map((item) => finiteNumber(item.estimated_cost_usd) || 0), 0);
  return (
    <div className="grid gap-3 rounded-xl border bg-muted/20 p-4">
      {models.slice(0, 12).map((item, index) => {
        const cost = finiteNumber(item.estimated_cost_usd) || 0;
        const width = maxCost > 0 ? (cost / maxCost) * 100 : 0;
        return (
          <div className="grid gap-1.5" key={item.model_name || index}>
            <div className="flex min-w-0 items-center justify-between gap-4 text-sm">
              <div className="flex min-w-0 items-center gap-2">
                <span className="flex size-5 shrink-0 items-center justify-center rounded bg-primary/10 oaix-tabular text-primary text-[10px]">{index + 1}</span>
                <span className="truncate font-medium" title={item.model_name}>{item.model_name || "unknown"}</span>
              </div>
              <div className="flex shrink-0 items-center gap-3 oaix-tabular text-xs">
                <span className="hidden text-muted-foreground sm:inline">{formatCompactNumber(item.total_tokens || 0)} Tokens</span>
                <span className="font-semibold">{formatCurrency(cost)}</span>
              </div>
            </div>
            <div className="h-2 overflow-hidden rounded-full bg-muted">
              <div className="h-full rounded-full bg-gradient-to-r from-primary/65 to-primary transition-[width]" style={{ width: `${width}%` }} />
            </div>
          </div>
        );
      })}
      <div className="flex flex-wrap items-center gap-4 border-t pt-3 text-muted-foreground text-xs">
        <span className="flex items-center gap-1.5"><CoinsIcon className="size-3.5" />合计 {formatCurrency(models.reduce((sum, item) => sum + (finiteNumber(item.estimated_cost_usd) || 0), 0))}</span>
        <span>{formatNumber(models.reduce((sum, item) => sum + Number(item.request_count || 0), 0))} 次请求</span>
      </div>
    </div>
  );
}

function finiteNumber(value: unknown): number | null {
  if (value == null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function weightedCacheRate(points: DashboardTrendPoint[]): number {
  const input = points.reduce((sum, point) => sum + Number(point.input_tokens || 0), 0);
  const cached = points.reduce((sum, point) => sum + Number(point.cached_input_tokens || 0), 0);
  return input > 0 ? cached / input : 0;
}

function formatPercent(value: number): string {
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1, style: "percent" }).format(clamp(value, 0, 1));
}

function formatCompactNumber(value: unknown): string {
  const number = finiteNumber(value) || 0;
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1, notation: "compact" }).format(number);
}

function formatCompactCurrency(value: unknown): string {
  const number = finiteNumber(value) || 0;
  if (Math.abs(number) < 1000) {
    return formatCurrency(number);
  }
  return `$${new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1, notation: "compact" }).format(number)}`;
}

function formatTrendLabel(value: string | undefined, bucket: string): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  if (bucket === "hour") {
    return date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" });
  }
  if (bucket === "month") {
    return date.toLocaleDateString("zh-CN", { month: "short" });
  }
  return date.toLocaleDateString("zh-CN", { day: "2-digit", month: "2-digit" });
}
