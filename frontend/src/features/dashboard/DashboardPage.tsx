import {
  ActivityIcon,
  ChartColumnBigIcon,
  CoinsIcon,
  HashIcon,
  GaugeIcon,
  LayoutDashboardIcon,
  LayersIcon,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { Input } from "@/registry/default/ui/input";
import {
  api,
  getServiceKey,
  isAuthContextPending,
  type DashboardData,
  type DashboardModel,
  type DashboardRange,
  type DashboardTrendPoint,
} from "@/lib/api";
import { clamp, formatCurrency, formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";

const RANGE_OPTIONS: Array<{ label: string; value: DashboardRange }> = [
  { label: "今天", value: "today" },
  { label: "本周", value: "week" },
  { label: "本月", value: "month" },
  { label: "今年", value: "year" },
  { label: "自定义", value: "custom" },
];

const PERIOD_LABELS: Record<DashboardRange, string> = {
  today: "今天",
  week: "本周",
  month: "本月",
  year: "今年",
  custom: "自定义",
};

export function DashboardPage({ refreshNonce }: { refreshNonce: number }) {
  const [range, setRange] = useState<DashboardRange>("today");
  const today = localDate(new Date());
  const [customFrom, setCustomFrom] = useState(today);
  const [customTo, setCustomTo] = useState(today);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [loadedSelection, setLoadedSelection] = useState("");
  const [concurrency, setConcurrency] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const credential = getServiceKey().trim();
  const timezone = useMemo(() => Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC", []);
  const dateError = range !== "custom" ? "" : !customFrom || !customTo ? "请选择起止日期" :
    customFrom > customTo ? "开始日期不能晚于结束日期" :
    customFrom < "1970-01-01" || customTo > today ? "日期需在 1970-01-01 至今天之间" : "";
  const selection = range === "custom" ? `${range}:${customFrom}:${customTo}` : range;

  useEffect(() => {
    if (isAuthContextPending() || dateError) {
      return;
    }
    const controller = new AbortController();
    let cancelled = false;
    const loadDashboard = async () => {
      setLoading(true);
      setError("");
      const requestCredential = credential;
      try {
        const payload = await api.dashboard(range, timezone, requestCredential, controller.signal, { from: customFrom, to: customTo });
        if (cancelled || getServiceKey().trim() !== requestCredential) {
          return;
        }
        setDashboard(payload.dashboard || null);
        setLoadedSelection(selection);
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
  }, [credential, range, timezone, refreshNonce, customFrom, customTo, dateError, selection]);

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
        if (!cancelled) setConcurrency(null);
      } finally {
        pending = false;
      }
    };
    void refreshConcurrency();
    const timer = window.setInterval(() => void refreshConcurrency(), 5_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [credential]);

  const selectedRangeLoaded = !dateError && loadedSelection === selection && dashboard?.range === range;
  const selectedPeriod = selectedRangeLoaded ? dashboard?.periods?.[range] : undefined;
  const generatedAt = dashboard?.generated_at;

  const rangeLabel = range === "custom" ? `${customFrom} ~ ${customTo}` : PERIOD_LABELS[range];
  const pendingRange = !selectedRangeLoaded && !error && !dateError;
  const cacheRate = selectedPeriod && Number(selectedPeriod.input_tokens) > 0
    ? formatPercent(Number(selectedPeriod.cache_hit_ratio || 0)) : "--";

  return (
    <div className="grid min-w-0 gap-8 p-1 sm:p-3">
      <section aria-labelledby="dashboard-title" className="min-w-0">
        <header className="flex flex-wrap items-center justify-between gap-3 pb-5">
          <h2 id="dashboard-title" className="flex items-center gap-2 font-semibold text-lg">
            <LayoutDashboardIcon className="size-5 text-muted-foreground" />
            使用仪表盘
          </h2>
          <span className="text-muted-foreground text-xs">{timezone}</span>
        </header>
        <div className="flex flex-wrap items-end justify-between gap-4 border-b pb-5">
          <fieldset aria-label="仪表盘时间范围" className="grid w-full grid-cols-5 gap-1 rounded-lg bg-muted p-1 sm:w-auto">
            {RANGE_OPTIONS.map((option) => (
              <label key={option.value} className="relative min-w-0 cursor-pointer">
                <input
                  className="peer sr-only"
                  type="radio"
                  name="dashboard-range"
                  value={option.value}
                  checked={range === option.value}
                  onChange={() => { setError(""); setRange(option.value); }}
                />
                <span className="flex h-9 items-center justify-center whitespace-nowrap rounded-md px-2 text-sm text-muted-foreground transition-colors hover:text-foreground peer-checked:bg-background peer-checked:font-medium peer-checked:text-foreground peer-checked:shadow-xs peer-focus-visible:ring-2 peer-focus-visible:ring-ring sm:px-4">
                  {option.label}
                </span>
              </label>
            ))}
          </fieldset>
          {range === "custom" ? (
            <div className="grid w-full grid-cols-2 gap-3 sm:w-auto sm:grid-cols-[160px_160px]">
              <label className="grid min-w-0 gap-1.5 text-muted-foreground text-xs">
                开始日期
                <Input nativeInput type="date" min="1970-01-01" max={customTo || today} value={customFrom}
                  aria-invalid={Boolean(dateError)} onChange={(event) => { setError(""); setCustomFrom(event.target.value); }} />
              </label>
              <label className="grid min-w-0 gap-1.5 text-muted-foreground text-xs">
                结束日期
                <Input nativeInput type="date" min={customFrom || "1970-01-01"} max={today} value={customTo}
                  aria-invalid={Boolean(dateError)} onChange={(event) => { setError(""); setCustomTo(event.target.value); }} />
              </label>
            </div>
          ) : <span className="pb-2 text-muted-foreground text-xs">{PERIOD_LABELS[range]}</span>}
        </div>
        {dateError ? <p role="alert" className="mt-4 text-destructive text-sm">{dateError}</p> : null}
        {error ? <div className="mt-4"><ErrorAlert title="仪表盘载入失败" message={error} /></div> : null}
        <div data-dashboard-metrics className="grid grid-cols-2 gap-3 pt-6 xl:grid-cols-4">
          <MetricCard label="当前并发" value={concurrency == null ? "--" : formatNumber(concurrency)}
            icon={<ActivityIcon className="size-4" />} detail="实时" tone="emerald" />
          <MetricCard label="消耗金额" value={selectedPeriod ? formatCompactCurrency(selectedPeriod.estimated_cost_usd) : "--"}
            icon={<CoinsIcon className="size-4" />} detail={PERIOD_LABELS[range]} tone="amber"
            title={selectedPeriod ? formatCurrency(selectedPeriod.estimated_cost_usd || 0) : undefined} />
          <MetricCard label="Token 用量" value={selectedPeriod ? formatCompactNumber(selectedPeriod.total_tokens) : "--"}
            icon={<LayersIcon className="size-4" />} detail={PERIOD_LABELS[range]} tone="blue"
            title={selectedPeriod ? formatNumber(selectedPeriod.total_tokens || 0) : undefined} />
          <MetricCard label="请求数量" value={selectedPeriod ? formatCompactNumber(selectedPeriod.request_count) : "--"}
            icon={<HashIcon className="size-4" />} detail={PERIOD_LABELS[range]} tone="neutral"
            title={selectedPeriod ? formatNumber(selectedPeriod.request_count || 0) : undefined} />
        </div>
      </section>

      <section aria-labelledby="dashboard-trend-title" className="grid min-w-0 gap-5 border-t pt-6">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div className="grid gap-2">
            <h2 id="dashboard-trend-title" className="flex items-center gap-2 font-semibold text-base">
              <GaugeIcon className="size-4 text-blue-600 dark:text-blue-400" />缓存命中率
            </h2>
            <span className="text-muted-foreground text-xs">{rangeLabel}</span>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-muted-foreground text-xs">平均缓存率</span>
            <span className="oaix-tabular font-semibold text-xl">{cacheRate}</span>
          </div>
        </header>
        <div className="min-h-[284px]" aria-busy={pendingRange}>
          {pendingRange ? <div className="grid h-[284px] place-items-center"><LoadingState label="正在载入" /></div> :
            <CacheRateChart bucket={dashboard?.bucket || "day"} points={selectedRangeLoaded ? dashboard?.trend || [] : []} />}
        </div>
      </section>

      <section aria-labelledby="dashboard-models-title" className="grid min-w-0 gap-5 border-t pt-6">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <h2 id="dashboard-models-title" className="flex items-center gap-2 font-semibold text-base">
            <ChartColumnBigIcon className="size-4 text-amber-600 dark:text-amber-400" />模型消费
          </h2>
          <span className="text-muted-foreground text-xs">{rangeLabel}</span>
        </header>
        <div className="min-h-40" aria-busy={pendingRange}>
          {pendingRange ? <div className="grid h-40 place-items-center"><LoadingState label="正在载入" /></div> :
            <ModelSpendChart models={selectedRangeLoaded ? dashboard?.models || [] : []} />}
        </div>
      </section>
      <footer className="flex flex-wrap items-center justify-between gap-2 border-t pt-4 text-muted-foreground text-xs">
        <span>预估消费 · USD</span>
        <span>统计更新时间 {selectedRangeLoaded ? formatDate(generatedAt) : "--"}</span>
      </footer>
    </div>
  );
}

function MetricCard({ label, value, icon, detail, tone, title }: {
  label: string; value: string; icon: ReactNode; detail: string;
  tone: "emerald" | "amber" | "blue" | "neutral"; title?: string;
}) {
  const accent = {
    emerald: "border-emerald-500/25 text-emerald-700 dark:text-emerald-400",
    amber: "border-border text-amber-700 dark:text-amber-400",
    blue: "border-border text-blue-700 dark:text-blue-400",
    neutral: "border-border text-muted-foreground",
  }[tone];
  return (
    <article aria-label={label} className={`grid min-h-[152px] min-w-0 content-between gap-4 rounded-lg border bg-card p-4 sm:p-5 ${accent}`}>
      <div className="flex items-center justify-between gap-2 text-xs sm:text-sm">
        <span>{label}</span><span aria-hidden="true">{icon}</span>
      </div>
      <div data-metric-value className="break-all oaix-tabular font-semibold text-2xl leading-tight text-foreground sm:text-3xl" title={title}>{value}</div>
      <span className="text-muted-foreground text-xs">{detail}</span>
    </article>
  );
}

function localDate(value: Date): string {
  return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
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
    return <div className="grid h-[284px] place-items-center"><EmptyState compact title="暂无缓存率数据" description="所选时间范围内暂无缓存用量。" /></div>;
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
  const linePath = coordinates.map((point, index) => `${index === 0 || point.index !== coordinates[index - 1].index + 1 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const labelIndexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])].filter((index) => index >= 0);

  return (
    <div className="grid gap-3">
      <div ref={plotRef} className="min-w-0 py-3">
        <svg aria-label="缓存命中率曲线" className="h-[260px] w-full text-blue-600 dark:text-blue-400" role="img" viewBox={`0 0 ${width} ${height}`}>
          {[0, 0.25, 0.5, 0.75, 1].map((ratio) => {
            const y = padding.top + (1 - ratio) * plotHeight;
            return (
              <g key={ratio}>
                <line className="text-border" stroke="currentColor" strokeDasharray="4 5" x1={padding.left} x2={width - padding.right} y1={y} y2={y} />
                <text className="fill-muted-foreground text-[11px]" textAnchor="end" x={padding.left - 9} y={y + 4}>{Math.round(ratio * 100)}%</text>
              </g>
            );
          })}
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
    </div>
  );
}

function ModelSpendChart({ models }: { models: DashboardModel[] }) {
  if (!models.length) {
    return <EmptyState compact title="暂无模型消费" description="所选时间范围内暂无模型用量。" />;
  }
  const maxCost = Math.max(...models.map((item) => finiteNumber(item.estimated_cost_usd) || 0), 0);
  return (
    <div className="grid gap-4">
      {models.map((item, index) => {
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
                <span className="font-semibold" title={formatCurrency(cost)}>{formatCompactCurrency(cost)}</span>
              </div>
            </div>
            <div className="h-1.5 overflow-hidden rounded-sm bg-muted">
              <div className="h-full rounded-sm bg-amber-500/80 transition-[width]" style={{ width: `${width}%` }} />
            </div>
          </div>
        );
      })}
      <div className="flex flex-wrap items-center gap-4 border-t pt-3 text-muted-foreground text-xs">
        <span className="flex items-center gap-1.5"><CoinsIcon className="size-3.5" />所列模型合计 {formatCompactCurrency(models.reduce((sum, item) => sum + (finiteNumber(item.estimated_cost_usd) || 0), 0))}</span>
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
