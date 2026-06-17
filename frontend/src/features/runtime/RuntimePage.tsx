import { ActivityIcon, GaugeIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "@/registry/default/ui/badge";
import { Card, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Skeleton } from "@/registry/default/ui/skeleton";
import { api, type HealthResponse, type TokenCounts } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import { ErrorAlert, MiniMetric } from "@/shared/components";
import { errorMessage } from "@/shared/domain";

export function RuntimePage({
  counts,
  health,
  refreshNonce,
}: {
  counts: TokenCounts;
  health: HealthResponse | null;
  refreshNonce: number;
}) {
  const [runtime, setRuntime] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const statusCounts = useMemo(() => {
    const total = Math.max(1, counts.total || 0);
    return {
      available: Math.round(((counts.available || 0) / total) * 100),
      cooling: Math.round(((counts.cooling || 0) / total) * 100),
      disabled: Math.round(((counts.disabled || 0) / total) * 100),
    };
  }, [counts]);

  const loadRuntime = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      setRuntime(await api.runtime());
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRuntime();
  }, [loadRuntime, refreshNonce]);

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,.8fr)_minmax(0,1fr)]">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <GaugeIcon className="size-5" />
            池状态剖面
          </CardTitle>
          <CardDescription>Key 总量、可用状态和当前服务健康度。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-3">
          <div className="grid gap-3 sm:grid-cols-3">
            <Metric label="有效 Key" loading={loading && !counts.total} tone="success" value={counts.available} />
            <Metric label="冷却中" loading={loading && !counts.total} tone="warning" value={counts.cooling} />
            <Metric label="已禁用" loading={loading && !counts.total} tone="error" value={counts.disabled} />
          </div>
          <div className="rounded-lg border bg-muted/40 p-4">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div>
                <div className="font-medium text-sm">状态分布</div>
                <div className="text-muted-foreground text-xs">总量 {formatNumber(counts.total)}</div>
              </div>
              <Badge variant={health?.ok === false ? "warning" : "success"}>{health?.ok === false ? "degraded" : "healthy"}</Badge>
            </div>
            <div className="flex h-2 overflow-hidden rounded-full bg-muted">
              <span className="bg-success" style={{ width: `${statusCounts.available}%` }} />
              <span className="bg-warning" style={{ width: `${statusCounts.cooling}%` }} />
              <span className="bg-destructive" style={{ width: `${statusCounts.disabled}%` }} />
            </div>
          </div>
        </CardPanel>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ActivityIcon className="size-5" />
            Runtime
          </CardTitle>
          <CardDescription>Go 网关运行元数据和 token pool 快照。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error ? (
            <ErrorAlert title="Runtime 载入失败" message={error} />
          ) : (
            <>
              <div className="grid gap-3 sm:grid-cols-3">
                <MiniMetric label="health" value={health?.ok === false ? "degraded" : "ok"} />
                <MiniMetric label="service key" value={health?.service_key_protected ? "protected" : "open"} />
                <MiniMetric label="runtime" value={runtime ? "loaded" : loading ? "loading" : "-"} />
              </div>
              <pre className="max-h-[520px] overflow-auto rounded-lg border bg-muted/24 p-3 text-xs oaix-scrollbar">
                {JSON.stringify(runtime || health || {}, null, 2)}
              </pre>
            </>
          )}
        </CardPanel>
      </Card>
    </div>
  );
}

function Metric({
  label,
  loading,
  tone,
  value,
}: {
  label: string;
  loading: boolean;
  tone: "success" | "warning" | "error";
  value?: number;
}) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="text-muted-foreground text-sm">{label}</span>
        <Badge variant={tone}>{tone}</Badge>
      </div>
      {loading ? <Skeleton className="h-9 w-24" /> : <div className="oaix-tabular font-heading text-3xl font-semibold">{formatNumber(value)}</div>}
    </div>
  );
}
