import { ListFilterIcon } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Badge } from "@/registry/default/ui/badge";
import { Card, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { api, type RequestItem, type RequestSummary } from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingRows, MiniMetric } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import { RequestModelCell } from "@/shared/resourceTables";

export function RequestsPage({ refreshNonce }: { refreshNonce: number }) {
  const [requests, setRequests] = useState<RequestItem[]>([]);
  const [summary, setSummary] = useState<RequestSummary>({});
  const [modelStats, setModelStats] = useState<Array<Record<string, unknown>>>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const loadRequests = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [requestPayload, analyticsPayload] = await Promise.all([api.requests(120), api.analytics(24)]);
      setRequests(requestPayload.items || []);
      setSummary(requestPayload.summary || {});
      setModelStats(Array.isArray((analyticsPayload as any).models) ? (analyticsPayload as any).models : []);
    } catch (caught) {
      setError(errorMessage(caught));
      setRequests([]);
      setModelStats([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRequests();
  }, [loadRequests, refreshNonce]);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ListFilterIcon className="size-5" />
          请求观察
        </CardTitle>
        <CardDescription>最近请求、24 小时聚合和模型分布。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <div className="grid gap-3 sm:grid-cols-4">
          <MiniMetric label="总请求" value={summary.total} />
          <MiniMetric label="成功" value={summary.success} />
          <MiniMetric label="失败" value={summary.failure} />
          <MiniMetric label="平均首字" value={`${Math.round(summary.average_ttft_ms || 0)} ms`} />
        </div>
        {modelStats.length > 0 && (
          <div className="rounded-lg border bg-muted/40 p-4">
            <div className="mb-3 font-medium text-sm">模型分布</div>
            <div className="grid gap-2">
              {modelStats.slice(0, 12).map((item) => {
                const requestsCount = Number(item.requests || 0);
                const max = Math.max(...modelStats.map((row) => Number(row.requests || 0)), 1);
                return (
                  <div className="grid gap-1" key={String(item.model_name || "unknown")}>
                    <div className="flex justify-between gap-3 text-xs">
                      <span className="truncate">{String(item.model_name || "unknown")}</span>
                      <span className="oaix-tabular">{formatNumber(requestsCount)}</span>
                    </div>
                    <div className="h-2 overflow-hidden rounded-full bg-muted">
                      <span className="block h-full bg-primary" style={{ width: `${Math.max(4, (requestsCount / max) * 100)}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
        {error ? (
          <ErrorAlert title="请求日志载入失败" message={error} />
        ) : loading && !requests.length ? (
          <LoadingRows />
        ) : requests.length ? (
          <div className="min-w-0 overflow-x-auto rounded-lg border oaix-scrollbar">
            <Table style={{ width: "max(100%, 64rem)" }}>
              <TableHeader>
                <TableRow>
                  <TableHead>时间</TableHead>
                  <TableHead>模型</TableHead>
                  <TableHead>端点</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>TTFT</TableHead>
                  <TableHead>错误</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {requests.map((item, index) => (
                  <TableRow key={item.id || item.started_at || index}>
                    <TableCell>{formatDate(item.started_at)}</TableCell>
                    <TableCell>
                      <RequestModelCell item={item} />
                    </TableCell>
                    <TableCell>{item.endpoint || "-"}</TableCell>
                    <TableCell>
                      <Badge variant={item.success === false ? "error" : "success"}>{item.status_code || "-"}</Badge>
                    </TableCell>
                    <TableCell>{item.ttft_ms == null ? "-" : `${item.ttft_ms} ms`}</TableCell>
                    <TableCell className="max-w-96 truncate" title={item.error_message || ""}>
                      {item.error_message || "-"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyState title="暂无请求日志" description="有新的代理请求后会在这里出现。" />
        )}
      </CardPanel>
    </Card>
  );
}
