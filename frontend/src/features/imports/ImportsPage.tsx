import {
  ChevronDownIcon,
  ChevronRightIcon,
  SaveIcon,
  Trash2Icon,
  UploadIcon,
} from "lucide-react";
import type * as React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Separator } from "@/registry/default/ui/separator";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { Textarea } from "@/registry/default/ui/textarea";
import {
  api,
  getServiceKey,
  setServiceKey,
  type ImportBatch,
  type ImportBatchDetail,
  type TokenItem,
} from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";
import {
  collectImportEntries,
  errorMessage,
  formatUSDOptional,
  importBatchCancelable,
  importBatchStatusLabel,
  importBatchTotal,
  tokenPlanType,
  tokenStatusLabel,
  tokenStatusOf,
  tokenTitle,
} from "@/shared/domain";
import {
  EmptyState,
  ErrorAlert,
  LoadingRows,
  MiniMetric,
  TokenConcurrency,
  TokenObservedCost,
  TokenQuotaStrip,
  importBatchBadge,
  statusBadge,
} from "@/shared/components";
import type { RouteState } from "@/app/router";
import { navigateTo as go } from "@/app/router";
import type { ToastMessage } from "@/shared/types";

export function ImportsPage({
  pushToast,
  refreshNonce,
  route,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  route: RouteState;
}) {
  if (route.key === "import_new") {
    return <ImportNewPage pushToast={pushToast} />;
  }
  return <ImportBatchesPage pushToast={pushToast} refreshNonce={refreshNonce} />;
}

function ImportNewPage({ pushToast }: { pushToast: (title: string, variant?: ToastMessage["variant"]) => void }) {
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [tokenInput, setTokenInput] = useState("");
  const [queuePosition, setQueuePosition] = useState<"front" | "back">("front");
  const [importFeedback, setImportFeedback] = useState("等待导入。");
  const [importBusy, setImportBusy] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  async function saveServiceKey() {
    setServiceKey(serviceKeyDraft);
    pushToast("凭证已保存");
  }

  function clearServiceKey() {
    setServiceKey("");
    setServiceKeyDraft("");
    pushToast("凭证已清空", "info");
  }

  async function importTokens() {
    setImportBusy(true);
    try {
      setImportFeedback("正在解析导入内容...");
      const entries = await collectImportEntries(tokenInput, fileInputRef.current?.files);
      if (!entries.length) {
        setImportFeedback("没有可导入的 token。");
        return;
      }
      setImportFeedback(`正在提交 ${formatNumber(entries.length)} 条 token...`);
      const result = await api.importTokens({
        import_queue_position: queuePosition,
        tokens: entries,
      });
      const job = (result as any).job || {};
      const summary = (result as any).result || {};
      setImportFeedback(
        job.id
          ? `已提交批次 #${job.id}：共 ${formatNumber(job.total_count || entries.length)} 条。`
          : `导入完成：新建 ${formatNumber(summary.created)}，更新 ${formatNumber(summary.updated)}，跳过 ${formatNumber(summary.skipped)}，失败 ${formatNumber(summary.failed)}。`,
      );
      setTokenInput("");
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      pushToast("导入任务已提交");
    } catch (caught) {
      setImportFeedback(errorMessage(caught));
      pushToast(errorMessage(caught), "error");
    } finally {
      setImportBusy(false);
    }
  }

  return (
    <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(280px,.6fr)]">
      <Card className="min-w-0 overflow-hidden">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <UploadIcon className="size-5" />
            导入 Key
          </CardTitle>
          <CardDescription>保存凭证并导入 access token / refresh token 数据。</CardDescription>
          <CardAction>
            <Button onClick={() => go("/imports")} size="sm" variant="outline">
              查看批次
            </Button>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid min-w-0 gap-4">
          <div className="grid min-w-0 gap-2">
            <Label htmlFor="service-key">Service API Key</Label>
            <Input
              id="service-key"
              nativeInput
              onChange={(event) => setServiceKeyDraft(event.currentTarget.value)}
              placeholder="如果启用了 SERVICE_API_KEYS，请填在这里"
              type="password"
              value={serviceKeyDraft}
            />
          </div>
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void saveServiceKey()}>
              <SaveIcon />
              保存凭证
            </Button>
            <Button onClick={clearServiceKey} variant="outline">
              清空
            </Button>
          </div>
          <Separator />
          <div className="grid min-w-0 gap-2">
            <Label htmlFor="token-json">粘贴 Key 数据</Label>
            <Textarea
              className="block w-full min-w-0 max-w-full overflow-hidden"
              id="token-json"
              onChange={(event) => setTokenInput(event.currentTarget.value)}
              placeholder="支持普通 JSON / sub2api 导出 JSON；也支持每行 access_token / refresh_token，或 account_id,refresh_token。"
              rows={12}
              spellCheck={false}
              value={tokenInput}
              wrap="soft"
            />
          </div>
          <div className="grid min-w-0 gap-2">
            <Label htmlFor="token-files">选择文件</Label>
            <Input
              accept=".json,.txt,.csv,application/json,text/plain"
              id="token-files"
              multiple
              nativeInput
              ref={fileInputRef}
              type="file"
            />
          </div>
          <div className="grid min-w-0 gap-2">
            <Label>导入位置</Label>
            <div className="flex rounded-lg bg-muted p-1">
              <Button
                className="flex-1"
                onClick={() => setQueuePosition("front")}
                variant={queuePosition === "front" ? "secondary" : "ghost"}
              >
                开头
              </Button>
              <Button
                className="flex-1"
                onClick={() => setQueuePosition("back")}
                variant={queuePosition === "back" ? "secondary" : "ghost"}
              >
                最后
              </Button>
            </div>
          </div>
          <Button disabled={importBusy} loading={importBusy} onClick={() => void importTokens()}>
            开始导入
          </Button>
          <Alert variant={importFeedback.includes("失败") || importFeedback.includes("错误") ? "error" : "info"}>
            <UploadIcon />
            <AlertTitle>导入状态</AlertTitle>
            <AlertDescription>{importFeedback}</AlertDescription>
          </Alert>
        </CardPanel>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>导入说明</CardTitle>
          <CardDescription>导入任务会进入后台队列，批次页可查看每个 key 的校验和发布结果。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-3">
          <MiniMetric label="支持格式" value="access_token / refresh_token / account_id,refresh_token / JSON" />
          <MiniMetric label="导入位置" value={queuePosition === "front" ? "队列开头" : "队列最后"} />
          <MiniMetric label="凭证状态" value={serviceKeyDraft.trim() ? "已填写" : "未填写"} />
        </CardPanel>
      </Card>
    </div>
  );
}

function ImportBatchesPage({
  pushToast,
  refreshNonce,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [batches, setBatches] = useState<ImportBatch[]>([]);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [details, setDetails] = useState<Record<number, ImportBatchDetail>>({});
  const [detailErrors, setDetailErrors] = useState<Record<number, string>>({});
  const [detailLoadingId, setDetailLoadingId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const loadBatches = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await api.importJobs(80);
      setBatches(payload.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  const loadDetail = useCallback(async (id: number) => {
    setDetailLoadingId(id);
    setDetailErrors((items) => {
      const next = { ...items };
      delete next[id];
      return next;
    });
    try {
      const payload = await api.importJob(id);
      setDetails((items) => ({ ...items, [id]: payload }));
      if (payload.job) {
        setBatches((items) => items.map((job) => (job.id === id ? { ...job, ...payload.job } : job)));
      }
    } catch (caught) {
      setDetailErrors((items) => ({ ...items, [id]: errorMessage(caught) }));
    } finally {
      setDetailLoadingId(null);
    }
  }, []);

  useEffect(() => {
    void loadBatches();
  }, [loadBatches, refreshNonce]);

  async function cancelJob(id: number) {
    await api.cancelImportJob(id);
    pushToast("已发送取消导入请求");
    await loadBatches();
    await loadDetail(id);
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <UploadIcon className="size-5" />
          导入批次
        </CardTitle>
        <CardDescription>查看每次导入的处理结果、平均余额和关联 key。</CardDescription>
        <CardAction>
          <Button onClick={() => go("/imports/new")} size="sm">
            新导入
          </Button>
        </CardAction>
      </CardHeader>
      <CardPanel>
        {error ? (
          <ErrorAlert title="导入批次载入失败" message={error} />
        ) : (
          <ImportBatchList
            batches={batches}
            detailErrors={detailErrors}
            detailLoadingId={detailLoadingId}
            details={details}
            expandedId={expandedId}
            loading={loading && !batches.length}
            onCancel={(id) => void cancelJob(id)}
            onToggle={(id) => {
              if (expandedId === id) {
                setExpandedId(null);
                return;
              }
              setExpandedId(id);
              if (!details[id]) {
                void loadDetail(id);
              }
            }}
          />
        )}
      </CardPanel>
    </Card>
  );
}

function ImportBatchList({
  batches,
  detailErrors,
  detailLoadingId,
  details,
  expandedId,
  loading,
  onCancel,
  onToggle,
}: {
  batches: ImportBatch[];
  detailErrors: Record<number, string>;
  detailLoadingId: number | null;
  details: Record<number, ImportBatchDetail>;
  expandedId: number | null;
  loading: boolean;
  onCancel: (id: number) => void;
  onToggle: (id: number) => void;
}) {
  if (loading) {
    return <LoadingRows />;
  }
  if (!batches.length) {
    return <EmptyState title="暂无导入批次" description="导入任务提交后会在这里出现。" />;
  }
  return (
    <div className="grid gap-3">
      {batches.map((job) => {
        const expanded = expandedId === job.id;
        const detail = details[job.id];
        const cancelable = importBatchCancelable(job.status);
        return (
          <Card key={job.id} className="block overflow-hidden">
            <CardPanel className="grid flex-none gap-3 p-3">
              <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <strong className="text-sm">导入批次 #{job.id}</strong>
                    <Badge size="sm" variant={importBatchBadge(job.status)}>
                      {importBatchStatusLabel(job.status)}
                    </Badge>
                    <Badge size="sm" variant="outline">
                      {formatNumber(importBatchTotal(job))} key
                    </Badge>
                    <span className="oaix-tabular text-muted-foreground text-xs">提交 {formatDate(job.submitted_at)}</span>
                    {job.finished_at && <span className="oaix-tabular text-muted-foreground text-xs">完成 {formatDate(job.finished_at)}</span>}
                  </div>
                  <div className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                    <ImportBatchMetric label="进度" value={`${formatNumber(job.processed_count)} / ${formatNumber(job.total_count)}`} />
                    <ImportBatchMetric label="变更" value={`新 ${formatNumber(job.created_count)} · 更 ${formatNumber(job.updated_count)} · 跳 ${formatNumber(job.skipped_count)}`} />
                    <ImportBatchMetric label="状态" value={`有效 ${formatNumber(job.available)} · 冷却 ${formatNumber(job.cooling)} · 禁用 ${formatNumber(job.disabled)}`} />
                    <ImportBatchMetric label="余额" value={`${formatUSDOptional(job.observed_cost_usd)} · 平均 ${formatUSDOptional(job.average_observed_cost_usd)}`} />
                  </div>
                  {(job.failed_count || job.missing || job.last_error || job.error_message) ? (
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                      {Boolean(job.failed_count) && (
                        <Badge size="sm" variant="error">
                          失败 {formatNumber(job.failed_count)}
                        </Badge>
                      )}
                      {Boolean(job.missing) && (
                        <Badge size="sm" variant="warning">
                          缺失 {formatNumber(job.missing)}
                        </Badge>
                      )}
                      {(job.last_error || job.error_message) && (
                        <span className="min-w-0 truncate text-destructive-foreground" title={job.last_error || job.error_message || ""}>
                          {job.last_error || job.error_message}
                        </span>
                      )}
                    </div>
                  ) : null}
                </div>
                <div className="flex shrink-0 flex-wrap items-center gap-2 lg:justify-end">
                  <Button onClick={() => onToggle(job.id)} size="sm" variant={expanded ? "secondary" : "outline"}>
                    {expanded ? <ChevronDownIcon /> : <ChevronRightIcon />}
                    {expanded ? "收起 Key" : "查看 Key"}
                  </Button>
                  {cancelable && (
                    <Button onClick={() => onCancel(job.id)} size="sm" variant="destructive-outline">
                      <Trash2Icon />
                      取消
                    </Button>
                  )}
                </div>
              </div>
              {expanded && (
                <ImportBatchDetailPanel
                  detail={detail}
                  error={detailErrors[job.id]}
                  fallbackJob={job}
                  loading={detailLoadingId === job.id}
                />
              )}
            </CardPanel>
          </Card>
        );
      })}
    </div>
  );
}

function ImportBatchMetric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0 rounded-lg border bg-muted/32 px-2.5 py-2">
      <div className="text-muted-foreground text-[11px]">{label}</div>
      <div className="mt-0.5 min-w-0 truncate oaix-tabular text-xs font-medium" title={typeof value === "string" ? value : undefined}>
        {value || "-"}
      </div>
    </div>
  );
}

function ImportBatchDetailPanel({
  detail,
  error,
  fallbackJob,
  loading,
}: {
  detail?: ImportBatchDetail;
  error?: string;
  fallbackJob: ImportBatch;
  loading: boolean;
}) {
  if (error) {
    return <ErrorAlert title="批次详情载入失败" message={error} />;
  }
  if (loading && !detail) {
    return (
      <div className="grid gap-2 border-t pt-3">
        <LoadingRows rows={2} />
      </div>
    );
  }
  const tokens = detail?.tokens || [];
  const items = detail?.items || [];
  const failedItems = items.filter((item) => item.error_message);
  if (!tokens.length && !failedItems.length) {
    return <div className="rounded-lg border border-dashed bg-muted/24 p-4 text-muted-foreground text-sm">这个批次暂时没有可关联的 key 明细。</div>;
  }
  return (
    <div className="grid gap-3 border-t pt-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge size="sm" variant="outline">
          明细 {formatNumber(tokens.length)} key
        </Badge>
        <Badge size="sm" variant="secondary">
          批次 #{fallbackJob.id}
        </Badge>
        {failedItems.length > 0 && (
          <Badge size="sm" variant="error">
            失败项 {formatNumber(failedItems.length)}
          </Badge>
        )}
      </div>
      {tokens.length > 0 && (
        <div className="max-h-[460px] min-w-0 overflow-auto rounded-lg border oaix-scrollbar">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Key</TableHead>
                <TableHead>状态</TableHead>
                <TableHead>额度</TableHead>
                <TableHead>并发</TableHead>
                <TableHead>余额</TableHead>
                <TableHead>最近</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tokens.map((token) => (
                <ImportBatchTokenRow key={token.id} token={token} />
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      {failedItems.length > 0 && (
        <div className="grid gap-1.5">
          {failedItems.slice(0, 10).map((item) => (
            <div className="rounded-lg border bg-muted/32 px-2.5 py-2 text-xs" key={item.id}>
              <div className="flex flex-wrap items-center gap-2">
                <Badge size="sm" variant="error">
                  #{item.item_index + 1}
                </Badge>
                <span className="min-w-0 truncate text-muted-foreground" title={item.error_message || ""}>
                  {item.error_message || "-"}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ImportBatchTokenRow({ token }: { token: TokenItem }) {
  const status = tokenStatusOf(token);
  const title = tokenTitle(token);
  const planType = tokenPlanType(token);
  return (
    <TableRow>
      <TableCell className="min-w-[16rem]">
        <div className="flex min-w-0 flex-col gap-1">
          <div className="flex min-w-0 items-center gap-1.5">
            <span className="min-w-0 truncate font-medium" title={title}>
              {title}
            </span>
            <Badge size="sm" variant="outline">
              ID {token.id}
            </Badge>
          </div>
          <span className="min-w-0 truncate text-muted-foreground text-xs" title={token.remark || token.source_file || ""}>
            {token.remark || token.source_file || "-"}
          </span>
        </div>
      </TableCell>
      <TableCell>
        <div className="flex flex-wrap items-center gap-1">
          <Badge size="sm" variant={statusBadge(status)}>
            {tokenStatusLabel(status)}
          </Badge>
          <Badge className="max-w-24 truncate" size="sm" title={planType} variant="secondary">
            {planType}
          </Badge>
        </div>
      </TableCell>
      <TableCell>
        <div className="flex flex-wrap items-center gap-1 text-[11px]">
          <TokenQuotaStrip quota={token.quota} />
        </div>
      </TableCell>
      <TableCell>
        <TokenConcurrency fallbackCap={Number(token.active_stream_cap || 0)} item={token} />
      </TableCell>
      <TableCell>
        <TokenObservedCost value={token.observed_cost_usd} />
      </TableCell>
      <TableCell>
        <div className="grid gap-1 text-muted-foreground text-xs">
          <span className="oaix-tabular">使用 {formatDate(token.last_used_at)}</span>
          <span className="oaix-tabular">冷却 {formatDate(token.cooldown_until)}</span>
        </div>
      </TableCell>
    </TableRow>
  );
}
