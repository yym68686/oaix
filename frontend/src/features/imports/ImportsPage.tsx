import {
  CheckIcon,
  CopyIcon,
  ExternalLinkIcon,
  EyeIcon,
  SaveIcon,
  Trash2Icon,
  UploadIcon,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Dialog, DialogClose, DialogDescription, DialogFooter, DialogHeader, DialogPanel, DialogPopup, DialogTitle } from "@/registry/default/ui/dialog";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Separator } from "@/registry/default/ui/separator";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/registry/default/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { Textarea } from "@/registry/default/ui/textarea";
import {
  api,
  getServiceKey,
  setServiceKey,
  type ImportBatch,
  type ImportBatchDetail,
  type TokenCounts,
  type TokenItem,
  type TokenPlanCount,
} from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";
import {
  errorMessage,
  formatUSDOptional,
  importBatchCancelable,
  importBatchStatusLabel,
  importBatchTotal,
  normalizePlanValue,
  planOptionsWithCounts,
  statusOptionsWithCounts,
  tokenPlanType,
  tokenStatusOf,
  tokenTitle,
} from "@/shared/domain";
import {
  EmptyState,
  ErrorAlert,
  LoadingRows,
  SelectField,
  TokenConcurrency,
  TokenObservedCost,
  TokenStatusPlan,
  TokenQuotaStrip,
  importBatchBadge,
} from "@/shared/components";
import type { RouteState } from "@/app/router";
import { navigateTo as go } from "@/app/router";
import type { ToastMessage, TokenStatus } from "@/shared/types";

function oauthStateFromURL(value: string): string {
  try {
    return new URL(value).searchParams.get("state") || "";
  } catch {
    return "";
  }
}

function parseOAuthCallbackInput(value: string): { code: string; state: string } {
  const trimmed = value.trim();
  if (!trimmed) {
    return { code: "", state: "" };
  }
  const candidate = trimmed.startsWith("?") ? `http://localhost:1455/auth/callback${trimmed}` : trimmed;
  if (candidate.includes("code=")) {
    try {
      const parsed = new URL(candidate);
      const code = parsed.searchParams.get("code")?.trim() || "";
      const state = parsed.searchParams.get("state")?.trim() || "";
      if (code) {
        return { code, state };
      }
    } catch {
      const code = candidate.match(/[?&]code=([^&]+)/)?.[1] || "";
      const state = candidate.match(/[?&]state=([^&]+)/)?.[1] || "";
      if (code) {
        return {
          code: decodeURIComponent(code).trim(),
          state: decodeURIComponent(state).trim(),
        };
      }
    }
  }
  return { code: trimmed, state: "" };
}

export function ImportsPage({
  pushToast,
  refreshNonce,
  route,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  route: RouteState;
}) {
  return <ImportBatchesPage pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
}

function ImportForm({
  onImported,
  pushToast,
}: {
  onImported?: () => void | Promise<void>;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
}) {
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [tokenInput, setTokenInput] = useState("");
  const [importSource, setImportSource] = useState<"paste" | "file" | "oauth">("paste");
  const [queuePosition, setQueuePosition] = useState<"front" | "back">("front");
  const [importFeedback, setImportFeedback] = useState("等待导入。");
  const [importBusy, setImportBusy] = useState(false);
  const [oauthSession, setOAuthSession] = useState<{
    authUrl: string;
    redirectUri?: string;
    sessionId: string;
    state: string;
  } | null>(null);
  const [oauthCallbackInput, setOAuthCallbackInput] = useState("");
  const [oauthCopied, setOAuthCopied] = useState(false);
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

  function changeImportSource(value: unknown) {
    if (value === "paste" || value === "file" || value === "oauth") {
      setImportSource(value);
    }
  }

  async function startOAuthImport() {
    setImportBusy(true);
    try {
      if (serviceKeyDraft.trim()) {
        setServiceKey(serviceKeyDraft);
      }
      setImportFeedback("正在生成 ChatGPT 授权链接...");
      const result = await api.startOpenAIOAuth({
        import_queue_position: queuePosition,
      });
      if (!result.auth_url) {
        throw new Error("授权地址为空");
      }
      const state = oauthStateFromURL(result.auth_url) || result.state || "";
      if (!result.session_id || !state) {
        throw new Error("授权会话缺少 session_id 或 state");
      }
      setOAuthSession({
        authUrl: result.auth_url,
        redirectUri: result.redirect_uri,
        sessionId: result.session_id,
        state,
      });
      setOAuthCallbackInput("");
      setImportFeedback("授权链接已生成。授权完成后粘贴回调 URL 或 code。");
      window.open(result.auth_url, "_blank", "noopener,noreferrer");
    } catch (caught) {
      setImportFeedback(errorMessage(caught));
      pushToast(errorMessage(caught), "error");
    } finally {
      setImportBusy(false);
    }
  }

  async function copyOAuthURL() {
    if (!oauthSession?.authUrl) {
      return;
    }
    try {
      await navigator.clipboard.writeText(oauthSession.authUrl);
      setOAuthCopied(true);
      window.setTimeout(() => setOAuthCopied(false), 1200);
      pushToast("授权链接已复制");
    } catch {
      pushToast("复制失败，请手动复制", "error");
    }
  }

  async function completeOAuthImport() {
    if (!oauthSession) {
      await startOAuthImport();
      return;
    }
    const parsed = parseOAuthCallbackInput(oauthCallbackInput);
    const code = parsed.code || oauthCallbackInput.trim();
    const state = parsed.state || oauthSession.state;
    if (!code || !state) {
      setImportFeedback("请粘贴授权后的回调 URL 或 code。");
      return;
    }
    setImportBusy(true);
    try {
      setImportFeedback("正在兑换 OAuth code 并创建导入批次...");
      const result = await api.exchangeOpenAIOAuth({
        callback_url: oauthCallbackInput.trim(),
        code,
        session_id: oauthSession.sessionId,
        state,
      });
      const jobID = result.job?.id;
      setImportFeedback(jobID ? `OAuth 导入已创建批次 #${jobID}。` : "OAuth 导入已提交。");
      setOAuthSession(null);
      setOAuthCallbackInput("");
      pushToast(jobID ? `OAuth 导入已创建批次 #${jobID}` : "OAuth 导入已提交");
      await onImported?.();
    } catch (caught) {
      setImportFeedback(errorMessage(caught));
      pushToast(errorMessage(caught), "error");
    } finally {
      setImportBusy(false);
    }
  }

  async function importTokens() {
    if (importSource === "oauth") {
      await completeOAuthImport();
      return;
    }
    setImportBusy(true);
    try {
      setImportFeedback("正在解析导入内容...");
      let parsed;
      if (importSource === "file") {
        const form = new FormData();
        for (const file of Array.from(fileInputRef.current?.files || [])) {
          form.append("files", file, file.name);
        }
        parsed = await api.uploadImport(form);
      } else {
        parsed = await api.parseImport({ text: tokenInput });
      }
      const entries = parsed.items || [];
      if (!entries.length) {
        setImportFeedback("没有可导入的 token。");
        return;
      }
      setImportFeedback(`正在提交 ${formatNumber(entries.length)} 条 token...`);
      const result = await api.createImportJob({
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
      await onImported?.();
    } catch (caught) {
      setImportFeedback(errorMessage(caught));
      pushToast(errorMessage(caught), "error");
    } finally {
      setImportBusy(false);
    }
  }

  return (
    <div className="grid min-w-0 gap-4">
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
      <Tabs className="min-w-0" onValueChange={changeImportSource} value={importSource}>
        <TabsList className="w-full sm:w-fit">
          <TabsTab className="flex-1 sm:flex-none" value="paste">
            粘贴 Key 数据
          </TabsTab>
          <TabsTab className="flex-1 sm:flex-none" value="file">
            选择文件
          </TabsTab>
          <TabsTab className="flex-1 sm:flex-none" value="oauth">
            ChatGPT OAuth
          </TabsTab>
        </TabsList>
        <TabsPanel className="grid min-w-0 gap-2" keepMounted value="paste">
          <Label htmlFor="token-json">粘贴 Key 数据</Label>
          <Textarea
            className="block w-full min-w-0 max-w-full overflow-hidden"
            id="token-json"
            onChange={(event) => setTokenInput(event.currentTarget.value)}
            placeholder="支持普通 JSON / sub2api 导出 JSON；也支持每行 access_token / refresh_token，或 account_id,refresh_token。"
            rows={10}
            spellCheck={false}
            value={tokenInput}
            wrap="soft"
          />
        </TabsPanel>
        <TabsPanel className="grid min-w-0 gap-2" keepMounted value="file">
          <Label htmlFor="token-files">选择文件</Label>
          <Input
            accept=".json,.txt,.csv,application/json,text/plain"
            id="token-files"
            multiple
            nativeInput
            ref={fileInputRef}
            type="file"
          />
        </TabsPanel>
        <TabsPanel className="grid min-w-0 gap-3" keepMounted value="oauth">
          {oauthSession ? (
            <div className="grid min-w-0 gap-3 rounded-lg border bg-muted/30 p-3">
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="oauth-url">授权链接</Label>
                <div className="flex min-w-0 gap-2">
                  <Input
                    className="min-w-0 flex-1"
                    id="oauth-url"
                    nativeInput
                    readOnly
                    value={oauthSession.authUrl}
                  />
                  <Button onClick={copyOAuthURL} size="icon" title="复制授权链接" variant="outline">
                    {oauthCopied ? <CheckIcon /> : <CopyIcon />}
                  </Button>
                  <Button onClick={() => window.open(oauthSession.authUrl, "_blank", "noopener,noreferrer")} variant="outline">
                    <ExternalLinkIcon />
                    打开
                  </Button>
                </div>
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="oauth-callback">回调 URL / code</Label>
                <Textarea
                  className="block w-full min-w-0 max-w-full overflow-hidden"
                  id="oauth-callback"
                  onChange={(event) => setOAuthCallbackInput(event.currentTarget.value)}
                  placeholder="http://localhost:1455/auth/callback?code=...&state=...，或仅粘贴 code"
                  rows={4}
                  spellCheck={false}
                  value={oauthCallbackInput}
                  wrap="soft"
                />
              </div>
              <div className="flex flex-wrap gap-2">
                <Button onClick={() => void startOAuthImport()} variant="outline">
                  重新生成
                </Button>
                <Button disabled={!oauthCallbackInput.trim() || importBusy} loading={importBusy} onClick={() => void completeOAuthImport()}>
                  完成导入
                </Button>
              </div>
            </div>
          ) : (
            <Alert variant="info">
              <ExternalLinkIcon />
              <AlertTitle>ChatGPT OAuth</AlertTitle>
              <AlertDescription>生成授权链接后，在 ChatGPT 授权页完成登录，再粘贴回调 URL 或 code。</AlertDescription>
            </Alert>
          )}
        </TabsPanel>
      </Tabs>
      <div className="grid min-w-0 gap-2">
        <Label>导入位置</Label>
        <div className="flex rounded-lg bg-muted p-1">
          <Button className="flex-1" onClick={() => setQueuePosition("front")} variant={queuePosition === "front" ? "secondary" : "ghost"}>
            开头
          </Button>
          <Button className="flex-1" onClick={() => setQueuePosition("back")} variant={queuePosition === "back" ? "secondary" : "ghost"}>
            最后
          </Button>
        </div>
      </div>
      <Button disabled={importBusy} loading={importBusy} onClick={() => void importTokens()}>
        {importSource === "oauth" && <ExternalLinkIcon />}
        {importSource === "oauth" ? (oauthSession ? "完成 OAuth 导入" : "生成并打开授权链接") : "开始导入"}
      </Button>
      <Alert variant={importFeedback.includes("失败") || importFeedback.includes("错误") ? "error" : "info"}>
        <UploadIcon />
        <AlertTitle>导入状态</AlertTitle>
        <AlertDescription>{importFeedback}</AlertDescription>
      </Alert>
    </div>
  );
}

function ImportNewDialog({
  onImported,
  onOpenChange,
  open,
  pushToast,
}: {
  onImported: () => void | Promise<void>;
  onOpenChange: (open: boolean) => void;
  open: boolean;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogPopup className="max-w-[min(48rem,calc(100vw-2rem))]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <UploadIcon className="size-5" />
            导入 Key
          </DialogTitle>
          <DialogDescription>保存凭证并导入 access token / refresh token 数据，或使用 ChatGPT OAuth 授权。</DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <ImportForm onImported={onImported} pushToast={pushToast} />
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}

function ImportBatchesPage({
  pushToast,
  refreshNonce,
  route,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  route: RouteState;
}) {
  const [batches, setBatches] = useState<ImportBatch[]>([]);
  const [detailDialogId, setDetailDialogId] = useState<number | null>(null);
  const [detailStatus, setDetailStatus] = useState<TokenStatus>("all");
  const [detailPlan, setDetailPlan] = useState("all");
  const [importDialogOpen, setImportDialogOpen] = useState(route.key === "import_new");
  const [deleteTarget, setDeleteTarget] = useState<ImportBatch | null>(null);
  const [details, setDetails] = useState<Record<number, ImportBatchDetail>>({});
  const [detailErrors, setDetailErrors] = useState<Record<number, string>>({});
  const [detailLoadingId, setDetailLoadingId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const handledOAuthResultRef = useRef("");

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

  useEffect(() => {
    const params = new URLSearchParams(route.search);
    const jobID = params.get("oauth_job");
    const oauthError = params.get("oauth_error");
    if (!jobID && !oauthError) {
      return;
    }
    if (handledOAuthResultRef.current === route.search) {
      return;
    }
    handledOAuthResultRef.current = route.search;
    if (oauthError) {
      pushToast(`OAuth 导入失败：${oauthError}`, "error");
    } else {
      pushToast(`OAuth 导入已创建批次 #${jobID}`);
    }
    void loadBatches();
    go("/imports", { replace: true });
  }, [loadBatches, pushToast, route.search]);

  useEffect(() => {
    if (route.key === "import_new") {
      setImportDialogOpen(true);
    }
  }, [route.key]);

  function changeImportDialogOpen(open: boolean) {
    setImportDialogOpen(open);
    if (!open && route.key === "import_new") {
      go("/imports", { replace: true });
    }
  }

  async function cancelJob(id: number) {
    await api.cancelImportJob(id);
    pushToast("已发送取消导入请求");
    await loadBatches();
    await loadDetail(id);
  }

  async function deleteJob() {
    if (!deleteTarget) {
      return;
    }
    const id = deleteTarget.id;
    await api.deleteImportJob(id);
    pushToast(`已删除批次 #${id}`);
    setDeleteTarget(null);
    setDetailDialogId((current) => (current === id ? null : current));
    setDetails((items) => {
      const next = { ...items };
      delete next[id];
      return next;
    });
    setDetailErrors((items) => {
      const next = { ...items };
      delete next[id];
      return next;
    });
    await loadBatches();
  }

  function openDetail(job: ImportBatch) {
    setDetailDialogId(job.id);
    setDetailStatus("all");
    setDetailPlan("all");
    if (!details[job.id]) {
      void loadDetail(job.id);
    }
  }

  const detailJob = batches.find((job) => job.id === detailDialogId) || null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <UploadIcon className="size-5" />
          导入批次
        </CardTitle>
        <CardDescription>查看每次导入的处理结果、平均余额和关联 key。</CardDescription>
        <CardAction>
          <Button onClick={() => setImportDialogOpen(true)} size="sm">
            新导入
          </Button>
        </CardAction>
      </CardHeader>
      <CardPanel>
        {error ? (
          <ErrorAlert title="导入批次载入失败" message={error} />
        ) : (
          <>
            <ImportBatchList
              batches={batches}
              detailLoadingId={detailLoadingId}
              loading={loading && !batches.length}
              onCancel={(id) => void cancelJob(id)}
              onDelete={setDeleteTarget}
              onView={openDetail}
            />
            <ImportBatchDetailDialog
              detail={detailDialogId ? details[detailDialogId] : undefined}
              error={detailDialogId ? detailErrors[detailDialogId] : undefined}
              job={detailJob}
              loading={detailDialogId != null && detailLoadingId === detailDialogId}
              onOpenChange={(open) => {
                if (!open) {
                  setDetailDialogId(null);
                }
              }}
              onPlanChange={setDetailPlan}
              onStatusChange={(value) => setDetailStatus(readTokenStatus(value))}
              plan={detailPlan}
              status={detailStatus}
            />
            <DeleteImportBatchDialog
              onConfirm={() => void deleteJob()}
              onOpenChange={(open) => {
                if (!open) {
                  setDeleteTarget(null);
                }
              }}
              target={deleteTarget}
            />
            <ImportNewDialog
              onImported={loadBatches}
              onOpenChange={changeImportDialogOpen}
              open={importDialogOpen}
              pushToast={pushToast}
            />
          </>
        )}
      </CardPanel>
    </Card>
  );
}

function ImportBatchList({
  batches,
  detailLoadingId,
  loading,
  onCancel,
  onDelete,
  onView,
}: {
  batches: ImportBatch[];
  detailLoadingId: number | null;
  loading: boolean;
  onCancel: (id: number) => void;
  onDelete: (job: ImportBatch) => void;
  onView: (job: ImportBatch) => void;
}) {
  if (loading) {
    return <LoadingRows />;
  }
  if (!batches.length) {
    return <EmptyState title="暂无导入批次" description="导入任务提交后会在这里出现。" />;
  }
  return (
    <div className="min-w-0 overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>批次</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>数量</TableHead>
            <TableHead>进度</TableHead>
            <TableHead>变更</TableHead>
            <TableHead>余额</TableHead>
            <TableHead>时间</TableHead>
            <TableHead className="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {batches.map((job) => {
            const cancelable = importBatchCancelable(job.status);
            const deletable = importBatchDeletable(job.status);
            const errorText = job.last_error || job.error_message || "";
            return (
              <TableRow key={job.id}>
                <TableCell className="min-w-[12rem]">
                  <div className="grid min-w-0 gap-1">
                    <div className="flex min-w-0 items-center gap-2">
                      <strong className="truncate text-sm">导入批次 #{job.id}</strong>
                      <Badge className="shrink-0" size="sm" variant="outline">
                        {job.import_queue_position === "back" ? "最后" : "开头"}
                      </Badge>
                    </div>
                    {errorText && (
                      <span className="min-w-0 truncate text-destructive-foreground text-xs" title={errorText}>
                        {errorText}
                      </span>
                    )}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap items-center gap-1.5">
                    <Badge size="sm" variant={importBatchBadge(job.status)}>
                      {importBatchStatusLabel(job.status)}
                    </Badge>
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
                  </div>
                </TableCell>
                <TableCell>
                  <div className="grid gap-1 text-xs">
                    <span className="oaix-tabular font-medium">{formatNumber(importBatchTotal(job))} key</span>
                    <span className="oaix-tabular text-muted-foreground">
                      有效 {formatNumber(job.available)} · 冷却 {formatNumber(job.cooling)} · 禁用 {formatNumber(job.disabled)}
                    </span>
                  </div>
                </TableCell>
                <TableCell>
                  <span className="oaix-tabular text-sm">
                    {formatNumber(job.processed_count)} / {formatNumber(job.total_count)}
                  </span>
                </TableCell>
                <TableCell>
                  <span className="oaix-tabular text-muted-foreground text-xs">
                    新 {formatNumber(job.created_count)} · 更 {formatNumber(job.updated_count)} · 跳 {formatNumber(job.skipped_count)}
                  </span>
                </TableCell>
                <TableCell>
                  <div className="grid gap-1 text-xs">
                    <span className="oaix-tabular font-medium">{formatUSDOptional(job.observed_cost_usd)}</span>
                    <span className="oaix-tabular text-muted-foreground">平均 {formatUSDOptional(job.average_observed_cost_usd)}</span>
                  </div>
                </TableCell>
                <TableCell>
                  <div className="grid gap-1 text-muted-foreground text-xs">
                    <span className="oaix-tabular">提交 {formatDate(job.submitted_at)}</span>
                    <span className="oaix-tabular">完成 {formatDate(job.finished_at || job.completed_at)}</span>
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap justify-end gap-1">
                    <Button loading={detailLoadingId === job.id} onClick={() => onView(job)} size="xs" variant="outline">
                      <EyeIcon />
                      查看 Key
                    </Button>
                    {cancelable && (
                      <Button onClick={() => onCancel(job.id)} size="xs" variant="destructive-outline">
                        <Trash2Icon />
                        取消
                      </Button>
                    )}
                    {deletable && (
                      <Button onClick={() => onDelete(job)} size="xs" variant="destructive">
                        <Trash2Icon />
                        删除批次
                      </Button>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}

function ImportBatchDetailDialog({
  detail,
  error,
  job,
  loading,
  onOpenChange,
  onPlanChange,
  onStatusChange,
  plan,
  status,
}: {
  detail?: ImportBatchDetail;
  error?: string;
  job: ImportBatch | null;
  loading: boolean;
  onOpenChange: (open: boolean) => void;
  onPlanChange: (plan: string) => void;
  onStatusChange: (status: string) => void;
  plan: string;
  status: TokenStatus;
}) {
  const tokens = detail?.tokens || [];
  const items = detail?.items || [];
  const failedItems = items.filter((item) => item.error_message);
  const statusOptions = useMemo(() => statusOptionsWithCounts(countTokenStatuses(tokens)), [tokens]);
  const planOptions = useMemo(() => planOptionsWithCounts(countTokenPlans(tokens, status)), [status, tokens]);
  const filteredTokens = useMemo(
    () =>
      tokens.filter((token) => {
        const tokenStatus = tokenStatusOf(token);
        const matchesStatus = status === "all" || (status === "available" ? tokenStatus === "active" : tokenStatus === status);
        const matchesPlan = plan === "all" || normalizePlanValue(tokenPlanType(token)) === plan;
        return matchesStatus && matchesPlan;
      }),
    [plan, status, tokens],
  );

  return (
    <Dialog open={Boolean(job)} onOpenChange={onOpenChange}>
      <DialogPopup className="h-[min(84vh,760px)] max-w-[min(112rem,calc(100vw-2rem))]">
        <DialogHeader>
          <DialogTitle>导入批次 Key</DialogTitle>
          <DialogDescription>
            批次 #{job?.id || "-"} · 显示 {formatNumber(filteredTokens.length)} / {formatNumber(tokens.length)} 个 key
          </DialogDescription>
        </DialogHeader>
        <DialogPanel className="flex min-h-0 flex-col gap-4">
          <div className="grid min-w-0 gap-3 md:grid-cols-[minmax(0,1fr)_15rem_15rem]">
            <div className="flex min-w-0 flex-wrap items-end gap-2">
              <Badge size="sm" variant="outline">
                全部 {formatNumber(tokens.length)} key
              </Badge>
              {failedItems.length > 0 && (
                <Badge size="sm" variant="error">
                  失败项 {formatNumber(failedItems.length)}
                </Badge>
              )}
              {job && (
                <Badge size="sm" variant="secondary">
                  {importBatchStatusLabel(job.status)}
                </Badge>
              )}
            </div>
            <SelectField label="状态" onChange={onStatusChange} options={statusOptions} value={status} />
            <SelectField label="计划" onChange={onPlanChange} options={planOptions} value={plan} />
          </div>

          {error ? (
            <ErrorAlert title="批次详情载入失败" message={error} />
          ) : loading && !detail ? (
            <LoadingRows rows={6} />
          ) : !tokens.length && !failedItems.length ? (
            <div className="rounded-lg border border-dashed bg-muted/24 p-4 text-muted-foreground text-sm">这个批次暂时没有可关联的 key 明细。</div>
          ) : filteredTokens.length > 0 ? (
            <div className="max-h-[min(52vh,28rem)] min-w-0 overflow-auto rounded-lg border oaix-scrollbar">
              <Table className="table-fixed">
                <colgroup>
                  <col className="w-[28%]" />
                  <col className="w-[7rem]" />
                  <col className="w-[10rem]" />
                  <col className="w-[10rem]" />
                  <col className="w-[9.5rem]" />
                  <col />
                </colgroup>
                <TableHeader>
                  <TableRow>
                    <TableHead>Key</TableHead>
                    <TableHead>状态</TableHead>
                    <TableHead>额度</TableHead>
                    <TableHead>并发 / 金额</TableHead>
                    <TableHead>最近</TableHead>
                    <TableHead>备注</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredTokens.map((token) => (
                    <ImportBatchTokenRow key={token.id} token={token} />
                  ))}
                </TableBody>
              </Table>
            </div>
          ) : (
            <div className="rounded-lg border border-dashed bg-muted/24 p-4 text-muted-foreground text-sm">当前筛选下没有 key。</div>
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
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}

function countTokenStatuses(tokens: TokenItem[]): TokenCounts {
  const counts: TokenCounts = { available: 0, cooling: 0, disabled: 0, total: tokens.length };
  for (const token of tokens) {
    const status = tokenStatusOf(token);
    if (status === "active") {
      counts.available = (counts.available || 0) + 1;
    } else if (status === "cooling") {
      counts.cooling = (counts.cooling || 0) + 1;
    } else {
      counts.disabled = (counts.disabled || 0) + 1;
    }
  }
  return counts;
}

function countTokenPlans(tokens: TokenItem[], status: TokenStatus = "all"): TokenPlanCount[] {
  const countByPlan = new Map<string, number>();
  for (const token of tokens) {
    const tokenStatus = tokenStatusOf(token);
    const matchesStatus = status === "all" || (status === "available" ? tokenStatus === "active" : tokenStatus === status);
    if (!matchesStatus) {
      continue;
    }
    const plan = normalizePlanValue(tokenPlanType(token)) || "unknown";
    countByPlan.set(plan, (countByPlan.get(plan) || 0) + 1);
  }
  return Array.from(countByPlan.entries()).map(([plan, count]) => ({
    count,
    label: undefined,
    plan,
  }));
}

function readTokenStatus(value: string): TokenStatus {
  if (value === "all" || value === "cooling" || value === "disabled") {
    return value;
  }
  return "available";
}

function importBatchDeletable(status?: string): boolean {
  return !["queued", "running", "validating", "publishing"].includes(String(status || "").toLowerCase());
}

function DeleteImportBatchDialog({
  onConfirm,
  onOpenChange,
  target,
}: {
  onConfirm: () => void;
  onOpenChange: (open: boolean) => void;
  target: ImportBatch | null;
}) {
  const tokenCount = target ? importBatchTotal(target) : 0;
  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogPopup className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>删除导入批次？</DialogTitle>
          <DialogDescription>
            批次 #{target?.id || "-"} 会被删除，批次内关联的 {formatNumber(tokenCount)} 个 key 也会一起删除。此操作不可撤销。
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose render={<Button variant="ghost" />}>取消</DialogClose>
          <Button onClick={onConfirm} variant="destructive">
            <Trash2Icon />
            删除批次和 Key
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

function ImportBatchTokenRow({ token }: { token: TokenItem }) {
  const status = tokenStatusOf(token);
  const title = tokenTitle(token);
  const planType = tokenPlanType(token);
  const note = token.remark || token.source_file || "-";
  return (
    <TableRow className="h-auto">
      <TableCell className="min-w-0 py-2 align-middle">
        <div className="grid max-h-11 min-w-0 gap-1 overflow-hidden">
          <div className="flex min-w-0 items-center gap-1.5">
            <span className="min-w-0 truncate font-medium" title={title}>
              {title}
            </span>
            <Badge className="shrink-0" size="sm" variant="outline">
              ID {token.id}
            </Badge>
          </div>
          {token.account_id && token.email && (
            <span className="min-w-0 truncate text-muted-foreground text-xs" title={token.account_id}>
              {token.account_id}
            </span>
          )}
        </div>
      </TableCell>
      <TableCell className="py-2 align-middle">
        <TokenStatusPlan plan={planType} status={status} />
      </TableCell>
      <TableCell className="py-2 align-middle">
        <div className="flex max-h-11 flex-wrap items-center gap-1 overflow-hidden text-[11px]">
          <TokenQuotaStrip quota={token.quota} />
        </div>
      </TableCell>
      <TableCell className="py-2 align-middle">
        <div className="flex max-h-11 flex-wrap items-center gap-1 overflow-hidden text-[11px]">
          <TokenConcurrency fallbackCap={Number(token.active_stream_cap || 0)} item={token} />
          <TokenObservedCost value={token.observed_cost_usd} />
        </div>
      </TableCell>
      <TableCell className="py-2 align-middle">
        <div className="grid max-h-10 gap-1 overflow-hidden text-muted-foreground text-xs">
          <span className="oaix-tabular">最近 {formatDate(token.last_used_at)}</span>
          <span className="oaix-tabular">冷却 {formatDate(token.cooldown_until)}</span>
        </div>
      </TableCell>
      <TableCell className="py-2 align-top">
        <span className="block min-w-0 whitespace-pre-wrap break-words text-muted-foreground text-xs leading-relaxed [overflow-wrap:anywhere]" title={note}>
          {note}
        </span>
      </TableCell>
    </TableRow>
  );
}
