import {
  ActivityIcon,
  AlertCircleIcon,
  CheckCircle2Icon,
  ChevronLeftIcon,
  ChevronRightIcon,
  DatabaseIcon,
  GaugeIcon,
  KeyRoundIcon,
  ListFilterIcon,
  Loader2Icon,
  MoonIcon,
  RefreshCwIcon,
  SaveIcon,
  SearchIcon,
  Settings2Icon,
  ShieldCheckIcon,
  SunIcon,
  Trash2Icon,
  UploadIcon,
} from "lucide-react";
import type * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardPanel,
  CardTitle,
} from "@/registry/default/ui/card";
import { Checkbox } from "@/registry/default/ui/checkbox";
import {
  Dialog,
  DialogClose,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "@/registry/default/ui/dialog";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/registry/default/ui/empty";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "@/registry/default/ui/select";
import { Separator } from "@/registry/default/ui/separator";
import { Skeleton } from "@/registry/default/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/registry/default/ui/table";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/registry/default/ui/tabs";
import { Textarea } from "@/registry/default/ui/textarea";
import { cn } from "@/registry/default/lib/utils";
import {
  api,
  getServiceKey,
  hasServiceKey,
  setServiceKey,
  type HealthResponse,
  type ImportBatch,
  type RequestItem,
  type RequestSummary,
  type SettingItem,
  type TokenCounts,
  type TokenItem,
  type TokenProbeResponse,
  type TokenQuotaWindow,
} from "./lib/api";
import { clamp, formatCurrency, formatDate, formatNumber } from "./lib/format";

declare global {
  interface Window {
    __OAIX_WEB_VERSION__?: {
      hash: string;
      time: string;
    };
  }
}

type ThemePreference = "auto" | "light" | "dark";
type TokenStatus = "all" | "available" | "cooling" | "disabled";
type TokenMode = "keys" | "import_batches";
type ToastMessage = {
  id: number;
  title: string;
  variant: "success" | "warning" | "error" | "info";
};
type DeleteTarget = {
  ids: number[];
  title: string;
  description: string;
};
type RemarkTarget = {
  id: number;
  title: string;
  meta: string;
  remark: string;
};

const PAGE_SIZE = 100;
const STATUS_OPTION_DEFINITIONS: Array<{ label: string; value: TokenStatus }> = [
  { label: "全部", value: "all" },
  { label: "有效", value: "available" },
  { label: "冷却", value: "cooling" },
  { label: "禁用", value: "disabled" },
];
const SORT_OPTIONS = [
  { label: "最新入库", value: "-created_at" },
  { label: "最早入库", value: "created_at" },
  { label: "最近使用", value: "-last_used_at" },
  { label: "状态优先", value: "status" },
];

export function App(): React.ReactElement {
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [theme, setTheme] = useState<ThemePreference>(() => readThemePreference());
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [counts, setCounts] = useState<TokenCounts>({});
  const [protectedMode, setProtectedMode] = useState(false);
  const [authBlocked, setAuthBlocked] = useState(false);
  const [syncText, setSyncText] = useState("等待同步");
  const [loading, setLoading] = useState(true);
  const [tokenLoading, setTokenLoading] = useState(false);
  const [requestLoading, setRequestLoading] = useState(false);
  const [settingsLoading, setSettingsLoading] = useState(false);
  const [tokenError, setTokenError] = useState("");
  const [requestError, setRequestError] = useState("");
  const [settingsError, setSettingsError] = useState("");
  const [tokens, setTokens] = useState<TokenItem[]>([]);
  const [tokenTotal, setTokenTotal] = useState(0);
  const [tokenPage, setTokenPage] = useState(1);
  const [tokenStatus, setTokenStatus] = useState<TokenStatus>("available");
  const [tokenSort, setTokenSort] = useState("-created_at");
  const [tokenSearch, setTokenSearch] = useState("");
  const [tokenMode, setTokenMode] = useState<TokenMode>("keys");
  const [selectedIds, setSelectedIds] = useState<Set<number>>(() => new Set());
  const [probeBusyIds, setProbeBusyIds] = useState<Set<number>>(() => new Set());
  const [probeResults, setProbeResults] = useState<Record<number, TokenProbeResponse>>({});
  const [importBatches, setImportBatches] = useState<ImportBatch[]>([]);
  const [requests, setRequests] = useState<RequestItem[]>([]);
  const [requestSummary, setRequestSummary] = useState<RequestSummary>({});
  const [modelStats, setModelStats] = useState<Array<Record<string, unknown>>>([]);
  const [settings, setSettings] = useState<SettingItem[]>([]);
  const [settingKey, setSettingKey] = useState("");
  const [settingValue, setSettingValue] = useState("{\n  \"enabled\": true\n}");
  const [selectionSummary, setSelectionSummary] = useState("等待载入");
  const [streamCap, setStreamCap] = useState(10);
  const [tokenInput, setTokenInput] = useState("");
  const [queuePosition, setQueuePosition] = useState<"front" | "back">("front");
  const [importFeedback, setImportFeedback] = useState("等待导入。");
  const [importBusy, setImportBusy] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [remarkTarget, setRemarkTarget] = useState<RemarkTarget | null>(null);
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const searchTimer = useRef<number | null>(null);

  const totalPages = Math.max(1, Math.ceil(tokenTotal / PAGE_SIZE));
  const selectedCount = selectedIds.size;
  const webVersion = window.__OAIX_WEB_VERSION__;

  const pushToast = useCallback((title: string, variant: ToastMessage["variant"] = "success") => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setToasts((items) => [...items.slice(-3), { id, title, variant }]);
    window.setTimeout(() => {
      setToasts((items) => items.filter((item) => item.id !== id));
    }, 4200);
  }, []);

  const loadTokenSelection = useCallback(async () => {
    const payload = await api.tokenSelection();
    const cap = Number(payload.active_stream_cap || 10);
    setSelectionSummary(String(payload.strategy || "snapshot_round_robin"));
    setStreamCap(Number.isFinite(cap) ? clamp(cap, 1, 10) : 10);
  }, []);

  const loadTokens = useCallback(async () => {
    if (tokenMode !== "keys") {
      setTokenLoading(true);
      setTokenError("");
      try {
        const payload = await api.importJobs(50);
        setImportBatches(payload.items || []);
      } catch (error) {
        setTokenError(errorMessage(error));
      } finally {
        setTokenLoading(false);
      }
      return;
    }

    setTokenLoading(true);
    setTokenError("");
    try {
      const params = new URLSearchParams({
        include_quota: "true",
        limit: String(PAGE_SIZE),
        offset: String((tokenPage - 1) * PAGE_SIZE),
      });
      if (tokenSearch.trim()) {
        params.set("q", tokenSearch.trim());
      }
      if (tokenStatus !== "all") {
        params.set("status", tokenStatus);
      }
      const sort = sortParam(tokenSort);
      if (sort) {
        params.set("sort", sort);
      }
      const payload = await api.listTokens(params);
      const items = payload.items || [];
      setTokens(items);
      setTokenTotal(payload.pagination?.total ?? items.length);
      setCounts(payload.counts || {});
      setSelectedIds(new Set());
    } catch (error) {
      setTokenError(errorMessage(error));
      setTokens([]);
    } finally {
      setTokenLoading(false);
    }
  }, [tokenMode, tokenPage, tokenSearch, tokenSort, tokenStatus]);

  const loadRequests = useCallback(async () => {
    setRequestLoading(true);
    setRequestError("");
    try {
      const [requestPayload, analyticsPayload] = await Promise.all([api.requests(80), api.analytics(24)]);
      setRequests(requestPayload.items || []);
      setRequestSummary(requestPayload.summary || {});
      setModelStats(Array.isArray(analyticsPayload.by_model) ? analyticsPayload.by_model : []);
    } catch (error) {
      setRequestError(errorMessage(error));
      setRequests([]);
      setModelStats([]);
    } finally {
      setRequestLoading(false);
    }
  }, []);

  const loadSettings = useCallback(async () => {
    setSettingsLoading(true);
    setSettingsError("");
    try {
      const payload = await api.settings();
      setSettings(payload.items || []);
    } catch (error) {
      setSettingsError(errorMessage(error));
    } finally {
      setSettingsLoading(false);
    }
  }, []);

  const refreshAll = useCallback(async () => {
    setLoading(true);
    try {
      const healthPayload = await api.health();
      setHealth(healthPayload);
      setCounts(healthPayload.counts || {});
      setProtectedMode(Boolean(healthPayload.service_key_protected));
      if (healthPayload.service_key_protected && !hasServiceKey()) {
        setAuthBlocked(true);
        setSyncText(`需要凭证 · ${new Date().toLocaleTimeString("zh-CN")}`);
        return;
      }
      setAuthBlocked(false);
      await Promise.allSettled([loadTokenSelection(), loadTokens(), loadRequests(), loadSettings()]);
      setSyncText(`已同步 ${new Date().toLocaleTimeString("zh-CN")}`);
    } catch (error) {
      setHealth(null);
      pushToast(errorMessage(error), "error");
      setSyncText(`同步失败 ${new Date().toLocaleTimeString("zh-CN")}`);
    } finally {
      setLoading(false);
    }
  }, [loadRequests, loadSettings, loadTokenSelection, loadTokens, pushToast]);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => void refreshAll(), 30_000);
    return () => window.clearInterval(timer);
  }, [refreshAll]);

  useEffect(() => {
    if (!health || (health.service_key_protected && !hasServiceKey())) {
      return;
    }
    if (searchTimer.current != null) {
      window.clearTimeout(searchTimer.current);
    }
    searchTimer.current = window.setTimeout(
      () => {
        void loadTokens();
      },
      tokenSearch.trim() ? 260 : 0,
    );
    return () => {
      if (searchTimer.current != null) {
        window.clearTimeout(searchTimer.current);
      }
    };
  }, [health, loadTokens, tokenSearch]);

  async function saveServiceKey() {
    setServiceKey(serviceKeyDraft);
    pushToast("凭证已保存");
    await refreshAll();
  }

  async function clearServiceKey() {
    setServiceKey("");
    setServiceKeyDraft("");
    pushToast("凭证已清空", "info");
    await refreshAll();
  }

  async function importTokens() {
    setImportBusy(true);
    try {
      setImportFeedback("正在解析导入内容...");
      const entries = await collectImportEntries(tokenInput, fileInputRef.current?.files);
      if (!entries.length) {
        setImportFeedback("没有可导入的 token。");
        pushToast("没有可导入的 token", "warning");
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
        `导入任务 #${job.id || "-"} 已提交：新增 ${summary.created || 0}，更新 ${summary.updated || 0}，跳过 ${summary.skipped || 0}。`,
      );
      setTokenInput("");
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      pushToast("导入任务已提交");
      await refreshAll();
    } catch (error) {
      setImportFeedback(errorMessage(error));
      pushToast(errorMessage(error), "error");
    } finally {
      setImportBusy(false);
    }
  }

  async function updateActivation(id: number, active: boolean) {
    await api.updateActivation(id, { active, clear_cooldown: true });
    pushToast(active ? "Key 已启用" : "Key 已禁用");
    await refreshAll();
  }

  async function runTokenProbe(id: number) {
    setProbeBusyIds((current) => new Set(current).add(id));
    try {
      const result = await api.probeToken(id);
      setProbeResults((current) => ({ ...current, [id]: result }));
      pushToast(result.message || "测试完成", probeToastVariant(result.outcome));
      await loadTokens();
    } catch (error) {
      const message = errorMessage(error);
      setProbeResults((current) => ({
        ...current,
        [id]: {
          detail: message,
          id,
          message,
          outcome: "inconclusive",
        },
      }));
      pushToast(message, "error");
    } finally {
      setProbeBusyIds((current) => {
        const next = new Set(current);
        next.delete(id);
        return next;
      });
    }
  }

  async function runBatchActivation(active: boolean) {
    const ids = [...selectedIds];
    if (!ids.length) {
      pushToast("请先选择 key", "warning");
      return;
    }
    await api.batchTokens({
      action: "set_active",
      active,
      clear_cooldown: true,
      token_ids: ids,
    });
    pushToast(active ? "批量启用完成" : "批量禁用完成");
    await refreshAll();
  }

  async function confirmDelete() {
    if (!deleteTarget) {
      return;
    }
    if (deleteTarget.ids.length === 1) {
      await api.deleteToken(deleteTarget.ids[0]);
    } else {
      await api.batchTokens({ action: "delete", token_ids: deleteTarget.ids });
    }
    setDeleteTarget(null);
    pushToast("Key 已删除");
    await refreshAll();
  }

  async function saveRemark() {
    if (!remarkTarget) {
      return;
    }
    await api.updateRemark(remarkTarget.id, remarkTarget.remark);
    setRemarkTarget(null);
    pushToast("备注已保存");
    await refreshAll();
  }

  async function saveStreamCap() {
    await api.updateTokenSelection({ active_stream_cap: streamCap });
    pushToast("调度设置已提交");
    await loadTokenSelection();
  }

  async function saveSetting() {
    if (!settingKey.trim()) {
      pushToast("请输入设置 key", "warning");
      return;
    }
    let value: unknown = settingValue;
    try {
      value = settingValue.trim() ? JSON.parse(settingValue) : null;
    } catch {
      pushToast("Value 必须是合法 JSON", "error");
      return;
    }
    await api.updateSetting(settingKey.trim(), value);
    pushToast("设置已保存");
    await loadSettings();
  }

  const statusCounts = useMemo(() => {
    const total = Math.max(1, counts.total || 0);
    return {
      available: Math.round(((counts.available || 0) / total) * 100),
      cooling: Math.round(((counts.cooling || 0) / total) * 100),
      disabled: Math.round(((counts.disabled || 0) / total) * 100),
    };
  }, [counts]);

  return (
    <div className="min-h-screen px-4 py-5 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-[1480px] flex-col gap-5">
        <header className="grid gap-4 rounded-2xl border bg-card/80 p-5 shadow-xs/5 backdrop-blur md:grid-cols-[1fr_auto] md:items-end">
          <div className="min-w-0">
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <Badge variant="outline">oaix</Badge>
              <Badge variant={protectedMode ? "success" : "warning"}>
                {protectedMode ? "Service API Key 已启用" : "未启用服务侧凭证"}
              </Badge>
              <Badge variant="secondary">{syncText}</Badge>
            </div>
            <h1 className="font-heading text-3xl font-semibold tracking-normal sm:text-4xl">Key 池控制台</h1>
            <p className="mt-2 max-w-3xl text-muted-foreground text-sm leading-6">
              管理 key 状态、导入 token、观察请求、调整调度和运行设置。
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2 md:justify-end">
            <ThemeButton value="auto" current={theme} onSelect={setTheme} />
            <ThemeButton value="light" current={theme} onSelect={setTheme} icon={<SunIcon />} />
            <ThemeButton value="dark" current={theme} onSelect={setTheme} icon={<MoonIcon />} />
            <Button onClick={() => void refreshAll()} variant="outline">
              <RefreshCwIcon className={cn(loading && "animate-spin")} />
              刷新
            </Button>
          </div>
        </header>

        {authBlocked && (
          <Alert variant="warning">
            <ShieldCheckIcon />
            <AlertTitle>管理接口需要 Service API Key</AlertTitle>
            <AlertDescription>请先在导入面板填写并保存凭证，然后刷新管理数据。</AlertDescription>
          </Alert>
        )}

        <main className="grid gap-5 oaix-panel-grid">
          <section className="flex min-w-0 flex-col gap-5">
            <OverviewCards counts={counts} health={health} loading={loading && !counts.total} statusCounts={statusCounts} />
            <TokenExplorer
              counts={counts}
              deleteTarget={deleteTarget}
              importBatches={importBatches}
              mode={tokenMode}
              onBatchActivation={(active) => void runBatchActivation(active)}
              onCancelImportJob={async (id) => {
                await api.cancelImportJob(id);
                pushToast("已发送取消请求");
                await loadTokens();
              }}
              onDelete={(target) => setDeleteTarget(target)}
              onModeChange={(mode) => {
                setTokenMode(mode);
                setTokenPage(1);
              }}
              onPageChange={(page) => {
                setTokenPage(clamp(page, 1, totalPages));
              }}
              onRemark={(target) => setRemarkTarget(target)}
              onProbe={(id) => void runTokenProbe(id)}
              onSearchChange={(value) => {
                setTokenSearch(value);
                setTokenPage(1);
              }}
              onSelectedChange={setSelectedIds}
              onSortChange={(value) => {
                setTokenSort(value);
                setTokenPage(1);
              }}
              onStatusChange={(status) => {
                setTokenStatus(status);
                setTokenPage(1);
              }}
              onToggleActivation={(id, active) => void updateActivation(id, active)}
              activeStreamCap={streamCap}
              page={tokenPage}
              probeBusyIds={probeBusyIds}
              probeResults={probeResults}
              search={tokenSearch}
              selectedIds={selectedIds}
              sort={tokenSort}
              status={tokenStatus}
              tokenError={tokenError}
              tokenLoading={tokenLoading && !tokens.length}
              tokenTotal={tokenTotal}
              tokens={tokens}
              totalPages={totalPages}
            />
            <RequestPanel
              error={requestError}
              loading={requestLoading && !requests.length && !requestSummary.total}
              modelStats={modelStats}
              requests={requests}
              summary={requestSummary}
            />
          </section>

          <aside className="flex min-w-0 flex-col gap-5">
            <CredentialPanel
              draft={serviceKeyDraft}
              importBusy={importBusy}
              importFeedback={importFeedback}
              onClearKey={() => void clearServiceKey()}
              onDraftChange={setServiceKeyDraft}
              onImport={() => void importTokens()}
              onQueuePositionChange={setQueuePosition}
              onSaveKey={() => void saveServiceKey()}
              onTokenInputChange={setTokenInput}
              queuePosition={queuePosition}
              tokenInput={tokenInput}
              fileInputRef={fileInputRef}
            />
            <DispatchPanel
              selectionSummary={selectionSummary}
              streamCap={streamCap}
              onStreamCapChange={setStreamCap}
              onSave={() => void saveStreamCap()}
            />
            <SettingsPanel
              error={settingsError}
              items={settings}
              loading={settingsLoading}
              onRefresh={() => void loadSettings()}
              onSave={() => void saveSetting()}
              settingKey={settingKey}
              settingValue={settingValue}
              setSettingKey={setSettingKey}
              setSettingValue={setSettingValue}
            />
          </aside>
        </main>

        <footer className="flex flex-wrap items-center justify-between gap-2 pb-4 text-muted-foreground text-xs">
          <span>oaix admin console</span>
          <span title={`资源版本 ${webVersion?.hash || "-"}`}>前端版本 {webVersion?.time || "-"}</span>
        </footer>
      </div>

      <DeleteDialog target={deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)} onConfirm={() => void confirmDelete()} />
      <RemarkDialog
        target={remarkTarget}
        onChange={(remark) => setRemarkTarget((target) => (target ? { ...target, remark } : target))}
        onOpenChange={(open) => !open && setRemarkTarget(null)}
        onSave={() => void saveRemark()}
      />
      <ToastStack items={toasts} />
    </div>
  );
}

function OverviewCards({
  counts,
  health,
  loading,
  statusCounts,
}: {
  counts: TokenCounts;
  health: HealthResponse | null;
  loading: boolean;
  statusCounts: { available: number; cooling: number; disabled: number };
}) {
  return (
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
          <Metric label="有效 Key" loading={loading} tone="success" value={counts.available} />
          <Metric label="冷却中" loading={loading} tone="warning" value={counts.cooling} />
          <Metric label="已禁用" loading={loading} tone="error" value={counts.disabled} />
        </div>
        <div className="rounded-xl border bg-muted/40 p-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <div>
              <div className="font-medium text-sm">状态分布</div>
              <div className="text-muted-foreground text-xs">总量 {formatNumber(counts.total)}</div>
            </div>
            <Badge variant={health?.ok === false ? "warning" : "success"}>
              {health?.ok === false ? "degraded" : "healthy"}
            </Badge>
          </div>
          <div className="flex h-2 overflow-hidden rounded-full bg-muted">
            <span className="bg-success" style={{ width: `${statusCounts.available}%` }} />
            <span className="bg-warning" style={{ width: `${statusCounts.cooling}%` }} />
            <span className="bg-destructive" style={{ width: `${statusCounts.disabled}%` }} />
          </div>
        </div>
      </CardPanel>
    </Card>
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
    <div className="rounded-xl border bg-card p-4">
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="text-muted-foreground text-sm">{label}</span>
        <Badge variant={tone}>{tone}</Badge>
      </div>
      {loading ? <Skeleton className="h-9 w-24" /> : <div className="oaix-tabular font-heading text-3xl font-semibold">{formatNumber(value)}</div>}
    </div>
  );
}

function CredentialPanel({
  draft,
  fileInputRef,
  importBusy,
  importFeedback,
  onClearKey,
  onDraftChange,
  onImport,
  onQueuePositionChange,
  onSaveKey,
  onTokenInputChange,
  queuePosition,
  tokenInput,
}: {
  draft: string;
  fileInputRef: React.RefObject<HTMLInputElement | null>;
  importBusy: boolean;
  importFeedback: string;
  onClearKey: () => void;
  onDraftChange: (value: string) => void;
  onImport: () => void;
  onQueuePositionChange: (value: "front" | "back") => void;
  onSaveKey: () => void;
  onTokenInputChange: (value: string) => void;
  queuePosition: "front" | "back";
  tokenInput: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <UploadIcon className="size-5" />
          导入 Key
        </CardTitle>
        <CardDescription>保存凭证并导入 access token / refresh token 数据。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <div className="grid gap-2">
          <Label htmlFor="service-key">Service API Key</Label>
          <Input
            id="service-key"
            nativeInput
            onChange={(event) => onDraftChange(event.currentTarget.value)}
            placeholder="如果启用了 SERVICE_API_KEYS，请填在这里"
            type="password"
            value={draft}
          />
        </div>
        <div className="flex flex-wrap gap-2">
          <Button onClick={onSaveKey}>
            <SaveIcon />
            保存凭证
          </Button>
          <Button onClick={onClearKey} variant="outline">
            清空
          </Button>
        </div>
        <Separator />
        <div className="grid gap-2">
          <Label htmlFor="token-json">粘贴 Key 数据</Label>
          <Textarea
            id="token-json"
            onChange={(event) => onTokenInputChange(event.currentTarget.value)}
            placeholder="支持普通 JSON / sub2api 导出 JSON；也支持每行 access_token / refresh_token，或 account_id,refresh_token。"
            rows={9}
            spellCheck={false}
            value={tokenInput}
          />
        </div>
        <div className="grid gap-2">
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
        <div className="grid gap-2">
          <Label>导入位置</Label>
          <div className="flex rounded-lg bg-muted p-1">
            <Button
              className="flex-1"
              onClick={() => onQueuePositionChange("front")}
              variant={queuePosition === "front" ? "secondary" : "ghost"}
            >
              开头
            </Button>
            <Button
              className="flex-1"
              onClick={() => onQueuePositionChange("back")}
              variant={queuePosition === "back" ? "secondary" : "ghost"}
            >
              最后
            </Button>
          </div>
        </div>
        <Button disabled={importBusy} loading={importBusy} onClick={onImport}>
          开始导入
        </Button>
        <Alert variant={importFeedback.includes("失败") || importFeedback.includes("错误") ? "error" : "info"}>
          <AlertCircleIcon />
          <AlertTitle>导入状态</AlertTitle>
          <AlertDescription>{importFeedback}</AlertDescription>
        </Alert>
      </CardPanel>
    </Card>
  );
}

function DispatchPanel({
  onSave,
  onStreamCapChange,
  selectionSummary,
  streamCap,
}: {
  onSave: () => void;
  onStreamCapChange: (value: number) => void;
  selectionSummary: string;
  streamCap: number;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2Icon className="size-5" />
          调度设置
        </CardTitle>
        <CardDescription>Key 分发、每 Key 并发和当前 selector 状态。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <Alert variant="info">
          <DatabaseIcon />
          <AlertTitle>{selectionSummary}</AlertTitle>
          <AlertDescription>Go 网关按 snapshot selector 调度，保存后由后端配置决定是否立即生效。</AlertDescription>
        </Alert>
        <div className="grid gap-2">
          <Label htmlFor="stream-cap">每 Key 并发</Label>
          <Input
            id="stream-cap"
            max={10}
            min={1}
            nativeInput
            onChange={(event) => onStreamCapChange(clamp(Number(event.currentTarget.value || 1), 1, 10))}
            type="number"
            value={streamCap}
          />
        </div>
        <Button onClick={onSave}>
          <SaveIcon />
          保存调度设置
        </Button>
      </CardPanel>
    </Card>
  );
}

function TokenExplorer(props: {
  activeStreamCap: number;
  counts: TokenCounts;
  importBatches: ImportBatch[];
  mode: TokenMode;
  onBatchActivation: (active: boolean) => void;
  onCancelImportJob: (id: number) => void;
  onDelete: (target: DeleteTarget) => void;
  onModeChange: (mode: TokenMode) => void;
  onPageChange: (page: number) => void;
  onProbe: (id: number) => void;
  onRemark: (target: RemarkTarget) => void;
  onSearchChange: (value: string) => void;
  onSelectedChange: (selected: Set<number>) => void;
  onSortChange: (value: string) => void;
  onStatusChange: (status: TokenStatus) => void;
  onToggleActivation: (id: number, active: boolean) => void;
  page: number;
  probeBusyIds: Set<number>;
  probeResults: Record<number, TokenProbeResponse>;
  search: string;
  selectedIds: Set<number>;
  sort: string;
  status: TokenStatus;
  tokenError: string;
  tokenLoading: boolean;
  tokenTotal: number;
  tokens: TokenItem[];
  totalPages: number;
  deleteTarget: DeleteTarget | null;
}) {
  const pageIds = props.tokens.map((item) => item.id);
  const allSelected = pageIds.length > 0 && pageIds.every((id) => props.selectedIds.has(id));
  const statusOptions = useMemo(() => statusOptionsWithCounts(props.counts), [props.counts]);
  const note =
    props.mode === "keys"
      ? `显示 ${formatNumber(props.tokens.length)} / ${formatNumber(props.tokenTotal)} 个 key`
      : `最近 ${formatNumber(props.importBatches.length)} 个导入批次`;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <KeyRoundIcon className="size-5" />
          Key 状态
        </CardTitle>
        <CardDescription>{note}</CardDescription>
        <CardAction>
          <Tabs value={props.mode} onValueChange={(value) => props.onModeChange(value as TokenMode)}>
            <TabsList>
              <TabsTab value="keys">Key Explorer</TabsTab>
              <TabsTab value="import_batches">导入批次</TabsTab>
            </TabsList>
          </Tabs>
        </CardAction>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <Tabs value={props.mode} onValueChange={(value) => props.onModeChange(value as TokenMode)}>
          <TabsPanel value="keys" className="grid gap-4">
            <div className="grid gap-3 lg:grid-cols-[minmax(220px,1fr)_180px_180px]">
              <div className="grid gap-2">
                <Label htmlFor="token-search">搜索 Key</Label>
                <div className="relative">
                  <SearchIcon className="-translate-y-1/2 pointer-events-none absolute left-3 top-1/2 size-4 text-muted-foreground" />
                  <Input
                    className="pl-9"
                    id="token-search"
                    nativeInput
                    onChange={(event) => props.onSearchChange(event.currentTarget.value)}
                    placeholder="邮箱、Token ID、来源或错误"
                    type="search"
                    value={props.search}
                  />
                </div>
              </div>
              <SelectField
                label="状态"
                onChange={(value) => props.onStatusChange(value as TokenStatus)}
                options={statusOptions}
                value={props.status}
              />
              <SelectField
                label="排序"
                onChange={props.onSortChange}
                options={SORT_OPTIONS}
                value={props.sort}
              />
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border bg-muted/40 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <Label className="rounded-lg border bg-background px-3 py-2">
                  <Checkbox
                    checked={allSelected}
                    onCheckedChange={(checked) => {
                      const next = new Set(props.selectedIds);
                      if (checked) {
                        pageIds.forEach((id) => next.add(id));
                      } else {
                        pageIds.forEach((id) => next.delete(id));
                      }
                      props.onSelectedChange(next);
                    }}
                  />
                  全选本页
                </Label>
                <Button disabled={!props.selectedIds.size} onClick={() => props.onBatchActivation(true)} variant="outline">
                  启用
                </Button>
                <Button disabled={!props.selectedIds.size} onClick={() => props.onBatchActivation(false)} variant="outline">
                  禁用
                </Button>
                <Button
                  disabled={!props.selectedIds.size}
                  onClick={() =>
                    props.onDelete({
                      description: "批量删除后会从调度池和最近列表移除。此操作不可撤销。",
                      ids: [...props.selectedIds],
                      title: `删除 ${props.selectedIds.size} 把 Key？`,
                    })
                  }
                  variant="destructive"
                >
                  <Trash2Icon />
                  删除
                </Button>
              </div>
              <Badge variant="secondary">{props.selectedIds.size ? `已选择 ${props.selectedIds.size} 个` : "未选择"}</Badge>
            </div>
            <TokenList {...props} />
            <Pagination page={props.page} totalPages={props.totalPages} onPageChange={props.onPageChange} total={props.tokenTotal} />
          </TabsPanel>
          <TabsPanel value="import_batches">
            <ImportBatchList batches={props.importBatches} loading={props.tokenLoading} onCancel={props.onCancelImportJob} />
          </TabsPanel>
        </Tabs>
      </CardPanel>
    </Card>
  );
}

function TokenList(props: Parameters<typeof TokenExplorer>[0]) {
  if (props.tokenError) {
    return <ErrorAlert title="Key 池载入失败" message={props.tokenError} />;
  }
  if (props.tokenLoading) {
    return <LoadingRows />;
  }
  if (!props.tokens.length) {
    return <EmptyState title="没有匹配的 key" description="调整搜索、状态或排序后再试。" />;
  }
  return (
    <div className="grid max-h-[760px] gap-2 overflow-auto pr-1 oaix-scrollbar">
      {props.tokens.map((item) => (
        <TokenCard key={item.id} item={item} {...props} />
      ))}
    </div>
  );
}

function TokenCard({
  activeStreamCap,
  item,
  onDelete,
  onProbe,
  onRemark,
  onSelectedChange,
  onToggleActivation,
  probeBusyIds,
  probeResults,
  selectedIds,
}: Parameters<typeof TokenExplorer>[0] & { item: TokenItem }) {
  const status = tokenStatusOf(item);
  const title = tokenTitle(item);
  const checked = selectedIds.has(item.id);
  const secondaryText = item.remark || item.source_file || "-";
  const probeResult = probeResults[item.id];
  const probeBusy = probeBusyIds.has(item.id);
  return (
    <div
      className="relative overflow-hidden rounded-xl border bg-card not-dark:bg-clip-padding text-card-foreground shadow-xs/5 before:pointer-events-none before:absolute before:inset-0 before:rounded-[calc(var(--radius-xl)-1px)] before:shadow-[0_1px_--theme(--color-black/4%)] dark:before:shadow-[0_-1px_--theme(--color-white/6%)]"
      data-token-card
      data-token-id={item.id}
    >
      <div className="relative grid gap-1.5 p-2.5">
        <div className="grid grid-cols-[auto_minmax(0,1fr)] items-start gap-2">
          <Checkbox
            checked={checked}
            className="mt-0.5"
            onCheckedChange={(value) => {
              const next = new Set(selectedIds);
              if (value) {
                next.add(item.id);
              } else {
                next.delete(item.id);
              }
              onSelectedChange(next);
            }}
          />
          <div className="min-w-0 space-y-1.5">
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-1.5">
              <div className="flex min-w-0 items-center gap-1.5">
                <strong className="min-w-0 truncate text-sm font-semibold leading-5" title={title}>
                  {title}
                </strong>
                <Badge size="sm" variant="outline">ID {item.id}</Badge>
                <Badge size="sm" variant={statusBadge(status)}>{tokenStatusLabel(status)}</Badge>
                <Badge className="max-w-[7rem] truncate" size="sm" title={item.plan_type || "unknown"} variant="secondary">
                  {item.plan_type || "unknown"}
                </Badge>
              </div>
              <div className="flex shrink-0 flex-wrap items-center gap-1">
                <Button loading={probeBusy} onClick={() => onProbe(item.id)} size="xs" variant="outline">
                  <ActivityIcon />
                  测试
                </Button>
                <Button onClick={() => onToggleActivation(item.id, !item.is_active)} size="xs" variant="outline">
                  {item.is_active ? "禁用" : "启用"}
                </Button>
                <Button
                  onClick={() =>
                    onRemark({
                      id: item.id,
                      meta: item.source_file || "-",
                      remark: item.remark || "",
                      title,
                    })
                  }
                  size="xs"
                  variant="ghost"
                >
                  备注
                </Button>
                <Button
                  onClick={() =>
                    onDelete({
                      description: "删除后会从调度池和最近列表移除。此操作不可撤销。",
                      ids: [item.id],
                      title: `删除 ${title}？`,
                    })
                  }
                  size="xs"
                  variant="destructive"
                >
                  <Trash2Icon />
                  删除
                </Button>
              </div>
            </div>
            <div className="grid gap-1.5 text-[11px] xl:grid-cols-[auto_minmax(0,1fr)] xl:items-center">
              <div className="flex min-w-0 flex-wrap items-center gap-1">
                <TokenQuotaStrip quota={item.quota} />
                <TokenConcurrency fallbackCap={activeStreamCap} item={item} />
              </div>
              <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-muted-foreground">
                <span className="oaix-tabular">最近 {formatDate(item.last_used_at)}</span>
                <span className="oaix-tabular">冷却 {formatDate(item.cooldown_until)}</span>
                <span className="min-w-0 truncate" title={secondaryText}>
                  {secondaryText}
                </span>
              </div>
            </div>
          </div>
        </div>
        {(probeResult || item.last_error) && (
          <div className="grid gap-1 border-t pt-2 text-[11px] sm:ms-7">
            {probeResult && <TokenProbeResult result={probeResult} />}
            {item.last_error && (
              <p className="min-w-0 truncate rounded-md bg-muted/64 px-2 py-1 font-mono text-muted-foreground" title={item.last_error}>
                {item.last_error}
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function TokenQuotaStrip({ quota }: { quota?: TokenItem["quota"] | null }) {
  if (!quota) {
    return <div className="rounded-md bg-muted/64 px-2 py-0.5 text-muted-foreground">额度更新中</div>;
  }
  if (quota.error) {
    return (
      <div className="max-w-48 truncate rounded-md bg-warning/8 px-2 py-0.5 text-warning-foreground" title={quota.error}>
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
    <div className="inline-flex min-w-[5.4rem] items-center gap-1 rounded-md border bg-muted/32 px-2 py-0.5" title={title}>
      <span className="font-medium">{label}</span>
      <span className="oaix-tabular text-muted-foreground">余 {formatPercent(remaining)}</span>
      <div className="h-1 w-8 overflow-hidden rounded-full bg-muted">
        <div className={cn("h-full rounded-full", tone)} style={{ width: `${usedWidth}%` }} />
      </div>
    </div>
  );
}

function TokenConcurrency({ fallbackCap, item }: { fallbackCap: number; item: TokenItem }) {
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

function TokenProbeResult({ result }: { result: TokenProbeResponse }) {
  const text = result.message || result.detail || "测试完成";
  return (
    <div className="flex min-w-0 items-center gap-2">
      <Badge size="sm" variant={probeBadgeVariant(result.outcome)}>
        {probeOutcomeLabel(result.outcome)}
        {result.status_code ? ` ${result.status_code}` : ""}
      </Badge>
      <span className="min-w-0 truncate text-muted-foreground" title={result.detail || text}>
        {text}
      </span>
    </div>
  );
}

function RequestPanel({
  error,
  loading,
  modelStats,
  requests,
  summary,
}: {
  error: string;
  loading: boolean;
  modelStats: Array<Record<string, unknown>>;
  requests: RequestItem[];
  summary: RequestSummary;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ListFilterIcon className="size-5" />
          请求观察
        </CardTitle>
        <CardDescription>最近 80 条请求、24 小时聚合和模型分布。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <div className="grid gap-3 sm:grid-cols-4">
          <MiniMetric label="总请求" value={summary.total} />
          <MiniMetric label="成功" value={summary.success} />
          <MiniMetric label="失败" value={summary.failure} />
          <MiniMetric label="平均首字" value={`${Math.round(summary.average_ttft_ms || 0)} ms`} />
        </div>
        {modelStats.length > 0 && (
          <div className="rounded-xl border bg-muted/40 p-4">
            <div className="mb-3 font-medium text-sm">模型分布</div>
            <div className="grid gap-2">
              {modelStats.slice(0, 8).map((item) => {
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
        ) : loading ? (
          <LoadingRows />
        ) : requests.length ? (
          <Table variant="card">
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
                  <TableCell>{item.model_name || item.model || "-"}</TableCell>
                  <TableCell>{item.endpoint || "-"}</TableCell>
                  <TableCell>
                    <Badge variant={item.success === false ? "error" : "success"}>{item.status_code || "-"}</Badge>
                  </TableCell>
                  <TableCell>{item.ttft_ms == null ? "-" : `${item.ttft_ms} ms`}</TableCell>
                  <TableCell className="max-w-80 truncate" title={item.error_message || ""}>
                    {item.error_message || "-"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <EmptyState title="暂无请求日志" description="有新的代理请求后会在这里出现。" />
        )}
      </CardPanel>
    </Card>
  );
}

function SettingsPanel({
  error,
  items,
  loading,
  onRefresh,
  onSave,
  settingKey,
  settingValue,
  setSettingKey,
  setSettingValue,
}: {
  error: string;
  items: SettingItem[];
  loading: boolean;
  onRefresh: () => void;
  onSave: () => void;
  settingKey: string;
  settingValue: string;
  setSettingKey: (value: string) => void;
  setSettingValue: (value: string) => void;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>运行设置</CardTitle>
        <CardDescription>查看和写入 JSON 设置项。</CardDescription>
        <CardAction>
          <Button onClick={onRefresh} size="sm" variant="outline">
            <RefreshCwIcon className={cn(loading && "animate-spin")} />
            刷新
          </Button>
        </CardAction>
      </CardHeader>
      <CardPanel className="grid gap-4">
        <div className="grid gap-2">
          <Label htmlFor="settings-key">Key</Label>
          <Input id="settings-key" nativeInput onChange={(event) => setSettingKey(event.currentTarget.value)} placeholder="例如 token_selection" value={settingKey} />
        </div>
        <div className="grid gap-2">
          <Label htmlFor="settings-value">Value JSON</Label>
          <Textarea id="settings-value" onChange={(event) => setSettingValue(event.currentTarget.value)} rows={4} spellCheck={false} value={settingValue} />
        </div>
        <Button onClick={onSave}>
          <SaveIcon />
          保存设置
        </Button>
        {error ? (
          <ErrorAlert title="设置载入失败" message={error} />
        ) : (
          <div className="grid gap-2">
            {items.map((item) => (
              <div className="rounded-xl border bg-muted/40 p-3" key={item.key}>
                <div className="font-medium text-sm">{item.key}</div>
                <pre className="mt-2 max-h-36 overflow-auto rounded-lg bg-background p-3 text-xs oaix-scrollbar">
                  {JSON.stringify(item.value ?? null, null, 2)}
                </pre>
                <div className="mt-2 text-muted-foreground text-xs">{item.updated_at || "-"}</div>
              </div>
            ))}
            {!items.length && !loading && <EmptyState title="暂无设置项" description="保存后会显示在这里。" compact />}
          </div>
        )}
      </CardPanel>
    </Card>
  );
}

function ImportBatchList({
  batches,
  loading,
  onCancel,
}: {
  batches: ImportBatch[];
  loading: boolean;
  onCancel: (id: number) => void;
}) {
  if (loading) {
    return <LoadingRows />;
  }
  if (!batches.length) {
    return <EmptyState title="暂无导入批次" description="导入任务提交后会在这里出现。" />;
  }
  return (
    <div className="grid gap-3">
      {batches.map((job) => (
        <Card key={job.id} className="block">
          <CardPanel className="grid flex-none gap-3 p-4 sm:grid-cols-[1fr_auto] sm:items-center">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <strong>导入批次 #{job.id}</strong>
                <Badge variant={job.status === "failed" ? "error" : job.status === "completed" ? "success" : "warning"}>
                  {job.status}
                </Badge>
              </div>
              <div className="mt-2 grid gap-2 text-muted-foreground text-xs sm:grid-cols-5">
                <span>总量 {formatNumber(job.total_count)}</span>
                <span>已处理 {formatNumber(job.processed_count)}</span>
                <span>新增 {formatNumber(job.created_count)}</span>
                <span>更新 {formatNumber(job.updated_count)}</span>
                <span>失败 {formatNumber(job.failed_count)}</span>
              </div>
            </div>
            <Button onClick={() => onCancel(job.id)} size="sm" variant="outline">
              取消
            </Button>
          </CardPanel>
        </Card>
      ))}
    </div>
  );
}

function DeleteDialog({
  onConfirm,
  onOpenChange,
  target,
}: {
  onConfirm: () => void;
  onOpenChange: (open: boolean) => void;
  target: DeleteTarget | null;
}) {
  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogPopup className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{target?.title || "删除 Key？"}</DialogTitle>
          <DialogDescription>{target?.description}</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose render={<Button variant="ghost" />}>取消</DialogClose>
          <Button onClick={onConfirm} variant="destructive">
            <Trash2Icon />
            删除 Key
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

function RemarkDialog({
  onChange,
  onOpenChange,
  onSave,
  target,
}: {
  onChange: (value: string) => void;
  onOpenChange: (open: boolean) => void;
  onSave: () => void;
  target: RemarkTarget | null;
}) {
  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogPopup className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>编辑备注</DialogTitle>
          <DialogDescription>
            {target?.title || "-"} · {target?.meta || "-"}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <Textarea onChange={(event) => onChange(event.currentTarget.value)} rows={6} value={target?.remark || ""} />
        </DialogPanel>
        <DialogFooter>
          <DialogClose render={<Button variant="ghost" />}>取消</DialogClose>
          <Button onClick={onSave}>保存备注</Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

function SelectField({
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
    <div className="grid gap-2">
      <Label>{label}</Label>
      <Select aria-label={label} items={options} onValueChange={(next) => onChange(String(next))} value={value}>
        <SelectTrigger>
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

function Pagination({
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
    <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border bg-muted/40 p-3">
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

function MiniMetric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-xl border bg-muted/40 p-3">
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className="mt-1 oaix-tabular font-medium">{typeof value === "number" ? formatNumber(value) : value || "-"}</div>
    </div>
  );
}

function ThemeButton({
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
  return (
    <Button onClick={() => onSelect(value)} size="sm" variant={current === value ? "secondary" : "ghost"}>
      {icon}
      {label}
    </Button>
  );
}

function EmptyState({ compact = false, description, title }: { compact?: boolean; description: string; title: string }) {
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

function ErrorAlert({ message, title }: { message: string; title: string }) {
  return (
    <Alert variant="error">
      <AlertCircleIcon />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

function LoadingRows() {
  return (
    <div className="grid gap-3">
      {[0, 1, 2].map((item) => (
        <Card key={item} className="block">
          <CardPanel className="grid flex-none gap-3 p-4">
            <Skeleton className="h-5 w-56" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-3/4" />
          </CardPanel>
        </Card>
      ))}
    </div>
  );
}

function ToastStack({ items }: { items: ToastMessage[] }) {
  return (
    <div className="fixed bottom-4 right-4 z-50 grid w-[min(420px,calc(100vw-2rem))] gap-2">
      {items.map((item) => (
        <Alert key={item.id} variant={item.variant === "success" ? "success" : item.variant === "warning" ? "warning" : item.variant === "error" ? "error" : "info"}>
          {item.variant === "success" ? <CheckCircle2Icon /> : <AlertCircleIcon />}
          <AlertTitle>{item.title}</AlertTitle>
        </Alert>
      ))}
    </div>
  );
}

function tokenTitle(item: TokenItem): string {
  return item.email || item.account_id || `Token #${item.id}`;
}

function tokenStatusOf(item: TokenItem): "active" | "cooling" | "disabled" {
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

function tokenStatusLabel(status: "active" | "cooling" | "disabled"): string {
  if (status === "active") {
    return "有效";
  }
  if (status === "cooling") {
    return "冷却";
  }
  return "禁用";
}

function statusBadge(status: "active" | "cooling" | "disabled"): "success" | "warning" | "error" {
  if (status === "active") {
    return "success";
  }
  if (status === "cooling") {
    return "warning";
  }
  return "error";
}

function quotaWindowFor(quota: NonNullable<TokenItem["quota"]>, label: "5h" | "7d"): TokenQuotaWindow | undefined {
  return quota.windows?.find((item) => item.label === label || item.id.endsWith(label));
}

function formatPercent(value: unknown): string {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "-";
  }
  return `${Math.round(clamp(number, 0, 100))}%`;
}

function probeOutcomeLabel(outcome?: string): string {
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

function probeBadgeVariant(outcome?: string): React.ComponentProps<typeof Badge>["variant"] {
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

function probeToastVariant(outcome?: string): ToastMessage["variant"] {
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

function statusOptionsWithCounts(counts: TokenCounts): Array<{ label: string; value: TokenStatus }> {
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

function sortParam(value: string): string {
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

function readThemePreference(): ThemePreference {
  const raw = document.documentElement.dataset.themePreference || localStorage.getItem("oaix.themePreference");
  return raw === "light" || raw === "dark" || raw === "auto" ? raw : "auto";
}

function applyTheme(preference: ThemePreference): void {
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

async function collectImportEntries(raw: string, files?: FileList | null): Promise<string[]> {
  const values = [...parseTokenText(raw)];
  for (const file of Array.from(files || [])) {
    values.push(...parseTokenText(await file.text()));
  }
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function parseTokenText(raw: string): string[] {
  const text = raw.trim();
  if (!text) {
    return [];
  }
  const values: string[] = [];
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
    if (value.includes(",") && !value.includes(".")) {
      const parts = value.split(",").map((part) => part.trim()).filter(Boolean);
      if (parts.length) {
        values.push(parts.at(-1) || "");
      }
      continue;
    }
    values.push(value);
  }
  return values;
}

function collectFromJSON(value: unknown, output: string[]): void {
  if (!value) {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectFromJSON(item, output));
    return;
  }
  if (typeof value === "string") {
    output.push(value);
    return;
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["access_token", "accessToken", "refresh_token", "refreshToken", "token"]) {
      if (typeof record[key] === "string") {
        output.push(record[key]);
      }
    }
    for (const nested of Object.values(record)) {
      if (Array.isArray(nested)) {
        collectFromJSON(nested, output);
      }
    }
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error || "请求失败");
}
