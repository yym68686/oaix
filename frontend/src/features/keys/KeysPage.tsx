import {
  ActivityIcon,
  ChevronLeftIcon,
  CopyIcon,
  KeyRoundIcon,
  LoaderCircleIcon,
  RefreshCwIcon,
  RotateCcwIcon,
  SearchIcon,
  Trash2Icon,
} from "lucide-react";
import type * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
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
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { Textarea } from "@/registry/default/ui/textarea";
import { cn } from "@/registry/default/lib/utils";
import {
  api,
  getAuthContext,
  type TokenAPIScope,
  type TokenCounts,
  type TokenItem,
  type TokenPlanCount,
  type TokenProbeResponse,
} from "@/lib/api";
import { ImportNewDialog } from "@/features/imports/ImportsPage";
import { clamp, formatDate, formatNumber } from "@/lib/format";
import {
  ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY,
  DEFAULT_TEST_MODEL,
  USER_TOKEN_PROBE_MODEL_SETTING_KEY,
  testModelFromSettings,
  type TestModel,
} from "@/lib/test-models";
import {
  PAGE_SIZE,
  SORT_OPTIONS,
  errorMessage,
  planOptionsWithCounts,
  probeResultNeedsRawInspection,
  probeToastVariant,
  sortParam,
  statusOptionsWithCounts,
  tokenPlanType,
  tokenResetAtLabel,
  tokenStatusLabel,
  tokenStatusOf,
  tokenTitle,
} from "@/shared/domain";
import {
  EmptyState,
  ErrorAlert,
  LoadingRows,
  Pagination,
  SelectField,
  TokenConcurrency,
  TokenObservedCost,
  TokenProbeResult,
  TokenQuotaStrip,
  TokenStatusPlan,
  statusBadge,
} from "@/shared/components";
import { TokenFilters } from "@/shared/resourceTables";
import type { DeleteTarget, RemarkTarget, ToastMessage, TokenStatus } from "@/shared/types";
import { navigateTo, type RouteState, useRouteSearch } from "@/app/router";

type OwnerFilterOption = {
  label: string;
  value: string;
};

type KeyListPageConfig = {
  apiScope?: TokenAPIScope;
  basePath?: string;
  description?: string;
  detailBasePath?: string;
  emptyDescription?: string;
  importHref?: string | null;
  ownerFilterOptions?: OwnerFilterOption[];
  searchId?: string;
  title?: string;
};

type ProbeResultTarget = {
  result: TokenProbeResponse;
  title: string;
};

function probeResultMessage(result: TokenProbeResponse): string {
  const message = result.message || "测试完成";
  const detail = String(result.detail || "").trim();
  const rawResponse = String(result.raw_response || "").trim();
  if (!rawResponse) {
    if (detail && detail !== message) {
      return `${message} 原因：${detail}`;
    }
    return message;
  }
  const preview = rawResponse.length > 220 ? `${rawResponse.slice(0, 220)}...` : rawResponse;
  return `${message} 上游响应：${preview}`;
}

function useTokenProbeModel(apiScope: TokenAPIScope): TestModel {
  const [model, setModel] = useState<TestModel>(DEFAULT_TEST_MODEL);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        if (apiScope === "admin" || (apiScope === "auto" && !getAuthContext()?.user?.id)) {
          const payload = await api.settings();
          if (active) {
            setModel(testModelFromSettings(payload.items || [], ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY));
          }
          return;
        }
        const payload = await api.mySettings();
        if (active) {
          setModel(testModelFromSettings(payload.items || [], USER_TOKEN_PROBE_MODEL_SETTING_KEY));
        }
      } catch {
        if (active) {
          setModel(DEFAULT_TEST_MODEL);
        }
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, [apiScope]);

  return model;
}

export function KeysPage({
  activeStreamCap,
  onCountsChange,
  pushToast,
  refreshNonce,
  route,
}: {
  activeStreamCap: number;
  onCountsChange: (counts: TokenCounts) => void;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  route: RouteState;
}) {
  if (route.key === "key_detail") {
    return (
      <KeyDetailPage
        activeStreamCap={activeStreamCap}
        apiScope="self"
        backHref={`/keys${route.search || ""}`}
        id={Number(route.params.id)}
        pushToast={pushToast}
        refreshNonce={refreshNonce}
      />
    );
  }
  return (
    <KeyListPage
      activeStreamCap={activeStreamCap}
      config={{ apiScope: "self" }}
      onCountsChange={onCountsChange}
      pushToast={pushToast}
      refreshNonce={refreshNonce}
      route={route}
    />
  );
}

export function KeyListPage({
  activeStreamCap,
  config = {},
  onCountsChange,
  pushToast,
  refreshNonce,
  route,
}: {
  activeStreamCap: number;
  config?: KeyListPageConfig;
  onCountsChange?: (counts: TokenCounts) => void;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  route: RouteState;
}) {
  const searchParams = useRouteSearch(route);
  const status = readStatus(searchParams.get("status"));
  const plan = searchParams.get("plan") || "all";
  const sort = searchParams.get("sort") || "-created_at";
  const search = searchParams.get("q") || "";
  const ownerUserID = config.ownerFilterOptions ? searchParams.get("owner_user_id") || "all" : "all";
  const page = Math.max(1, Number(searchParams.get("page") || 1));
  const [tokens, setTokens] = useState<TokenItem[]>([]);
  const [tokenTotal, setTokenTotal] = useState(0);
  const [counts, setCounts] = useState<TokenCounts>({});
  const [planCounts, setPlanCounts] = useState<TokenPlanCount[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [selectedIds, setSelectedIds] = useState<Set<number>>(() => new Set());
  const [probeBusyIds, setProbeBusyIds] = useState<Set<number>>(() => new Set());
  const [probeResultTarget, setProbeResultTarget] = useState<ProbeResultTarget | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [remarkTarget, setRemarkTarget] = useState<RemarkTarget | null>(null);
  const [importDialogOpen, setImportDialogOpen] = useState(false);
  const [quotaRefreshPending, setQuotaRefreshPending] = useState(false);
  const requestSeq = useRef(0);
  const quotaRefreshPolls = useRef(0);

  const totalPages = Math.max(1, Math.ceil(tokenTotal / PAGE_SIZE));
  const statusOptions = useMemo(() => statusOptionsWithCounts(counts), [counts]);
  const planOptions = useMemo(() => planOptionsWithCounts(planCounts), [planCounts]);
  const queryKey = useMemo(() => JSON.stringify({ ownerUserID, page, plan, search, sort, status }), [ownerUserID, page, plan, search, sort, status]);
  const [loadedQueryKey, setLoadedQueryKey] = useState("");
  const tableLoading = loading && (!tokens.length || loadedQueryKey !== queryKey);
  const pageIds = tokens.map((item) => item.id);
  const allSelected = pageIds.length > 0 && pageIds.every((id) => selectedIds.has(id));
  const canMutate = String(getAuthContext()?.role || getAuthContext()?.user?.role || "").toLowerCase() !== "readonly_admin";
  const apiScope = config.apiScope || "auto";
  const probeModel = useTokenProbeModel(apiScope);
  const basePath = config.basePath || "/keys";
  const importMode = config.importHref === undefined ? "dialog" : config.importHref ? "route" : "hidden";
  const searchId = config.searchId || "token-search";
  const title = config.title || "Key 状态";
  const description = config.description || `显示 ${formatNumber(tokens.length)} / ${formatNumber(tokenTotal)} 个 key`;
  const detailBasePath = config.detailBasePath || "/keys";

  const beginQueryLoading = useCallback(() => {
    requestSeq.current += 1;
    quotaRefreshPolls.current = 0;
    setLoading(true);
    setError("");
    setLoadedQueryKey("");
    setQuotaRefreshPending(false);
    setSelectedIds(new Set());
  }, []);

  const updateQuery = useCallback(
    (patch: Record<string, string | number | null>, replace = true) => {
      const params = new URLSearchParams(route.search);
      for (const [key, value] of Object.entries(patch)) {
        const stringValue = value == null ? "" : String(value);
        if (!stringValue || (key === "status" && stringValue === "available") || (key === "sort" && stringValue === "-created_at")) {
          params.delete(key);
        } else if (key !== "status" && stringValue === "all") {
          params.delete(key);
        } else {
          params.set(key, stringValue);
        }
      }
      const query = params.toString();
      const nextPath = `${basePath}${query ? `?${query}` : ""}`;
      if (nextPath !== `${route.path}${route.search}`) {
        beginQueryLoading();
      }
      navigateTo(nextPath, { replace });
    },
    [basePath, beginQueryLoading, route.path, route.search],
  );

  const loadTokens = useCallback(async () => {
    const requestID = requestSeq.current + 1;
    requestSeq.current = requestID;
    setLoading(true);
    setQuotaRefreshPending(false);
    setError("");
    try {
      const params = new URLSearchParams({
        include_quota: "true",
        limit: String(PAGE_SIZE),
        offset: String((page - 1) * PAGE_SIZE),
      });
      if (search.trim()) {
        params.set("q", search.trim());
      }
      if (status !== "all") {
        params.set("status", status);
      }
      if (plan !== "all") {
        params.set("plan", plan);
      }
      if (ownerUserID !== "all") {
        params.set("owner_user_id", ownerUserID);
      }
      const mappedSort = sortParam(sort);
      if (mappedSort) {
        params.set("sort", mappedSort);
      }
      const payload = await api.listTokens(params, apiScope);
      if (requestID !== requestSeq.current) {
        return;
      }
      const items = payload.items || [];
      const nextCounts = payload.counts || {};
      setTokens(items);
      const hasPendingQuota = items.some((item) => item.quota_fetch_state === "pending");
      setQuotaRefreshPending(hasPendingQuota);
      if (!hasPendingQuota) {
        quotaRefreshPolls.current = 0;
      }
      setTokenTotal(payload.pagination?.total ?? items.length);
      setCounts(nextCounts);
      setPlanCounts(payload.plan_counts || []);
      setSelectedIds(new Set());
      setLoadedQueryKey(queryKey);
      onCountsChange?.(nextCounts);
    } catch (caught) {
      if (requestID !== requestSeq.current) {
        return;
      }
      setError(errorMessage(caught));
      setTokens([]);
      setQuotaRefreshPending(false);
    } finally {
      if (requestID === requestSeq.current) {
        setLoading(false);
      }
    }
  }, [apiScope, onCountsChange, ownerUserID, page, plan, queryKey, search, sort, status]);

  useEffect(() => {
    if (loadedQueryKey && loadedQueryKey !== queryKey) {
      beginQueryLoading();
    }
  }, [beginQueryLoading, loadedQueryKey, queryKey]);

  useEffect(() => {
    quotaRefreshPolls.current = 0;
  }, [queryKey, refreshNonce]);

  useEffect(() => {
    const timer = window.setTimeout(() => void loadTokens(), search.trim() ? 260 : 0);
    return () => window.clearTimeout(timer);
  }, [loadTokens, refreshNonce, search]);

  useEffect(() => {
    if (!quotaRefreshPending || quotaRefreshPolls.current >= 12) {
      return;
    }
    const timer = window.setTimeout(() => {
      quotaRefreshPolls.current += 1;
      void loadTokens();
    }, 10_000);
    return () => window.clearTimeout(timer);
  }, [loadTokens, quotaRefreshPending]);

  async function updateActivation(id: number, active: boolean) {
    await api.updateActivation(id, { active, clear_cooldown: true }, apiScope);
    pushToast(active ? "Key 已启用" : "Key 已禁用");
    await loadTokens();
  }

  async function runTokenProbe(id: number) {
    setProbeBusyIds((current) => new Set(current).add(id));
    try {
      const result = await api.probeToken(id, { model: probeModel }, apiScope);
      pushToast(probeResultMessage(result), probeToastVariant(result.outcome));
      if (probeResultNeedsRawInspection(result)) {
        const item = tokens.find((token) => token.id === id);
        setProbeResultTarget({ result, title: item ? tokenTitle(item) : `Token #${id}` });
      }
      await loadTokens();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setProbeBusyIds((current) => {
        const next = new Set(current);
        next.delete(id);
        return next;
      });
    }
  }

  async function runBatchActivation(active: boolean) {
    if (!selectedIds.size) {
      return;
    }
    await api.batchTokens({
      action: active ? "enable" : "disable",
      clear_cooldown: active,
      token_ids: [...selectedIds],
    }, apiScope);
    pushToast(active ? "已批量启用" : "已批量禁用");
    await loadTokens();
  }

  async function confirmDelete() {
    if (!deleteTarget) {
      return;
    }
    try {
      if (deleteTarget.ids.length === 1) {
        await api.deleteToken(deleteTarget.ids[0], apiScope);
      } else {
        await api.batchTokens({ action: "delete", token_ids: deleteTarget.ids }, apiScope);
      }
      pushToast("Key 已删除");
      setDeleteTarget(null);
      await loadTokens();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    }
  }

  async function saveRemark() {
    if (!remarkTarget) {
      return;
    }
    await api.updateRemark(remarkTarget.id, remarkTarget.remark, apiScope);
    pushToast("备注已保存");
    setRemarkTarget(null);
    await loadTokens();
  }

  async function handleImported() {
    setImportDialogOpen(false);
    await loadTokens();
  }

  function openImport() {
    if (config.importHref) {
      navigateTo(config.importHref);
      return;
    }
    setImportDialogOpen(true);
  }

  return (
    <div className="grid min-w-0 gap-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <KeyRoundIcon className="size-5" />
            {title}
          </CardTitle>
          <CardDescription>{description}</CardDescription>
          {importMode !== "hidden" && (
            <CardAction>
              <Button onClick={openImport} size="sm" variant="outline">
                导入 Key
              </Button>
            </CardAction>
          )}
        </CardHeader>
        <CardPanel className="grid gap-4">
          <div className={cn("grid gap-3", config.ownerFilterOptions ? "xl:grid-cols-[minmax(220px,1fr)_210px_190px_190px_190px]" : "xl:grid-cols-[minmax(260px,1fr)_190px_190px_190px]")}>
            <div className="grid min-w-0 gap-2">
              <Label htmlFor={searchId}>搜索 Key</Label>
              <div className="relative min-w-0">
                <SearchIcon className="-translate-y-1/2 pointer-events-none absolute left-3 top-1/2 size-4 text-muted-foreground" />
                <Input
                  className="min-w-0 pl-9"
                  id={searchId}
                  nativeInput
                  onChange={(event) => updateQuery({ page: 1, q: event.currentTarget.value })}
                  placeholder="邮箱、Token ID、来源或错误"
                  type="search"
                  value={search}
                />
              </div>
            </div>
            {config.ownerFilterOptions && (
              <SelectField
                label="账号"
                onChange={(value) => updateQuery({ owner_user_id: value, page: 1 })}
                options={config.ownerFilterOptions}
                value={ownerUserID}
              />
            )}
            <div className="xl:col-span-2">
              <TokenFilters
                onPlanChange={(value) => updateQuery({ page: 1, plan: value })}
                onStatusChange={(value) => updateQuery({ page: 1, status: value })}
                plan={plan}
                planOptions={planOptions}
                status={status}
                statusOptions={statusOptions}
              />
            </div>
            <SelectField label="排序" onChange={(value) => updateQuery({ page: 1, sort: value })} options={SORT_OPTIONS} value={sort} />
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/40 p-3">
            <div className="flex flex-wrap items-center gap-2">
              <Label className="rounded-lg border bg-background px-3 py-2">
                <Checkbox
                  disabled={!canMutate}
                  checked={allSelected}
                  onCheckedChange={(checked) => {
                    const next = new Set(selectedIds);
                    if (checked) {
                      pageIds.forEach((id) => next.add(id));
                    } else {
                      pageIds.forEach((id) => next.delete(id));
                    }
                    setSelectedIds(next);
                  }}
                />
                全选本页
              </Label>
              <Button disabled={!canMutate || !selectedIds.size} onClick={() => void runBatchActivation(true)} variant="outline">
                启用
              </Button>
              <Button disabled={!canMutate || !selectedIds.size} onClick={() => void runBatchActivation(false)} variant="outline">
                禁用
              </Button>
              <Button
                disabled={!canMutate || !selectedIds.size}
                onClick={() =>
                  setDeleteTarget({
                    description: "批量删除后会从调度池和最近列表移除。此操作不可撤销。",
                    ids: [...selectedIds],
                    title: `删除 ${selectedIds.size} 把 Key？`,
                  })
                }
                variant="destructive"
              >
                <Trash2Icon />
                删除
              </Button>
            </div>
            <Badge variant="secondary">{selectedIds.size ? `已选择 ${selectedIds.size} 个` : "未选择"}</Badge>
          </div>

          <TokenTable
            activeStreamCap={activeStreamCap}
            detailBasePath={detailBasePath}
            detailSearch={route.search}
            emptyDescription={config.emptyDescription}
            error={error}
            loading={tableLoading}
            onDelete={setDeleteTarget}
            onProbe={(id) => void runTokenProbe(id)}
            onRemark={setRemarkTarget}
            onSelectedChange={setSelectedIds}
            onToggleActivation={(id, active) => void updateActivation(id, active)}
            probeBusyIds={probeBusyIds}
            selectedIds={selectedIds}
            tokens={tokens}
          />
          <Pagination page={page} totalPages={totalPages} onPageChange={(next) => updateQuery({ page: clamp(next, 1, totalPages) }, false)} total={tokenTotal} />
        </CardPanel>
      </Card>
      <DeleteDialog target={deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)} onConfirm={() => void confirmDelete()} />
      <ProbeResultDialog target={probeResultTarget} onOpenChange={(open) => !open && setProbeResultTarget(null)} />
      <RemarkDialog
        target={remarkTarget}
        onChange={(remark) => setRemarkTarget((target) => (target ? { ...target, remark } : target))}
        onOpenChange={(open) => !open && setRemarkTarget(null)}
        onSave={() => void saveRemark()}
      />
      {importMode === "dialog" && (
        <ImportNewDialog
          onImported={handleImported}
          onOpenChange={setImportDialogOpen}
          open={importDialogOpen}
          pushToast={pushToast}
        />
      )}
    </div>
  );
}

function TokenTable({
  activeStreamCap,
  detailBasePath,
  detailSearch,
  emptyDescription,
  error,
  loading,
  onDelete,
  onProbe,
  onRemark,
  onSelectedChange,
  onToggleActivation,
  probeBusyIds,
  selectedIds,
  tokens,
}: {
  activeStreamCap: number;
  detailBasePath: string;
  detailSearch: string;
  emptyDescription?: string;
  error: string;
  loading: boolean;
  onDelete: (target: DeleteTarget) => void;
  onProbe: (id: number) => void;
  onRemark: (target: RemarkTarget) => void;
  onSelectedChange: (selected: Set<number>) => void;
  onToggleActivation: (id: number, active: boolean) => void;
  probeBusyIds: Set<number>;
  selectedIds: Set<number>;
  tokens: TokenItem[];
}) {
  if (error) {
    return <ErrorAlert title="Key 池载入失败" message={error} />;
  }
  if (loading) {
    return (
      <div className="grid min-h-[18rem] place-items-center rounded-lg border bg-muted/24">
        <div className="flex items-center gap-2 rounded-lg border bg-background px-4 py-3 text-muted-foreground text-sm shadow-xs">
          <LoaderCircleIcon className="size-4 animate-spin" />
          <span>正在加载 Key</span>
        </div>
      </div>
    );
  }
  if (!tokens.length) {
    return <EmptyState title="没有匹配的 key" description={emptyDescription || "调整搜索、状态、计划或排序后再试。"} />;
  }
  return (
    <div className="min-w-0 overflow-x-auto rounded-lg border oaix-scrollbar">
      <Table className="table-fixed" style={{ width: "max(100%, 86rem)" }}>
        <colgroup>
          <col className="w-9" />
          <col className="w-[26%]" />
          <col className="w-[5.75rem]" />
          <col className="w-[8rem]" />
          <col className="w-[9.5rem]" />
          <col className="w-[9.75rem]" />
          <col className="w-[17rem]" />
          <col className="w-[13rem]" />
        </colgroup>
        <TableHeader>
          <TableRow>
            <TableHead className="w-9" />
            <TableHead>Key</TableHead>
            <TableHead>状态</TableHead>
            <TableHead className="px-1.5">额度</TableHead>
            <TableHead className="px-1.5">并发 / 金额</TableHead>
            <TableHead className="px-1.5">最近</TableHead>
            <TableHead>备注</TableHead>
            <TableHead className="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tokens.map((item) => (
            <TokenRow
              activeStreamCap={activeStreamCap}
              detailBasePath={detailBasePath}
              detailSearch={detailSearch}
              item={item}
              key={item.id}
              onDelete={onDelete}
              onProbe={onProbe}
              onRemark={onRemark}
              onSelectedChange={onSelectedChange}
              onToggleActivation={onToggleActivation}
              probeBusy={probeBusyIds.has(item.id)}
              selectedIds={selectedIds}
            />
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function TokenRow({
  activeStreamCap,
  detailBasePath,
  detailSearch,
  item,
  onDelete,
  onProbe,
  onRemark,
  onSelectedChange,
  onToggleActivation,
  probeBusy,
  selectedIds,
}: {
  activeStreamCap: number;
  detailBasePath: string;
  detailSearch: string;
  item: TokenItem;
  onDelete: (target: DeleteTarget) => void;
  onProbe: (id: number) => void;
  onRemark: (target: RemarkTarget) => void;
  onSelectedChange: (selected: Set<number>) => void;
  onToggleActivation: (id: number, active: boolean) => void;
  probeBusy: boolean;
  selectedIds: Set<number>;
}) {
  const status = tokenStatusOf(item);
  const title = tokenTitle(item);
  const planType = tokenPlanType(item);
  const checked = selectedIds.has(item.id);
  const secondaryText = item.remark || item.source_file || "-";
  return (
    <TableRow data-state={checked ? "selected" : undefined}>
      <TableCell>
        <Checkbox
          checked={checked}
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
      </TableCell>
      <TableCell className="min-w-[18rem] max-w-[28rem]">
        <div className="grid max-h-11 min-w-0 gap-1 overflow-hidden">
          <button className="min-w-0 text-left font-medium hover:underline" onClick={() => navigateTo(`${detailBasePath}/${item.id}${detailSearch || ""}`)} type="button">
            <span className="block truncate" title={title}>
              {title}
            </span>
          </button>
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge size="sm" variant="outline">
              ID {item.id}
            </Badge>
            {item.account_id && item.email && (
              <span className="max-w-52 truncate text-muted-foreground text-xs" title={item.account_id}>
                {item.account_id}
              </span>
            )}
          </div>
        </div>
      </TableCell>
      <TableCell>
        <TokenStatusPlan plan={planType} status={status} />
      </TableCell>
      <TableCell className="px-1.5">
        <div className="flex max-h-11 min-w-0 flex-wrap items-center gap-1 overflow-hidden text-[11px]">
          <TokenQuotaStrip quota={item.quota} state={item.quota_fetch_state} />
        </div>
      </TableCell>
      <TableCell className="px-1.5">
        <div className="flex max-h-11 min-w-0 flex-wrap items-center gap-1 overflow-hidden text-[11px]">
          <TokenConcurrency fallbackCap={activeStreamCap} item={item} />
          <TokenObservedCost
            value={item.combined_observed_cost_usd ?? item.observed_cost_usd}
            local={item.local_observed_cost_usd ?? item.observed_cost_usd}
            remote={item.sub2api_observed_cost_usd}
            stale={item.sub2api_usage_stale}
            syncedAt={item.sub2api_usage_synced_at}
          />
        </div>
      </TableCell>
      <TableCell className="px-1.5">
        <div className="grid max-h-10 min-w-0 gap-1 overflow-hidden text-xs">
          <span className="oaix-tabular text-muted-foreground" title={formatDate(item.last_used_at)}>
            最近 {formatDate(item.last_used_at)}
          </span>
          <span className="oaix-tabular text-muted-foreground" title={tokenResetAtLabel(item)}>
            重置 {tokenResetAtLabel(item)}
          </span>
        </div>
      </TableCell>
      <TableCell className="min-w-[14rem] max-w-[24rem] align-top">
        <span className="block min-w-0 whitespace-pre-wrap break-words text-muted-foreground text-xs leading-relaxed [overflow-wrap:anywhere]" title={secondaryText}>
          {secondaryText}
        </span>
      </TableCell>
      <TableCell>
        <div className="flex justify-end gap-1">
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
      </TableCell>
    </TableRow>
  );
}

function quotaResetCreditCount(quota?: TokenItem["quota"] | null): number | null {
  const raw = quota?.rate_limit_reset_credits?.available_count;
  if (raw === null || raw === undefined) {
    return null;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return null;
  }
  return Math.max(0, Math.floor(value));
}

export function KeyDetailPage({
  activeStreamCap,
  apiScope = "auto",
  backHref = "/keys?status=available",
  backLabel = "返回 Key",
  id,
  pushToast,
  refreshNonce,
}: {
  activeStreamCap: number;
  apiScope?: TokenAPIScope;
  backHref?: string;
  backLabel?: string;
  id: number;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [token, setToken] = useState<TokenItem | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [probeBusy, setProbeBusy] = useState(false);
  const [copyRefreshBusy, setCopyRefreshBusy] = useState(false);
  const [quotaCreditBusy, setQuotaCreditBusy] = useState(false);
  const [quotaResetBusy, setQuotaResetBusy] = useState(false);
  const [quotaCreditCount, setQuotaCreditCount] = useState<number | null>(null);
  const [probeResult, setProbeResult] = useState<TokenProbeResponse | null>(null);
  const [remarkTarget, setRemarkTarget] = useState<RemarkTarget | null>(null);
  const probeModel = useTokenProbeModel(apiScope);

  const loadToken = useCallback(async () => {
    if (!Number.isFinite(id) || id <= 0) {
      setError("无效的 Token ID");
      setLoading(false);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const nextToken = await api.getToken(id, true, apiScope);
      setToken(nextToken);
      setQuotaCreditCount(quotaResetCreditCount(nextToken.quota));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [apiScope, id]);

  useEffect(() => {
    void loadToken();
  }, [loadToken, refreshNonce]);

  useEffect(() => {
    setQuotaCreditCount(null);
    setProbeResult(null);
  }, [id]);

  async function runProbe() {
    if (!token) {
      return;
    }
    setProbeBusy(true);
    try {
      const result = await api.probeToken(token.id, { model: probeModel }, apiScope);
      setProbeResult(result);
      pushToast(probeResultMessage(result), probeToastVariant(result.outcome));
      await loadToken();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setProbeBusy(false);
    }
  }

  async function copyRefreshToken() {
    if (!token) {
      return;
    }
    if (token.credential_mode === "agent_identity") {
      pushToast("Agent Identity 不使用 refresh_token", "warning");
      return;
    }
    setCopyRefreshBusy(true);
    try {
      const payload = await api.tokenRefreshToken(token.id, apiScope);
      const refreshToken = payload.refresh_token?.trim();
      if (!refreshToken) {
        throw new Error("refresh_token 为空");
      }
      if (!navigator.clipboard?.writeText) {
        throw new Error("当前浏览器不支持剪贴板写入");
      }
      await navigator.clipboard.writeText(refreshToken);
      pushToast("refresh_token 已复制");
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setCopyRefreshBusy(false);
    }
  }

  function applyQuotaCreditPayload(payload: Awaited<ReturnType<typeof api.tokenQuotaResetCredits>>, tokenID: number): number {
    const count = quotaResetCreditCount(payload.quota) ?? Math.max(0, Math.floor(Number(payload.rate_limit_reset_credits?.available_count ?? 0)));
    setQuotaCreditCount(Number.isFinite(count) ? count : 0);
    if (payload.quota) {
      setToken((current) => (current && current.id === tokenID ? { ...current, quota: payload.quota } : current));
    }
    return Number.isFinite(count) ? count : 0;
  }

  async function queryQuotaResetCredits() {
    if (!token) {
      return;
    }
    setQuotaCreditBusy(true);
    try {
      const payload = await api.tokenQuotaResetCredits(token.id, apiScope);
      const count = applyQuotaCreditPayload(payload, token.id);
      if (payload.quota?.error) {
        pushToast(payload.quota.error, "warning");
      } else {
        pushToast(`剩余重置次数 ${formatNumber(count)}`);
      }
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setQuotaCreditBusy(false);
    }
  }

  async function resetQuotaCredit() {
    if (!token) {
      return;
    }
    if (quotaCreditCount === null) {
      pushToast("请先查询次数", "warning");
      return;
    }
    if (quotaCreditCount <= 0) {
      pushToast("没有可用的重置次数", "warning");
      return;
    }
    setQuotaResetBusy(true);
    try {
      const result = await api.resetTokenQuota(token.id, apiScope);
      let refreshedCount = 0;
      try {
        const payload = await api.tokenQuotaResetCredits(token.id, apiScope);
        refreshedCount = applyQuotaCreditPayload(payload, token.id);
      } catch (caught) {
        setQuotaCreditCount(null);
        await loadToken();
        pushToast(`重置完成，次数刷新失败：${errorMessage(caught)}`, "warning");
        return;
      }
      await loadToken();
      pushToast(`已重置 ${formatNumber(result.windows_reset ?? 0)} 个窗口，剩余 ${formatNumber(refreshedCount)}`);
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setQuotaResetBusy(false);
    }
  }

  async function saveRemark() {
    if (!remarkTarget) {
      return;
    }
    await api.updateRemark(remarkTarget.id, remarkTarget.remark, apiScope);
    pushToast("备注已保存");
    setRemarkTarget(null);
    await loadToken();
  }

  if (error) {
    return (
      <div className="grid gap-4">
        <Button className="w-fit" onClick={() => navigateTo(backHref)} variant="outline">
          <ChevronLeftIcon />
          {backLabel}
        </Button>
        <ErrorAlert title="Key 详情载入失败" message={error} />
      </div>
    );
  }
  if (loading && !token) {
    return <LoadingRows />;
  }
  if (!token) {
    return <EmptyState title="没有 Key 详情" description="返回列表选择一个 key。" />;
  }

  const status = tokenStatusOf(token);
  const title = tokenTitle(token);
  const plan = tokenPlanType(token);
  return (
    <div className="grid gap-4">
      <Button className="w-fit" onClick={() => navigateTo(backHref)} variant="outline">
        <ChevronLeftIcon />
        {backLabel}
      </Button>
      <Card>
        <CardHeader>
          <CardTitle className="flex min-w-0 flex-wrap items-center gap-2">
            <span className="min-w-0 truncate" title={title}>
              {title}
            </span>
            <Badge variant="outline">ID {token.id}</Badge>
            <Badge variant={statusBadge(status)}>{tokenStatusLabel(status)}</Badge>
            <Badge variant="secondary">{plan}</Badge>
          </CardTitle>
          <CardDescription>{token.account_id || token.source_file || "-"}</CardDescription>
          <CardAction>
            <div className="flex flex-wrap gap-2">
              <Button loading={probeBusy} onClick={() => void runProbe()} size="sm" variant="outline">
                <ActivityIcon />
                测试
              </Button>
              {token.credential_mode !== "agent_identity" ? (
                <Button loading={copyRefreshBusy} onClick={() => void copyRefreshToken()} size="sm" variant="outline">
                  <CopyIcon />
                  复制 refresh_token
                </Button>
              ) : null}
              <Button
                disabled={quotaResetBusy}
                loading={quotaCreditBusy}
                onClick={() => void queryQuotaResetCredits()}
                size="sm"
                title={quotaCreditCount === null ? "查询重置次数" : "刷新重置次数"}
                variant="outline"
              >
                <RefreshCwIcon />
                {quotaCreditCount === null ? "次数" : `次数 ${formatNumber(quotaCreditCount)}`}
              </Button>
              <Button
                disabled={quotaCreditBusy || quotaCreditCount === null || quotaCreditCount <= 0}
                loading={quotaResetBusy}
                onClick={() => void resetQuotaCredit()}
                size="sm"
                title={quotaCreditCount === null ? "先查询次数" : quotaCreditCount <= 0 ? "没有可用的重置次数" : "消耗 1 次重置额度"}
                variant="outline"
              >
                <RotateCcwIcon />
                重置
              </Button>
              <Button
                onClick={() =>
                  setRemarkTarget({
                    id: token.id,
                    meta: token.source_file || "-",
                    remark: token.remark || "",
                    title,
                  })
                }
                size="sm"
                variant="outline"
              >
                备注
              </Button>
            </div>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid gap-4">
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-lg border bg-muted/32 p-3">
              <div className="text-muted-foreground text-xs">额度</div>
              <div className="mt-2 flex flex-wrap gap-1 text-[11px]">
                <TokenQuotaStrip quota={token.quota} state={token.quota_fetch_state} />
              </div>
            </div>
            <div className="rounded-lg border bg-muted/32 p-3">
              <div className="text-muted-foreground text-xs">并发</div>
              <div className="mt-2 text-[11px]">
                <TokenConcurrency fallbackCap={activeStreamCap} item={token} />
              </div>
            </div>
            <div className="rounded-lg border bg-muted/32 p-3">
              <div className="text-muted-foreground text-xs">已用金额</div>
              <div className="mt-2 text-[11px]">
                <TokenObservedCost
                  value={token.combined_observed_cost_usd ?? token.observed_cost_usd}
                  local={token.local_observed_cost_usd ?? token.observed_cost_usd}
                  remote={token.sub2api_observed_cost_usd}
                  stale={token.sub2api_usage_stale}
                  syncedAt={token.sub2api_usage_synced_at}
                />
              </div>
            </div>
          </div>
          <div className="grid gap-2 rounded-lg border bg-muted/24 p-3 text-sm">
            <div className="grid gap-2 md:grid-cols-2">
              <DetailItem label="最近使用" value={formatDate(token.last_used_at)} />
              <DetailItem label="重置时间" value={tokenResetAtLabel(token)} />
              <DetailItem label="创建" value={formatDate(token.created_at)} />
              <DetailItem label="更新" value={formatDate(token.updated_at)} />
            </div>
            <DetailItem label="备注" value={token.remark || "-"} />
            <DetailItem label="最后错误" value={token.last_error || "-"} code />
          </div>
          {probeResult && (
            <Alert variant="info">
              <ActivityIcon />
              <AlertTitle>测试结果</AlertTitle>
              <AlertDescription>
                <TokenProbeResult result={probeResult} />
              </AlertDescription>
            </Alert>
          )}
        </CardPanel>
      </Card>
      <RemarkDialog
        target={remarkTarget}
        onChange={(remark) => setRemarkTarget((target) => (target ? { ...target, remark } : target))}
        onOpenChange={(open) => !open && setRemarkTarget(null)}
        onSave={() => void saveRemark()}
      />
    </div>
  );
}

function DetailItem({ code = false, label, value }: { code?: boolean; label: string; value: string }) {
  return (
    <div className="min-w-0">
      <div className="text-muted-foreground text-xs">{label}</div>
      <div className={cn("mt-1 min-w-0 break-words", code && "rounded-md bg-background p-2 font-mono text-xs")}>{value}</div>
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

function ProbeResultDialog({
  onOpenChange,
  target,
}: {
  onOpenChange: (open: boolean) => void;
  target: ProbeResultTarget | null;
}) {
  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogPopup className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>测试结果</DialogTitle>
          <DialogDescription>{target?.title || "-"} · 请结合上游原始响应核对测试结论。</DialogDescription>
        </DialogHeader>
        <DialogPanel>{target && <TokenProbeResult result={target.result} />}</DialogPanel>
        <DialogFooter>
          <DialogClose render={<Button variant="outline" />}>关闭</DialogClose>
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

function readStatus(value: string | null): TokenStatus {
  if (value === "all" || value === "cooling" || value === "disabled") {
    return value;
  }
  return "available";
}
