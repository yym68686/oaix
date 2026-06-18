import {
  ActivityIcon,
  ChevronLeftIcon,
  KeyRoundIcon,
  LoaderCircleIcon,
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
  type TokenCounts,
  type TokenItem,
  type TokenPlanCount,
  type TokenProbeResponse,
} from "@/lib/api";
import { clamp, formatDate, formatNumber } from "@/lib/format";
import {
  PAGE_SIZE,
  SORT_OPTIONS,
  errorMessage,
  planOptionsWithCounts,
  probeToastVariant,
  sortParam,
  statusOptionsWithCounts,
  tokenPlanType,
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
import type { DeleteTarget, RemarkTarget, ToastMessage, TokenStatus } from "@/shared/types";
import { navigateTo, type RouteState, useRouteSearch } from "@/app/router";

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
    return <KeyDetailPage activeStreamCap={activeStreamCap} id={Number(route.params.id)} pushToast={pushToast} refreshNonce={refreshNonce} />;
  }
  return (
    <KeyListPage
      activeStreamCap={activeStreamCap}
      onCountsChange={onCountsChange}
      pushToast={pushToast}
      refreshNonce={refreshNonce}
      route={route}
    />
  );
}

function KeyListPage({
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
  const searchParams = useRouteSearch(route);
  const status = readStatus(searchParams.get("status"));
  const plan = searchParams.get("plan") || "all";
  const sort = searchParams.get("sort") || "-created_at";
  const search = searchParams.get("q") || "";
  const page = Math.max(1, Number(searchParams.get("page") || 1));
  const [tokens, setTokens] = useState<TokenItem[]>([]);
  const [tokenTotal, setTokenTotal] = useState(0);
  const [counts, setCounts] = useState<TokenCounts>({});
  const [planCounts, setPlanCounts] = useState<TokenPlanCount[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selectedIds, setSelectedIds] = useState<Set<number>>(() => new Set());
  const [probeBusyIds, setProbeBusyIds] = useState<Set<number>>(() => new Set());
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [remarkTarget, setRemarkTarget] = useState<RemarkTarget | null>(null);
  const requestSeq = useRef(0);

  const totalPages = Math.max(1, Math.ceil(tokenTotal / PAGE_SIZE));
  const statusOptions = useMemo(() => statusOptionsWithCounts(counts), [counts]);
  const planOptions = useMemo(() => planOptionsWithCounts(planCounts), [planCounts]);
  const queryKey = useMemo(() => JSON.stringify({ page, plan, search, sort, status }), [page, plan, search, sort, status]);
  const [loadedQueryKey, setLoadedQueryKey] = useState("");
  const tableLoading = loading && (!tokens.length || loadedQueryKey !== queryKey);
  const pageIds = tokens.map((item) => item.id);
  const allSelected = pageIds.length > 0 && pageIds.every((id) => selectedIds.has(id));

  const beginQueryLoading = useCallback(() => {
    requestSeq.current += 1;
    setLoading(true);
    setError("");
    setLoadedQueryKey("");
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
      const nextPath = `/keys${query ? `?${query}` : ""}`;
      if (nextPath !== `${route.path}${route.search}`) {
        beginQueryLoading();
      }
      navigateTo(nextPath, { replace });
    },
    [beginQueryLoading, route.path, route.search],
  );

  const loadTokens = useCallback(async () => {
    const requestID = requestSeq.current + 1;
    requestSeq.current = requestID;
    setLoading(true);
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
      const mappedSort = sortParam(sort);
      if (mappedSort) {
        params.set("sort", mappedSort);
      }
      const payload = await api.listTokens(params);
      if (requestID !== requestSeq.current) {
        return;
      }
      const items = payload.items || [];
      const nextCounts = payload.counts || {};
      setTokens(items);
      setTokenTotal(payload.pagination?.total ?? items.length);
      setCounts(nextCounts);
      setPlanCounts(payload.plan_counts || []);
      setSelectedIds(new Set());
      setLoadedQueryKey(queryKey);
      onCountsChange(nextCounts);
    } catch (caught) {
      if (requestID !== requestSeq.current) {
        return;
      }
      setError(errorMessage(caught));
      setTokens([]);
    } finally {
      if (requestID === requestSeq.current) {
        setLoading(false);
      }
    }
  }, [onCountsChange, page, plan, queryKey, search, sort, status]);

  useEffect(() => {
    if (loadedQueryKey && loadedQueryKey !== queryKey) {
      beginQueryLoading();
    }
  }, [beginQueryLoading, loadedQueryKey, queryKey]);

  useEffect(() => {
    const timer = window.setTimeout(() => void loadTokens(), search.trim() ? 260 : 0);
    return () => window.clearTimeout(timer);
  }, [loadTokens, refreshNonce, search]);

  async function updateActivation(id: number, active: boolean) {
    await api.updateActivation(id, { active, clear_cooldown: true });
    pushToast(active ? "Key 已启用" : "Key 已禁用");
    await loadTokens();
  }

  async function runTokenProbe(id: number) {
    setProbeBusyIds((current) => new Set(current).add(id));
    try {
      const result = await api.probeToken(id);
      pushToast(result.message || "测试完成", probeToastVariant(result.outcome));
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
    });
    pushToast(active ? "已批量启用" : "已批量禁用");
    await loadTokens();
  }

  async function confirmDelete() {
    if (!deleteTarget) {
      return;
    }
    try {
      if (deleteTarget.ids.length === 1) {
        await api.deleteToken(deleteTarget.ids[0]);
      } else {
        await api.batchTokens({ action: "delete", token_ids: deleteTarget.ids });
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
    await api.updateRemark(remarkTarget.id, remarkTarget.remark);
    pushToast("备注已保存");
    setRemarkTarget(null);
    await loadTokens();
  }

  return (
    <div className="grid min-w-0 gap-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <KeyRoundIcon className="size-5" />
            Key 状态
          </CardTitle>
          <CardDescription>
            显示 {formatNumber(tokens.length)} / {formatNumber(tokenTotal)} 个 key
          </CardDescription>
          <CardAction>
            <Button onClick={() => navigateTo("/imports/new")} size="sm" variant="outline">
              导入 Key
            </Button>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid gap-4">
          <div className="grid gap-3 xl:grid-cols-[minmax(260px,1fr)_190px_190px_190px]">
            <div className="grid min-w-0 gap-2">
              <Label htmlFor="token-search">搜索 Key</Label>
              <div className="relative min-w-0">
                <SearchIcon className="-translate-y-1/2 pointer-events-none absolute left-3 top-1/2 size-4 text-muted-foreground" />
                <Input
                  className="min-w-0 pl-9"
                  id="token-search"
                  nativeInput
                  onChange={(event) => updateQuery({ page: 1, q: event.currentTarget.value })}
                  placeholder="邮箱、Token ID、来源或错误"
                  type="search"
                  value={search}
                />
              </div>
            </div>
            <SelectField label="状态" onChange={(value) => updateQuery({ page: 1, status: value })} options={statusOptions} value={status} />
            <SelectField label="计划" onChange={(value) => updateQuery({ page: 1, plan: value })} options={planOptions} value={plan} />
            <SelectField label="排序" onChange={(value) => updateQuery({ page: 1, sort: value })} options={SORT_OPTIONS} value={sort} />
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/40 p-3">
            <div className="flex flex-wrap items-center gap-2">
              <Label className="rounded-lg border bg-background px-3 py-2">
                <Checkbox
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
              <Button disabled={!selectedIds.size} onClick={() => void runBatchActivation(true)} variant="outline">
                启用
              </Button>
              <Button disabled={!selectedIds.size} onClick={() => void runBatchActivation(false)} variant="outline">
                禁用
              </Button>
              <Button
                disabled={!selectedIds.size}
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
      <RemarkDialog
        target={remarkTarget}
        onChange={(remark) => setRemarkTarget((target) => (target ? { ...target, remark } : target))}
        onOpenChange={(open) => !open && setRemarkTarget(null)}
        onSave={() => void saveRemark()}
      />
    </div>
  );
}

function TokenTable({
  activeStreamCap,
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
    return <EmptyState title="没有匹配的 key" description="调整搜索、状态、计划或排序后再试。" />;
  }
  return (
    <div className="min-w-0 overflow-hidden rounded-lg border">
      <Table className="table-fixed">
        <colgroup>
          <col className="w-9" />
          <col className="w-[28%]" />
          <col className="w-[5.75rem]" />
          <col className="w-[8.75rem]" />
          <col className="w-[9.5rem]" />
          <col className="w-[9.75rem]" />
          <col />
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
          <button className="min-w-0 text-left font-medium hover:underline" onClick={() => navigateTo(`/keys/${item.id}`)} type="button">
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
          <TokenQuotaStrip quota={item.quota} />
        </div>
      </TableCell>
      <TableCell className="px-1.5">
        <div className="flex max-h-11 min-w-0 flex-wrap items-center gap-1 overflow-hidden text-[11px]">
          <TokenConcurrency fallbackCap={activeStreamCap} item={item} />
          <TokenObservedCost value={item.observed_cost_usd} />
        </div>
      </TableCell>
      <TableCell className="px-1.5">
        <div className="grid max-h-10 min-w-0 gap-1 overflow-hidden text-xs">
          <span className="oaix-tabular text-muted-foreground">最近 {formatDate(item.last_used_at)}</span>
          <span className="oaix-tabular text-muted-foreground">冷却 {formatDate(item.cooldown_until)}</span>
        </div>
      </TableCell>
      <TableCell className="min-w-0 max-w-[18rem] align-top">
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

function KeyDetailPage({
  activeStreamCap,
  id,
  pushToast,
  refreshNonce,
}: {
  activeStreamCap: number;
  id: number;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [token, setToken] = useState<TokenItem | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [probeBusy, setProbeBusy] = useState(false);
  const [probeResult, setProbeResult] = useState<TokenProbeResponse | null>(null);
  const [remarkTarget, setRemarkTarget] = useState<RemarkTarget | null>(null);

  const loadToken = useCallback(async () => {
    if (!Number.isFinite(id) || id <= 0) {
      setError("无效的 Token ID");
      return;
    }
    setLoading(true);
    setError("");
    try {
      setToken(await api.getToken(id, true));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void loadToken();
  }, [loadToken, refreshNonce]);

  async function runProbe() {
    if (!token) {
      return;
    }
    setProbeBusy(true);
    try {
      const result = await api.probeToken(token.id);
      setProbeResult(result);
      pushToast(result.message || "测试完成", probeToastVariant(result.outcome));
      await loadToken();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setProbeBusy(false);
    }
  }

  async function saveRemark() {
    if (!remarkTarget) {
      return;
    }
    await api.updateRemark(remarkTarget.id, remarkTarget.remark);
    pushToast("备注已保存");
    setRemarkTarget(null);
    await loadToken();
  }

  if (error) {
    return (
      <div className="grid gap-4">
        <Button className="w-fit" onClick={() => navigateTo("/keys?status=available")} variant="outline">
          <ChevronLeftIcon />
          返回 Key
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
      <Button className="w-fit" onClick={() => navigateTo("/keys?status=available")} variant="outline">
        <ChevronLeftIcon />
        返回 Key
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
                <TokenQuotaStrip quota={token.quota} />
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
                <TokenObservedCost value={token.observed_cost_usd} />
              </div>
            </div>
          </div>
          <div className="grid gap-2 rounded-lg border bg-muted/24 p-3 text-sm">
            <div className="grid gap-2 md:grid-cols-2">
              <DetailItem label="最近使用" value={formatDate(token.last_used_at)} />
              <DetailItem label="冷却至" value={formatDate(token.cooldown_until)} />
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
