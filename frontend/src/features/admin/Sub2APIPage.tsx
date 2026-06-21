import { CheckCircle2Icon, PlayIcon, PlusIcon, RefreshCwIcon, SendIcon, Trash2Icon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Checkbox } from "@/registry/default/ui/checkbox";
import { Dialog, DialogDescription, DialogHeader, DialogPanel, DialogPopup, DialogTitle } from "@/registry/default/ui/dialog";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { api, type PlatformUser, type Sub2APIGroup, type Sub2APIProxy, type Sub2APIRun, type Sub2APITarget, type TokenPlanCount } from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState, MiniMetric, SelectField } from "@/shared/components";
import { errorMessage, normalizePlanValue, planLabel } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

type Draft = {
  id?: number;
  name: string;
  base_url: string;
  admin_key: string;
  enabled: boolean;
  owner_user_id: number;
  plan_filters: string[];
  token_status_filters: string[];
  target_group_ids: number[];
  target_group_names: string[];
  check_interval_seconds: number;
  min_available: number;
  top_up_batch_size: number;
  account_concurrency: number;
  account_priority: number;
  proxy_id: number;
  auto_sync_new: boolean;
};

const DEFAULT_TOKEN_STATUS_FILTERS = ["available", "cooling"];

const TOKEN_STATUS_OPTIONS = [
  { label: "可用", value: "available" },
  { label: "冷却", value: "cooling" },
  { label: "禁用", value: "disabled" },
] as const;

const TOKEN_STATUS_LABELS: Record<string, string> = Object.fromEntries(TOKEN_STATUS_OPTIONS.map((item) => [item.value, item.label]));

const EMPTY_DRAFT: Draft = {
  name: "",
  base_url: "",
  admin_key: "",
  enabled: true,
  owner_user_id: 0,
  plan_filters: [],
  token_status_filters: [...DEFAULT_TOKEN_STATUS_FILTERS],
  target_group_ids: [],
  target_group_names: [],
  check_interval_seconds: 300,
  min_available: 10,
  top_up_batch_size: 10,
  account_concurrency: 10,
  account_priority: 50,
  proxy_id: 0,
  auto_sync_new: false,
};

export function AdminSub2APIPage({
  pushToast,
  refreshNonce,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [targets, setTargets] = useState<Sub2APITarget[]>([]);
  const [runs, setRuns] = useState<Sub2APIRun[]>([]);
  const [users, setUsers] = useState<PlatformUser[]>([]);
  const [groups, setGroups] = useState<Sub2APIGroup[]>([]);
  const [proxies, setProxies] = useState<Sub2APIProxy[]>([]);
  const [globalPlanCounts, setGlobalPlanCounts] = useState<TokenPlanCount[]>([]);
  const [planCountsByUser, setPlanCountsByUser] = useState<Record<number, TokenPlanCount[]>>({});
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT);
  const [targetDialogOpen, setTargetDialogOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [groupLoading, setGroupLoading] = useState(false);
  const [proxyLoading, setProxyLoading] = useState(false);
  const [busyID, setBusyID] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [groupError, setGroupError] = useState("");
  const [proxyError, setProxyError] = useState("");

  const userOptions = useMemo(() => {
    const options = users.map((user) => ({ label: `${user.email || `User #${user.id}`} · ${user.id}`, value: String(user.id) }));
    if (draft.owner_user_id && !options.some((item) => item.value === String(draft.owner_user_id))) {
      options.unshift({ label: `User #${draft.owner_user_id}`, value: String(draft.owner_user_id) });
    }
    return options.length ? options : [{ label: "暂无用户", value: "0" }];
  }, [draft.owner_user_id, users]);

  const selectedGroupNames = useMemo(() => {
    const byID = new Map(groups.map((group) => [group.id, group.name]));
    return draft.target_group_ids.map((id, index) => byID.get(id) || draft.target_group_names[index] || `Group #${id}`);
  }, [draft.target_group_ids, draft.target_group_names, groups]);

  const planOptions = useMemo(() => {
    const sourceCounts = draft.owner_user_id > 0 ? planCountsByUser[draft.owner_user_id] || [] : globalPlanCounts;
    return planOptionsFromPool(sourceCounts, draft.plan_filters);
  }, [draft.owner_user_id, draft.plan_filters, globalPlanCounts, planCountsByUser]);

  const proxyOptions = useMemo(() => {
    const options = [{ label: "不使用代理", value: "0" }];
    const seen = new Set<number>();
    for (const proxy of proxies) {
      if (!proxy.id || seen.has(proxy.id)) continue;
      seen.add(proxy.id);
      options.push({ label: proxyOptionLabel(proxy), value: String(proxy.id) });
    }
    if (draft.proxy_id > 0 && !seen.has(draft.proxy_id)) {
      options.push({ label: `Proxy #${draft.proxy_id}`, value: String(draft.proxy_id) });
    }
    return options;
  }, [draft.proxy_id, proxies]);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const userParams = new URLSearchParams({ limit: "200", status: "active" });
      const [targetPayload, runPayload, userPayload, poolPayload, poolByUserPayload] = await Promise.all([
        api.sub2APITargets(),
        api.sub2APIRuns(undefined, 80),
        api.adminUsers(userParams),
        api.adminPoolSummary(),
        api.adminPoolSummaryByUser(userParams),
      ]);
      setTargets(targetPayload.items || []);
      setRuns(runPayload.items || []);
      setUsers(userPayload.items || []);
      setGlobalPlanCounts(poolPayload.plan_counts || []);
      const nextPlanCountsByUser: Record<number, TokenPlanCount[]> = {};
      for (const item of poolByUserPayload.items || []) {
        if (item.user?.id) {
          nextPlanCountsByUser[item.user.id] = item.plan_counts || [];
        }
      }
      setPlanCountsByUser(nextPlanCountsByUser);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  function updateDraft(patch: Partial<Draft>) {
    setDraft((current) => ({ ...current, ...patch }));
  }

  function editTarget(target: Sub2APITarget) {
    setGroupError("");
    setProxyError("");
    setGroups([]);
    setProxies([]);
    setDraft({
      id: target.id,
      name: target.name || "",
      base_url: target.base_url || "",
      admin_key: "",
      enabled: Boolean(target.enabled),
      owner_user_id: Number(target.owner_user_id || 0),
      plan_filters: target.plan_filters || [],
      token_status_filters: target.token_status_filters?.length ? target.token_status_filters : [...DEFAULT_TOKEN_STATUS_FILTERS],
      target_group_ids: target.target_group_ids || [],
      target_group_names: target.target_group_names || [],
      check_interval_seconds: Number(target.check_interval_seconds || 300),
      min_available: Number(target.min_available ?? 10),
      top_up_batch_size: Number(target.top_up_batch_size || 10),
      account_concurrency: Number(target.account_concurrency || 10),
      account_priority: Number(target.account_priority ?? 50),
      proxy_id: Number(target.proxy_id || 0),
      auto_sync_new: Boolean(target.auto_sync_new),
    });
    setTargetDialogOpen(true);
    void fetchProxiesFor({ targetID: target.id });
  }

  function resetDraft() {
    setDraft({ ...EMPTY_DRAFT });
    setGroups([]);
    setProxies([]);
    setGroupError("");
    setProxyError("");
  }

  function openCreateDialog() {
    resetDraft();
    setTargetDialogOpen(true);
  }

  function changeTargetDialogOpen(open: boolean) {
    if (!open && saving) return;
    setTargetDialogOpen(open);
    if (!open) {
      resetDraft();
    }
  }

  function autoFetchProxiesFromFields(baseURL: string, adminKey: string) {
    if (!baseURL.trim() || !adminKey.trim()) return;
    void fetchProxiesFor({ baseURL, adminKey });
  }

  async function fetchGroups() {
    setGroupLoading(true);
    setGroupError("");
    try {
      const payload = draft.id && !draft.admin_key.trim()
        ? await api.sub2APITargetGroups(draft.id)
        : await api.probeSub2APIGroups({ base_url: draft.base_url, admin_key: draft.admin_key });
      const nextGroups = (payload.items || []).filter((group) => !group.platform || group.platform === "openai");
      setGroups(nextGroups);
      if (!nextGroups.length) {
        setGroupError("没有读取到 openai 分组。");
      }
    } catch (caught) {
      setGroupError(errorMessage(caught));
    } finally {
      setGroupLoading(false);
    }
  }

  async function fetchProxiesFor(params?: { targetID?: number; baseURL?: string; adminKey?: string }) {
    setProxyLoading(true);
    setProxyError("");
    try {
      const targetID = params?.targetID ?? draft.id;
      const adminKey = params?.adminKey ?? draft.admin_key;
      const baseURL = params?.baseURL ?? draft.base_url;
      const payload = targetID && !adminKey.trim()
        ? await api.sub2APITargetProxies(targetID)
        : await api.probeSub2APIProxies({ base_url: baseURL, admin_key: adminKey });
      const nextProxies = (payload.items || []).filter((proxy) => !proxy.status || proxy.status === "active");
      setProxies(nextProxies);
      if (!nextProxies.length) {
        setProxyError("没有读取到可用代理。");
      }
    } catch (caught) {
      setProxyError(errorMessage(caught));
    } finally {
      setProxyLoading(false);
    }
  }

  async function saveTarget() {
    setSaving(true);
    setError("");
    const editing = Boolean(draft.id);
    try {
      const payload: Record<string, unknown> = {
        name: draft.name.trim(),
        base_url: draft.base_url.trim(),
        enabled: draft.enabled,
        owner_user_id: draft.owner_user_id,
        plan_filters: draft.plan_filters,
        token_status_filters: draft.token_status_filters,
        target_group_ids: draft.target_group_ids,
        target_group_names: selectedGroupNames,
        check_interval_seconds: Number(draft.check_interval_seconds || 300),
        min_available: Number(draft.min_available || 0),
        top_up_batch_size: Number(draft.top_up_batch_size || 10),
        account_concurrency: Number(draft.account_concurrency || 10),
        account_priority: Number(draft.account_priority ?? 50),
        proxy_id: Number(draft.proxy_id || 0),
        auto_sync_new: draft.auto_sync_new,
      };
      if (draft.admin_key.trim()) {
        payload.admin_key = draft.admin_key.trim();
      }
      if (draft.id) {
        await api.updateSub2APITarget(draft.id, payload);
      } else {
        await api.createSub2APITarget(payload);
      }
      setTargetDialogOpen(false);
      resetDraft();
      pushToast(editing ? "Sub2API 配置已更新" : "Sub2API 配置已创建");
      await load();
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setSaving(false);
    }
  }

  async function deleteTarget(target: Sub2APITarget) {
    if (!window.confirm(`删除 ${target.name}？`)) return;
    setBusyID(target.id);
    try {
      await api.deleteSub2APITarget(target.id);
      if (draft.id === target.id) {
        setTargetDialogOpen(false);
        resetDraft();
      }
      pushToast("Sub2API 配置已删除");
      await load();
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusyID(null);
    }
  }

  async function runTarget(target: Sub2APITarget, mode: "check" | "sync") {
    setBusyID(target.id);
    try {
      const payload = mode === "check" ? await api.checkSub2APITarget(target.id) : await api.syncSub2APITarget(target.id);
      const run = payload.run;
      if (run?.status === "failed") {
        pushToast(run.error_message || "Sub2API 同步失败", "error");
      } else {
        pushToast(mode === "check" ? "Sub2API 检查完成" : "Sub2API 同步完成");
      }
      await load();
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusyID(null);
    }
  }

  function togglePlan(value: string) {
    const selected = new Set(draft.plan_filters);
    if (selected.has(value)) {
      selected.delete(value);
    } else {
      selected.add(value);
    }
    updateDraft({ plan_filters: Array.from(selected) });
  }

  function toggleTokenStatus(value: string) {
    const selected = new Set(draft.token_status_filters);
    if (selected.has(value)) {
      selected.delete(value);
    } else {
      selected.add(value);
    }
    updateDraft({ token_status_filters: Array.from(selected) });
  }

  function toggleGroup(group: Sub2APIGroup) {
    const selected = new Set(draft.target_group_ids);
    if (selected.has(group.id)) {
      selected.delete(group.id);
    } else {
      selected.add(group.id);
    }
    const ids = Array.from(selected);
    const nameByID = new Map(groups.map((item) => [item.id, item.name]));
    updateDraft({ target_group_ids: ids, target_group_names: ids.map((id) => nameByID.get(id) || `Group #${id}`) });
  }

  const canSave = Boolean(
    draft.name.trim() &&
      draft.base_url.trim() &&
      draft.owner_user_id > 0 &&
      draft.token_status_filters.length &&
      draft.target_group_ids.length &&
      (draft.id || draft.admin_key.trim()),
  );

  function renderTargetForm(resetLabel: string) {
    return (
      <div className="grid gap-4">
        <div className="grid gap-3">
          <Field label="名称">
            <Input nativeInput onChange={(event) => updateDraft({ name: event.currentTarget.value })} placeholder="s2a-prod" value={draft.name} />
          </Field>
          <Field label="Base URL">
            <Input
              nativeInput
              onBlur={(event) => autoFetchProxiesFromFields(event.currentTarget.value, draft.admin_key)}
              onChange={(event) => updateDraft({ base_url: event.currentTarget.value })}
              placeholder="https://s2a.example.com"
              value={draft.base_url}
            />
          </Field>
          <Field label={draft.id ? "Admin Key（留空保留）" : "Admin Key"}>
            <Input
              nativeInput
              onBlur={(event) => autoFetchProxiesFromFields(draft.base_url, event.currentTarget.value)}
              onChange={(event) => updateDraft({ admin_key: event.currentTarget.value })}
              placeholder="admin-..."
              type="password"
              value={draft.admin_key}
            />
          </Field>
          <SelectField label="OAIX 用户" onChange={(value) => updateDraft({ owner_user_id: Number(value) })} options={userOptions} value={String(draft.owner_user_id || "0")} />
        </div>

        <div className="grid gap-2 rounded-lg border bg-muted/32 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label>计划</Label>
            <Button onClick={() => updateDraft({ plan_filters: [] })} size="xs" variant={!draft.plan_filters.length ? "secondary" : "outline"}>
              全部计划
            </Button>
          </div>
          <div className="flex flex-wrap gap-2">
            {planOptions.map((plan) => (
              <Label className="rounded-lg border bg-background px-3 py-2" key={plan.value}>
                <Checkbox checked={draft.plan_filters.includes(plan.value)} onCheckedChange={() => togglePlan(plan.value)} />
                <span>{plan.label}</span>
                <span className="text-muted-foreground text-xs">{formatNumber(plan.count)}</span>
              </Label>
            ))}
            {!planOptions.length && <div className="text-muted-foreground text-sm">当前号池没有可选计划。</div>}
          </div>
        </div>

        <div className="grid gap-2 rounded-lg border bg-muted/32 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label>Key 状态</Label>
            <span className="text-muted-foreground text-xs">默认同步可用和冷却</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {TOKEN_STATUS_OPTIONS.map((status) => (
              <Label className="rounded-lg border bg-background px-3 py-2" key={status.value}>
                <Checkbox checked={draft.token_status_filters.includes(status.value)} onCheckedChange={() => toggleTokenStatus(status.value)} />
                <span>{status.label}</span>
              </Label>
            ))}
          </div>
          {!draft.token_status_filters.length && <div className="text-destructive-foreground text-xs">至少选择一个 Key 状态。</div>}
        </div>

        <div className="grid gap-2 rounded-lg border bg-muted/32 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label>Sub2API 分组</Label>
            <Button disabled={groupLoading || (!draft.id && (!draft.base_url.trim() || !draft.admin_key.trim()))} onClick={() => void fetchGroups()} size="xs" variant="outline">
              <RefreshCwIcon />
              获取分组
            </Button>
          </div>
          {groupError && (
            <Alert variant="error">
              <AlertTitle>分组读取失败</AlertTitle>
              <AlertDescription>{groupError}</AlertDescription>
            </Alert>
          )}
          {groups.length ? (
            <div className="grid max-h-60 gap-2 overflow-auto pr-1">
              {groups.map((group) => (
                <Label className="rounded-lg border bg-background px-3 py-2" key={group.id}>
                  <Checkbox checked={draft.target_group_ids.includes(group.id)} onCheckedChange={() => toggleGroup(group)} />
                  <span className="min-w-0 truncate">{group.name}</span>
                  <Badge variant={group.status === "active" ? "success" : "secondary"}>{group.id}</Badge>
                </Label>
              ))}
            </div>
          ) : (
            <div className="rounded-lg border bg-background p-3 text-muted-foreground text-sm">
              {selectedGroupNames.length ? selectedGroupNames.join(" · ") : "尚未选择分组"}
            </div>
          )}
        </div>

        <div className="grid gap-2 rounded-lg border bg-muted/32 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <Label>Sub2API 代理</Label>
            <Button disabled={proxyLoading || (!draft.id && (!draft.base_url.trim() || !draft.admin_key.trim()))} onClick={() => void fetchProxiesFor()} size="xs" variant="outline">
              <RefreshCwIcon />
              获取代理
            </Button>
          </div>
          {proxyError && (
            <Alert variant="error">
              <AlertTitle>代理读取失败</AlertTitle>
              <AlertDescription>{proxyError}</AlertDescription>
            </Alert>
          )}
          <SelectField label="代理" onChange={(value) => updateDraft({ proxy_id: Number(value) })} options={proxyOptions} value={String(draft.proxy_id || 0)} />
        </div>

        <div className="grid gap-3 md:grid-cols-5">
          <Field label="检查间隔（秒）">
            <Input min={60} nativeInput onChange={(event) => updateDraft({ check_interval_seconds: Number(event.currentTarget.value) })} type="number" value={draft.check_interval_seconds} />
          </Field>
          <Field label="低于数量">
            <Input min={0} nativeInput onChange={(event) => updateDraft({ min_available: Number(event.currentTarget.value) })} type="number" value={draft.min_available} />
          </Field>
          <Field label="每次最多">
            <Input min={1} nativeInput onChange={(event) => updateDraft({ top_up_batch_size: Number(event.currentTarget.value) })} type="number" value={draft.top_up_batch_size} />
          </Field>
          <Field label="账号并发">
            <Input min={1} nativeInput onChange={(event) => updateDraft({ account_concurrency: Number(event.currentTarget.value) })} type="number" value={draft.account_concurrency} />
          </Field>
          <Field label="账号优先级">
            <Input min={0} nativeInput onChange={(event) => updateDraft({ account_priority: Number(event.currentTarget.value) })} type="number" value={draft.account_priority} />
          </Field>
        </div>

        <div className="grid gap-2 rounded-lg border bg-muted/32 p-3">
          <Label className="rounded-lg border bg-background px-3 py-2">
            <Checkbox checked={draft.enabled} onCheckedChange={(value) => updateDraft({ enabled: Boolean(value) })} />
            启用定时检查
          </Label>
          <Label className="rounded-lg border bg-background px-3 py-2">
            <Checkbox checked={draft.auto_sync_new} onCheckedChange={(value) => updateDraft({ auto_sync_new: Boolean(value) })} />
            新账号入池后自动同步
          </Label>
        </div>

        <div className="flex flex-wrap justify-end gap-2">
          <Button onClick={draft.id ? () => changeTargetDialogOpen(false) : resetDraft} variant="outline">
            {resetLabel}
          </Button>
          <Button disabled={!canSave || saving} onClick={() => void saveTarget()}>
            {saving ? <RefreshCwIcon className="animate-spin" /> : <CheckCircle2Icon />}
            保存
          </Button>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="grid gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <SendIcon className="size-5" />
              Sub2API 推送
            </CardTitle>
            <CardDescription>管理多个 Sub2API 目标和自动补充策略。</CardDescription>
            <CardAction>
              <Button onClick={openCreateDialog} variant="outline">
                <PlusIcon />
                新增目标
              </Button>
            </CardAction>
          </CardHeader>
          <CardPanel className="grid gap-4">
            {error && <ErrorAlert title="Sub2API 载入失败" message={error} />}
            <div className="grid gap-3 md:grid-cols-4">
              <MiniMetric label="目标" value={targets.length} />
              <MiniMetric label="启用" value={targets.filter((item) => item.enabled).length} />
              <MiniMetric label="最近同步" value={runs[0] ? formatDate(runs[0].finished_at || runs[0].started_at) : "-"} />
              <MiniMetric label="最近结果" value={runs[0]?.status || "-"} />
            </div>
            <TargetTable
              busyID={busyID}
              items={targets}
              loading={loading && !targets.length}
              onCheck={(target) => void runTarget(target, "check")}
              onDelete={(target) => void deleteTarget(target)}
              onEdit={editTarget}
              onSync={(target) => void runTarget(target, "sync")}
            />
            <RunTable items={runs.slice(0, 12)} loading={loading && !runs.length} />
          </CardPanel>
        </Card>
      </div>

      <Dialog open={targetDialogOpen} onOpenChange={changeTargetDialogOpen}>
        <DialogPopup className="max-h-[min(88vh,760px)] max-w-[min(52rem,calc(100vw-2rem))]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {draft.id ? <CheckCircle2Icon className="size-5" /> : <PlusIcon className="size-5" />}
              {draft.id ? "编辑 Sub2API 目标" : "新增 Sub2API 目标"}
            </DialogTitle>
            <DialogDescription>{draft.id ? `ID ${draft.id}` : "保存后会进入定时检查队列。"}</DialogDescription>
          </DialogHeader>
          <DialogPanel className="max-h-[min(70vh,600px)] overflow-y-auto pr-1">
            {renderTargetForm(draft.id ? "取消编辑" : "清空")}
          </DialogPanel>
        </DialogPopup>
      </Dialog>
    </>
  );
}

function Field({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <div className="grid gap-2">
      <Label>{label}</Label>
      {children}
    </div>
  );
}

function planOptionsFromPool(planCounts: TokenPlanCount[], selectedPlans: string[]): Array<{ count: number; label: string; value: string }> {
  const options: Array<{ count: number; label: string; value: string }> = [];
  const seen = new Set<string>();
  for (const item of planCounts) {
    const value = normalizePlanValue(item.plan);
    const count = Number(item.count) || 0;
    if (!value || seen.has(value) || count <= 0) {
      continue;
    }
    seen.add(value);
    options.push({ count, label: item.label || planLabel(value), value });
  }
  for (const selected of selectedPlans) {
    const value = normalizePlanValue(selected);
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    options.push({ count: 0, label: planLabel(value), value });
  }
  return options;
}

function tokenStatusLabels(values?: string[] | null): string {
  const selected = values?.length ? values : DEFAULT_TOKEN_STATUS_FILTERS;
  const labels = selected.map((value) => TOKEN_STATUS_LABELS[value] || value).filter(Boolean);
  return labels.length ? labels.join(" · ") : "可用 · 冷却";
}

function proxyOptionLabel(proxy: Sub2APIProxy): string {
  const name = proxy.name || `Proxy #${proxy.id}`;
  const protocol = proxy.protocol ? proxy.protocol.toUpperCase() : "";
  const count = typeof proxy.account_count === "number" ? ` · ${formatNumber(proxy.account_count)} 账号` : "";
  return [name, protocol].filter(Boolean).join(" · ") + count;
}

function TargetTable({
  busyID,
  items,
  loading,
  onCheck,
  onDelete,
  onEdit,
  onSync,
}: {
  busyID: number | null;
  items: Sub2APITarget[];
  loading: boolean;
  onCheck: (target: Sub2APITarget) => void;
  onDelete: (target: Sub2APITarget) => void;
  onEdit: (target: Sub2APITarget) => void;
  onSync: (target: Sub2APITarget) => void;
}) {
  if (loading && !items.length) {
    return <LoadingState label="正在载入 Sub2API 目标" />;
  }
  if (!items.length) {
    return <EmptyState title="暂无 Sub2API 目标" description="新增目标后会在这里显示。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>目标</TableHead>
            <TableHead>源账号</TableHead>
            <TableHead>策略</TableHead>
            <TableHead>最近状态</TableHead>
            <TableHead className="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((target) => (
            <TableRow key={target.id}>
              <TableCell>
                <div className="grid gap-1">
                  <strong>{target.name}</strong>
                  <span className="max-w-72 truncate text-muted-foreground text-xs">{target.base_url}</span>
                  <div className="flex flex-wrap gap-1">
                    <Badge variant={target.enabled ? "success" : "secondary"}>{target.enabled ? "启用" : "停用"}</Badge>
                    <Badge variant={target.admin_key_set ? "outline" : "warning"}>{target.admin_key_set ? "key set" : "missing key"}</Badge>
                  </div>
                </div>
              </TableCell>
              <TableCell>
                <div className="grid gap-1 text-sm">
                  <span>{target.owner_email || `User #${target.owner_user_id}`}</span>
                  <span className="text-muted-foreground text-xs">{(target.plan_filters || []).length ? (target.plan_filters || []).join(" · ") : "全部计划"}</span>
                </div>
              </TableCell>
              <TableCell className="text-sm">
                <div>低于 {formatNumber(target.min_available)} 补充</div>
                <div className="text-muted-foreground text-xs">状态 {tokenStatusLabels(target.token_status_filters)}</div>
                <div className="text-muted-foreground text-xs">
                  {formatNumber(target.check_interval_seconds)}s · 每次 {formatNumber(target.top_up_batch_size)} · 并发 {formatNumber(target.account_concurrency || 10)} · 优先级 {formatNumber(target.account_priority ?? 50)}
                </div>
                <div className="text-muted-foreground text-xs">{target.proxy_id ? `代理 #${target.proxy_id}` : "直连"} · {target.auto_sync_new ? "入池同步" : "按阈值"}</div>
              </TableCell>
              <TableCell className="text-sm">
                <Badge variant={statusVariant(target.last_status)}>{target.last_status || "idle"}</Badge>
                <div className="mt-1 text-muted-foreground text-xs">
                  远端 {formatNumber(target.last_remote_count)} · 同步 {formatNumber(target.last_synced_count)}
                </div>
                {target.last_error && <div className="mt-1 max-w-64 truncate text-destructive-foreground text-xs">{target.last_error}</div>}
              </TableCell>
              <TableCell className="text-right">
                <div className="flex flex-wrap justify-end gap-2">
                  <Button disabled={busyID === target.id} onClick={() => onEdit(target)} size="xs" variant="outline">
                    编辑
                  </Button>
                  <Button disabled={busyID === target.id} onClick={() => onCheck(target)} size="xs" variant="outline">
                    <RefreshCwIcon />
                    检查
                  </Button>
                  <Button disabled={busyID === target.id} onClick={() => onSync(target)} size="xs" variant="outline">
                    <PlayIcon />
                    同步
                  </Button>
                  <Button disabled={busyID === target.id} onClick={() => onDelete(target)} size="xs" variant="destructive-outline">
                    <Trash2Icon />
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function RunTable({ items, loading }: { items: Sub2APIRun[]; loading: boolean }) {
  if (loading && !items.length) {
    return <LoadingState compact label="正在载入同步记录" />;
  }
  if (!items.length) {
    return <EmptyState compact title="暂无同步记录" description="检查或同步后会生成运行记录。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>时间</TableHead>
            <TableHead>目标</TableHead>
            <TableHead>触发</TableHead>
            <TableHead>结果</TableHead>
            <TableHead>数量</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((run) => (
            <TableRow key={run.id}>
              <TableCell>{formatDate(run.finished_at || run.started_at)}</TableCell>
              <TableCell>#{run.target_id}</TableCell>
              <TableCell>{run.trigger}</TableCell>
              <TableCell>
                <Badge variant={statusVariant(run.status)}>{run.status}</Badge>
                {run.error_message && <div className="mt-1 max-w-72 truncate text-destructive-foreground text-xs">{run.error_message}</div>}
              </TableCell>
              <TableCell className="text-xs">
                远端 {formatNumber(run.remote_count)} · 阈值 {formatNumber(run.threshold)} · 选中 {formatNumber(run.selected_count)} · 成功 {formatNumber(run.synced_count)} · 失败 {formatNumber(run.failed_count)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function statusVariant(status?: string | null): "success" | "secondary" | "warning" | "error" | "outline" {
  switch (status) {
    case "completed":
      return "success";
    case "failed":
      return "error";
    case "skipped":
      return "warning";
    case "running":
      return "secondary";
    default:
      return "outline";
  }
}
