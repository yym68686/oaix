import { DatabaseIcon, FileClockIcon, ListFilterIcon, SearchIcon, UserRoundIcon, UsersRoundIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { navigateTo, type RouteState } from "@/app/router";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { api, type APIKeyItem, type CreatedAPIKey, type ImportBatch, type OwnerUsageSummary, type PlatformUser, type PoolSummaryResponse, type RequestItem, type TokenCounts, type TokenItem, type TokenPlanCount } from "@/lib/api";
import { formatCurrency, formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState, MiniMetric, SelectField } from "@/shared/components";
import { errorMessage, planOptionsWithCounts, statusOptionsWithCounts } from "@/shared/domain";
import { ApiKeyTable, ImportJobsTable, PoolSummaryCards, RequestLogsTable, TokenTable, UserSelector } from "@/shared/resourceTables";
import type { ToastMessage } from "@/shared/types";

export function AdminUsersPage({ pushToast, refreshNonce }: { pushToast: (title: string, variant?: ToastMessage["variant"]) => void; refreshNonce: number }) {
  const [users, setUsers] = useState<PlatformUser[]>([]);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [role, setRole] = useState("all");
  const [plan, setPlan] = useState("all");
  const [activity, setActivity] = useState("all");
  const [poolByUser, setPoolByUser] = useState<Record<number, TokenCounts>>({});
  const [usageByUser, setUsageByUser] = useState<Record<number, OwnerUsageSummary>>({});
  const [newEmail, setNewEmail] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [createdKey, setCreatedKey] = useState<CreatedAPIKey | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({ limit: "100", hours: "24" });
      if (query.trim()) params.set("q", query.trim());
      if (status !== "all") params.set("status", status);
      if (role !== "all") params.set("role", role);
      if (plan !== "all") params.set("plan", plan);
      if (activity === "24h") params.set("active_within_hours", "24");
      if (activity === "7d") params.set("active_within_hours", "168");
      if (activity === "inactive7d") params.set("inactive_for_hours", "168");
      const [payload, poolPayload] = await Promise.all([api.adminUsers(params), api.adminPoolSummaryByUser(params)]);
      setUsers(payload.items || []);
      const nextPools: Record<number, TokenCounts> = {};
      const nextUsage: Record<number, OwnerUsageSummary> = {};
      for (const item of poolPayload.items || []) {
        if (item.user?.id) {
          nextPools[item.user.id] = item.counts || {};
          nextUsage[item.user.id] = item.usage || {};
        }
      }
      setPoolByUser(nextPools);
      setUsageByUser(nextUsage);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [activity, plan, query, role, status]);

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), query.trim() ? 240 : 0);
    return () => window.clearTimeout(timer);
  }, [load, refreshNonce, query]);

  async function createUser() {
    try {
      const payload = await api.adminCreateUser({ email: newEmail, password: newPassword, role: "user", create_api_key: true });
      setCreatedKey(payload.api_key || null);
      setNewEmail("");
      setNewPassword("");
      pushToast("用户已创建");
      await load();
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <UsersRoundIcon className="size-5" />
          用户状态
        </CardTitle>
        <CardDescription>查看平台用户、状态和角色。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        {error && <ErrorAlert title="用户数据载入失败" message={error} />}
        <div className="grid gap-3 xl:grid-cols-[minmax(260px,1fr)_150px_150px_150px_170px]">
          <div className="grid gap-2">
            <Label htmlFor="admin-user-search">搜索用户</Label>
            <div className="relative">
              <SearchIcon className="-translate-y-1/2 pointer-events-none absolute left-3 top-1/2 size-4 text-muted-foreground" />
              <Input id="admin-user-search" className="pl-9" nativeInput onChange={(event) => setQuery(event.currentTarget.value)} value={query} />
            </div>
          </div>
          <SelectField label="状态" value={status} onChange={setStatus} options={[{ label: "全部", value: "all" }, { label: "active", value: "active" }, { label: "disabled", value: "disabled" }, { label: "suspended", value: "suspended" }]} />
          <SelectField label="角色" value={role} onChange={setRole} options={[{ label: "全部", value: "all" }, { label: "user", value: "user" }, { label: "admin", value: "admin" }, { label: "readonly_admin", value: "readonly_admin" }, { label: "service", value: "service" }]} />
          <SelectField label="计划" value={plan} onChange={setPlan} options={[{ label: "全部", value: "all" }, { label: "Free", value: "free" }, { label: "Plus", value: "plus" }, { label: "Team", value: "team" }, { label: "Pro", value: "pro" }, { label: "Unknown", value: "unknown" }]} />
          <SelectField label="活跃度" value={activity} onChange={setActivity} options={[{ label: "全部", value: "all" }, { label: "24h 活跃", value: "24h" }, { label: "7d 活跃", value: "7d" }, { label: "7d 未活跃", value: "inactive7d" }]} />
        </div>
        <div className="grid gap-3 rounded-lg border bg-muted/30 p-3 md:grid-cols-[minmax(180px,1fr)_minmax(160px,1fr)_auto]">
          <Input nativeInput onChange={(event) => setNewEmail(event.currentTarget.value)} placeholder="新用户邮箱" type="email" value={newEmail} />
          <Input nativeInput onChange={(event) => setNewPassword(event.currentTarget.value)} placeholder="初始密码" type="password" value={newPassword} />
          <Button disabled={!newEmail.trim() || newPassword.length < 6} onClick={() => void createUser()}>
            创建用户
          </Button>
          {createdKey?.plaintext_key && (
            <Input className="md:col-span-3" nativeInput readOnly value={createdKey.plaintext_key} />
          )}
        </div>
        {loading && !users.length ? (
          <LoadingState label="正在载入用户" />
        ) : (
          <div className="overflow-hidden rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>用户</TableHead>
                  <TableHead>角色</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>计划</TableHead>
                  <TableHead>账号池</TableHead>
                  <TableHead>请求</TableHead>
                  <TableHead>缓存 / 成本</TableHead>
                  <TableHead>最近活跃</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {users.map((user) => {
                  const counts = poolByUser[user.id] || {};
                  const usage = usageByUser[user.id] || {};
                  return (
                    <TableRow key={user.id}>
                      <TableCell>
                        <div className="grid gap-1">
                          <strong>{user.email || `User #${user.id}`}</strong>
                          <span className="text-muted-foreground text-xs">ID {user.id}</span>
                        </div>
                      </TableCell>
                      <TableCell>{user.role}</TableCell>
                      <TableCell>
                        <Badge variant={user.status === "active" ? "success" : "warning"}>{user.status}</Badge>
                      </TableCell>
                      <TableCell>{user.plan || "-"}</TableCell>
                      <TableCell className="text-xs">
                        总 {formatNumber(counts.total)} · 有效 {formatNumber(counts.available ?? counts.active)} · 冷却 {formatNumber(counts.cooling)} · 禁用 {formatNumber(counts.disabled)}
                      </TableCell>
                      <TableCell className="text-xs">
                        请求 {formatNumber(usage.request_count)} · 成功 {formatPercent(usage.success_rate)}
                      </TableCell>
                      <TableCell className="text-xs">
                        缓存 {formatPercent(usage.cache_hit_ratio)} · {formatCurrency(usage.estimated_cost_usd || 0)}
                      </TableCell>
                      <TableCell>{formatDate(user.last_seen_at || user.last_login_at)}</TableCell>
                      <TableCell className="text-right">
                        <Button onClick={() => navigateTo(`/admin/users/${user.id}`)} size="xs" variant="outline">
                          查看
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}
        {!loading && !users.length && <EmptyState title="暂无用户" description="创建用户后会在这里显示。" />}
      </CardPanel>
    </Card>
  );
}

export function AdminUserDetailPage({ pushToast, refreshNonce, route }: { pushToast: (title: string, variant?: ToastMessage["variant"]) => void; refreshNonce: number; route: RouteState }) {
  const userID = Number(route.params.id || 0);
  const [user, setUser] = useState<PlatformUser | null>(null);
  const [apiKeys, setAPIKeys] = useState<APIKeyItem[]>([]);
  const [tokens, setTokens] = useState<TokenItem[]>([]);
  const [imports, setImports] = useState<ImportBatch[]>([]);
  const [pool, setPool] = useState<PoolSummaryResponse>({});
  const [usage, setUsage] = useState<OwnerUsageSummary>({});
  const [requests, setRequests] = useState<RequestItem[]>([]);
  const [auditItems, setAuditItems] = useState<Array<Record<string, unknown>>>([]);
  const [tab, setTab] = useState<"overview" | "tokens" | "imports" | "requests" | "api_keys" | "audit">("overview");
  const [createdKey, setCreatedKey] = useState<CreatedAPIKey | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    if (!userID) {
      setLoading(false);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const [userResult, keysResult, tokenResult, importResult, requestResult, usageResult, auditResult] = await Promise.allSettled([
        api.adminUser(userID),
        api.adminUserAPIKeys(userID),
        api.adminUserTokens(userID, new URLSearchParams({ limit: "50", include_quota: "true" })),
        api.adminUserImportJobs(userID, new URLSearchParams({ limit: "50" })),
        api.adminUserRequests(userID, new URLSearchParams({ limit: "80", include_total: "true" })),
        api.adminUserUsage(userID, 24),
        api.adminAuditLogs(200),
      ]);
      const failures: string[] = [];
      const read = <T,>(result: PromiseSettledResult<T>, label: string): T | null => {
        if (result.status === "fulfilled") {
          return result.value;
        }
        failures.push(`${label}: ${errorMessage(result.reason)}`);
        return null;
      };
      const userPayload = read(userResult, "用户");
      const keysPayload = read(keysResult, "API Key");
      const tokenPayload = read(tokenResult, "Key");
      const importPayload = read(importResult, "导入");
      const requestPayload = read(requestResult, "请求");
      const usagePayload = read(usageResult, "用量");
      const auditPayload = read(auditResult, "审计");

      if (userPayload) setUser(userPayload.user || null);
      if (keysPayload) setAPIKeys(keysPayload.items || []);
      if (tokenPayload) {
        setTokens(tokenPayload.items || []);
        setPool({ counts: tokenPayload.counts, plan_counts: tokenPayload.plan_counts });
      }
      if (importPayload) setImports(importPayload.items || []);
      if (usagePayload) setUsage(usagePayload.usage || {});
      if (requestPayload) setRequests(requestPayload.items || []);
      if (auditPayload) {
        setAuditItems((auditPayload.items || []).filter((item) => String(item.target_id || "") === String(userID) || String((item.payload as any)?.user_id || "") === String(userID)).slice(0, 40));
      }
      if (failures.length) {
        setError(`部分用户详情载入失败：${failures.join("；")}`);
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [userID]);

  useEffect(() => {
    setLoading(true);
    setUser(null);
    setAPIKeys([]);
    setTokens([]);
    setImports([]);
    setPool({});
    setUsage({});
    setRequests([]);
    setAuditItems([]);
    setCreatedKey(null);
    setError("");
  }, [userID]);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  async function createAPIKey() {
    const payload = await api.adminCreateUserAPIKey(userID, { name: "admin-created", role: "user" });
    setCreatedKey(payload.api_key || null);
    pushToast("用户 API Key 已创建");
    await load();
  }

  async function revokeAPIKey(id: number) {
    await api.adminRevokeUserAPIKey(userID, id);
    pushToast("用户 API Key 已撤销");
    await load();
  }

  return (
    <div className="grid gap-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <UserRoundIcon className="size-5" />
            用户详情
          </CardTitle>
          <CardDescription>{user?.email || `User #${userID}`}</CardDescription>
          <CardAction>
            <Button onClick={() => navigateTo("/admin/users")} size="sm" variant="outline">
              返回
            </Button>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error && <ErrorAlert title={error.startsWith("部分") ? "用户详情部分载入失败" : "用户详情载入失败"} message={error} />}
          {loading && !user ? (
            <LoadingState compact label="正在载入用户详情" />
          ) : (
            <div className="grid gap-3 md:grid-cols-4">
              <MiniMetric label="角色" value={user?.role || "-"} />
              <MiniMetric label="状态" value={user?.status || "-"} />
              <MiniMetric label="Key 总数" value={pool.counts?.total || 0} />
              <MiniMetric label="有效 Key" value={pool.counts?.available ?? pool.counts?.active ?? 0} />
            </div>
          )}
        </CardPanel>
      </Card>
      <div className="flex flex-wrap gap-2">
        {[
          ["overview", "概览"],
          ["tokens", "Key"],
          ["imports", "导入"],
          ["requests", "请求"],
          ["api_keys", "API Key"],
          ["audit", "审计"],
        ].map(([value, label]) => (
          <Button key={value} onClick={() => setTab(value as typeof tab)} size="sm" variant={tab === value ? "default" : "outline"}>
            {label}
          </Button>
        ))}
      </div>
      {tab === "overview" && (
        <Card>
          <CardHeader>
            <CardTitle>概览</CardTitle>
          </CardHeader>
          <CardPanel className="grid gap-3 md:grid-cols-4">
            <MiniMetric label="请求量" value={usage.request_count || 0} />
            <MiniMetric label="成功率" value={formatPercent(usage.success_rate)} />
            <MiniMetric label="缓存率" value={formatPercent(usage.cache_hit_ratio)} />
            <MiniMetric label="成本" value={formatCurrency(usage.estimated_cost_usd || 0)} />
          </CardPanel>
        </Card>
      )}
      {tab === "tokens" && <TokenTable items={tokens} loading={loading && !tokens.length} scope={{ kind: "user", userId: userID }} />}
      {tab === "imports" && <ImportJobsTable items={imports} loading={loading && !imports.length} scope={{ kind: "user", userId: userID }} />}
      {tab === "requests" && <RequestLogsTable items={requests} loading={loading && !requests.length} scope={{ kind: "user", userId: userID }} />}
      {tab === "api_keys" && (
        <Card>
          <CardHeader>
            <CardTitle>API Key</CardTitle>
            <CardAction>
              <Button onClick={() => void createAPIKey()} size="sm">
                新建
              </Button>
            </CardAction>
          </CardHeader>
          <CardPanel className="grid gap-3">
            {createdKey?.plaintext_key && <Input nativeInput readOnly value={createdKey.plaintext_key} />}
            <ApiKeyTable items={apiKeys} loading={loading && !apiKeys.length} onRevoke={(id) => void revokeAPIKey(id)} scope={{ kind: "user", userId: userID }} />
          </CardPanel>
        </Card>
      )}
      {tab === "audit" && <AuditMiniTable items={auditItems} loading={loading && !auditItems.length} />}
    </div>
  );
}

export function AdminPoolsPage({ refreshNonce }: { refreshNonce: number }) {
  const [summary, setSummary] = useState<PoolSummaryResponse>({});
  const [byUser, setByUser] = useState<Array<{ user?: PlatformUser; counts?: TokenCounts; plan_counts?: TokenPlanCount[]; usage?: OwnerUsageSummary }>>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({ limit: "100", hours: "24" });
      const [pool, users] = await Promise.all([api.adminPoolSummary(), api.adminPoolSummaryByUser(params)]);
      setSummary(pool);
      setByUser(users.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <DatabaseIcon className="size-5" />
          号池总览
        </CardTitle>
        <CardDescription>全局账号池状态和按用户聚合。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-4">
        {error && <ErrorAlert title="号池载入失败" message={error} />}
        <PoolSummaryCards summary={summary} />
        <div className="flex flex-wrap gap-2">
          {planOptionsWithCounts(summary.plan_counts || []).slice(1).map((item) => (
            <Badge key={item.value} variant="secondary">
              {item.label}
            </Badge>
          ))}
        </div>
        {loading && !byUser.length ? (
          <LoadingState label="正在载入号池数据" />
        ) : byUser.length ? (
          <div className="overflow-hidden rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>用户</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>计划</TableHead>
                  <TableHead>请求</TableHead>
                  <TableHead>缓存 / 成本</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {byUser.map((item) => (
                  <TableRow key={item.user?.id}>
                    <TableCell>{item.user?.email || `User #${item.user?.id}`}</TableCell>
                    <TableCell>{statusOptionsWithCounts(item.counts || {}).map((option) => option.label).join(" · ")}</TableCell>
                    <TableCell>{planOptionsWithCounts(item.plan_counts || []).slice(1, 5).map((option) => option.label).join(" · ")}</TableCell>
                    <TableCell>请求 {formatNumber(item.usage?.request_count)} · 成功 {formatPercent(item.usage?.success_rate)}</TableCell>
                    <TableCell>缓存 {formatPercent(item.usage?.cache_hit_ratio)} · {formatCurrency(item.usage?.estimated_cost_usd || 0)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyState title="暂无用户号池" description="用户导入 Key 后会在这里显示。" />
        )}
      </CardPanel>
    </Card>
  );
}

export function AdminRequestsPage({ refreshNonce }: { refreshNonce: number }) {
  const [items, setItems] = useState<RequestItem[]>([]);
  const [userID, setUserID] = useState("");
  const [apiKeyID, setAPIKeyID] = useState("");
  const [model, setModel] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const requestParams = useCallback(() => {
    const params = new URLSearchParams({ limit: "160" });
    if (userID.trim()) params.set("user_id", userID.trim());
    if (apiKeyID.trim()) params.set("api_key_id", apiKeyID.trim());
    if (model.trim()) params.set("model", model.trim());
    return params;
  }, [apiKeyID, model, userID]);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await api.adminRequests(requestParams());
      setItems(payload.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [requestParams]);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ListFilterIcon className="size-5" />
          全局请求
        </CardTitle>
        <CardDescription>全局最近请求，包含 owner/api_key 字段。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-3">
        {error && <ErrorAlert title="全局请求载入失败" message={error} />}
        <UserSelector
          apiKeyID={apiKeyID}
          model={model}
          onAPIKeyIDChange={setAPIKeyID}
          onExport={() => {
            window.location.href = api.adminRequestsExportURL(requestParams());
          }}
          onModelChange={setModel}
          onUserIDChange={setUserID}
          userID={userID}
        />
        <RequestLogsTable items={items} loading={loading && !items.length} scope={{ kind: "all" }} />
      </CardPanel>
    </Card>
  );
}

export function AdminAuditPage({ refreshNonce }: { refreshNonce: number }) {
  const [items, setItems] = useState<Array<Record<string, unknown>>>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await api.adminAuditLogs(120);
      setItems(payload.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FileClockIcon className="size-5" />
          审计日志
        </CardTitle>
        <CardDescription>用户、API Key、导入和管理操作记录。</CardDescription>
      </CardHeader>
      <CardPanel className="grid gap-3">
        {error && <ErrorAlert title="审计日志载入失败" message={error} />}
        {loading && !items.length ? (
          <LoadingState label="正在载入审计日志" />
        ) : items.length ? (
          <div className="overflow-hidden rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>时间</TableHead>
                  <TableHead>动作</TableHead>
                  <TableHead>Actor</TableHead>
                  <TableHead>目标</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((item, index) => (
                  <TableRow key={String(item.id || index)}>
                    <TableCell>{formatDate(String(item.created_at || ""))}</TableCell>
                    <TableCell>{String(item.action || "-")}</TableCell>
                    <TableCell>{String(item.actor || "-")}</TableCell>
                    <TableCell>{String(item.target_type || "-")} {String(item.target_id || "")}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyState title="暂无审计记录" description="用户、API Key、导入和管理操作会在这里显示。" />
        )}
      </CardPanel>
    </Card>
  );
}

function AdminTokenMiniTable({ items }: { items: TokenItem[] }) {
  if (!items.length) {
    return <EmptyState title="暂无 Key" description="该用户还没有导入账号。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Key</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>计划</TableHead>
            <TableHead>最近使用</TableHead>
            <TableHead>备注</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => (
            <TableRow key={item.id}>
              <TableCell>
                <strong>{item.email || item.account_id || `Token #${item.id}`}</strong>
                <div className="text-muted-foreground text-xs">ID {item.id}</div>
              </TableCell>
              <TableCell>
                <Badge variant={item.is_active ? "success" : "warning"}>{item.is_active ? "有效" : "禁用"}</Badge>
              </TableCell>
              <TableCell>{item.plan_type || "unknown"}</TableCell>
              <TableCell>{formatDate(item.last_used_at)}</TableCell>
              <TableCell className="max-w-80 whitespace-pre-wrap break-words text-muted-foreground text-xs">{item.remark || item.source_file || "-"}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function ImportJobsMiniTable({ items }: { items: ImportBatch[] }) {
  if (!items.length) {
    return <EmptyState title="暂无导入批次" description="该用户还没有导入任务。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>批次</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>进度</TableHead>
            <TableHead>结果</TableHead>
            <TableHead>时间</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => (
            <TableRow key={item.id}>
              <TableCell>#{item.id}</TableCell>
              <TableCell><Badge variant={item.status === "completed" ? "success" : "secondary"}>{item.status}</Badge></TableCell>
              <TableCell>{formatNumber(item.processed_count)} / {formatNumber(item.total_count)}</TableCell>
              <TableCell>创建 {formatNumber(item.created_count)} · 更新 {formatNumber(item.updated_count)} · 失败 {formatNumber(item.failed_count)}</TableCell>
              <TableCell>{formatDate(item.submitted_at || item.completed_at)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function AuditMiniTable({ items, loading = false }: { items: Array<Record<string, unknown>>; loading?: boolean }) {
  if (loading && !items.length) {
    return <LoadingState label="正在载入审计记录" />;
  }
  if (!items.length) {
    return <EmptyState title="暂无审计记录" description="该用户相关操作会在这里显示。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>时间</TableHead>
            <TableHead>动作</TableHead>
            <TableHead>Actor</TableHead>
            <TableHead>目标</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item, index) => (
            <TableRow key={String(item.id || index)}>
              <TableCell>{formatDate(String(item.created_at || ""))}</TableCell>
              <TableCell>{String(item.action || "-")}</TableCell>
              <TableCell>{String(item.actor || "-")}</TableCell>
              <TableCell>{String(item.target_type || "-")} {String(item.target_id || "")}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function SimpleAPIKeyTable({ items, onRevoke }: { items: APIKeyItem[]; onRevoke: (id: number) => void }) {
  if (!items.length) {
    return <EmptyState title="暂无 API Key" description="新建后会在这里显示。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>名称</TableHead>
            <TableHead>Prefix</TableHead>
            <TableHead>角色</TableHead>
            <TableHead>最近使用</TableHead>
            <TableHead className="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => (
            <TableRow key={item.id}>
              <TableCell>{item.name || "-"}</TableCell>
              <TableCell>{item.key_prefix || item.prefix || "-"}</TableCell>
              <TableCell>{item.role || item.kind || "-"}</TableCell>
              <TableCell>{formatDate(item.last_used_at)}</TableCell>
              <TableCell className="text-right">
                <Button disabled={Boolean(item.revoked_at)} onClick={() => onRevoke(item.id)} size="xs" variant="destructive-outline">
                  撤销
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function RequestMiniTable({ items }: { items: RequestItem[] }) {
  if (!items.length) {
    return <EmptyState title="暂无请求" description="有流量后会在这里显示。" />;
  }
  return (
    <div className="overflow-hidden rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>时间</TableHead>
            <TableHead>Owner</TableHead>
            <TableHead>模型</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>成本</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item, index) => (
            <TableRow key={item.id || item.started_at || index}>
              <TableCell>{formatDate(item.started_at)}</TableCell>
              <TableCell>{String((item as any).owner_user_id || "-")}</TableCell>
              <TableCell>{item.model_name || item.model || "-"}</TableCell>
              <TableCell>
                <Badge variant={item.success === false ? "error" : "success"}>{item.status_code || "-"}</Badge>
              </TableCell>
              <TableCell>{formatCurrency((item as any).estimated_cost_usd || 0)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function formatPercent(value: unknown): string {
  const number = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(number) || number <= 0) {
    return "0%";
  }
  return `${Math.round(number * 100)}%`;
}
