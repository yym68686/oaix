import { KeyRoundIcon, ShieldCheckIcon, UserRoundIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { api, isServicePrincipal, type APIKeyItem, type CreatedAPIKey, type MeResponse, type PoolSummaryResponse, type UsageSummary } from "@/lib/api";
import { formatCurrency, formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, MiniMetric } from "@/shared/components";
import { errorMessage, planOptionsWithCounts } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

export function AccountPage({ me, refreshNonce }: { me: MeResponse | null; refreshNonce: number }) {
  const [usage, setUsage] = useState<UsageSummary>({});
  const [pool, setPool] = useState<PoolSummaryResponse>({});
  const [error, setError] = useState("");
  const serviceOnly = Boolean(me && isServicePrincipal(me) && !me.user?.id);

  const load = useCallback(async () => {
    setError("");
    try {
      if (!me) {
        setUsage({});
        setPool({});
        return;
      }
      if (serviceOnly) {
        setUsage({});
        setPool(await api.adminPoolSummary());
        return;
      }
      const [usagePayload, poolPayload] = await Promise.all([api.myUsage(24), api.myPoolSummary()]);
      setUsage(usagePayload.usage || {});
      setPool(poolPayload);
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }, [me, serviceOnly]);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  const user = me?.user;
  const cachePercent = Number(usage.cache_hit_ratio || 0) * 100;
  const title = serviceOnly ? "平台服务凭证" : "我的账号";
  const description = serviceOnly ? "当前使用 Service API Key，拥有平台级管理权限，不对应注册用户。" : "当前 API Key 对应的用户身份、请求概况和账号池摘要。";
  return (
    <div className="grid gap-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            {serviceOnly ? <ShieldCheckIcon className="size-5" /> : <UserRoundIcon className="size-5" />}
            {title}
          </CardTitle>
          <CardDescription>{description}</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error && <ErrorAlert title="账号数据载入失败" message={error} />}
          <div className="grid gap-3 md:grid-cols-4">
            <MiniMetric label={serviceOnly ? "凭证" : "邮箱"} value={serviceOnly ? "Service API Key" : user?.email || "-"} />
            <MiniMetric label="角色" value={me?.role || user?.role || "-"} />
            <MiniMetric label="状态" value={serviceOnly ? "platform" : user?.status || "-"} />
            <MiniMetric label="用户 ID" value={user?.id || "-"} />
          </div>
          {!serviceOnly && (
            <div className="grid gap-3 md:grid-cols-5">
              <MiniMetric label="24h 请求" value={usage.total || 0} />
              <MiniMetric label="成功" value={usage.success || 0} />
              <MiniMetric label="失败" value={usage.failure || 0} />
              <MiniMetric label="缓存率" value={`${Math.round(cachePercent)}%`} />
              <MiniMetric label="成本" value={formatCurrency(usage.estimated_cost_usd || 0)} />
            </div>
          )}
          <div className="grid gap-3 md:grid-cols-4">
            <MiniMetric label={serviceOnly ? "全局 Key 总数" : "Key 总数"} value={pool.counts?.total || 0} />
            <MiniMetric label="有效" value={pool.counts?.available ?? pool.counts?.active ?? 0} />
            <MiniMetric label="冷却" value={pool.counts?.cooling || 0} />
            <MiniMetric label="禁用" value={pool.counts?.disabled || 0} />
          </div>
          <div className="flex flex-wrap gap-2">
            {planOptionsWithCounts(pool.plan_counts || []).slice(1).map((item) => (
              <Badge key={item.value} variant="secondary">
                {item.label}
              </Badge>
            ))}
          </div>
        </CardPanel>
      </Card>
    </div>
  );
}

export function AccountAPIKeysPage({ me, pushToast, refreshNonce }: { me: MeResponse | null; pushToast: (title: string, variant?: ToastMessage["variant"]) => void; refreshNonce: number }) {
  const [items, setItems] = useState<APIKeyItem[]>([]);
  const [created, setCreated] = useState<CreatedAPIKey | null>(null);
  const [name, setName] = useState("web");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const serviceOnly = Boolean(me && isServicePrincipal(me) && !me.user?.id);

  const load = useCallback(async () => {
    if (!me || serviceOnly) {
      setItems([]);
      setError("");
      return;
    }
    setError("");
    try {
      const payload = await api.myAPIKeys();
      setItems(payload.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    }
  }, [me, serviceOnly]);

  useEffect(() => {
    void load();
  }, [load, refreshNonce]);

  async function createKey() {
    setBusy(true);
    setError("");
    try {
      const payload = await api.createMyAPIKey({ name });
      setCreated(payload.api_key || null);
      pushToast("API Key 已创建");
      await load();
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  }

  async function revokeKey(id: number) {
    await api.revokeMyAPIKey(id);
    pushToast("API Key 已撤销");
    await load();
  }

  const plaintext = created?.plaintext_key || created?.value || "";
  if (serviceOnly) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ShieldCheckIcon className="size-5" />
            平台服务凭证
          </CardTitle>
          <CardDescription>当前凭证来自 Service API Key，不对应注册用户；用户 API Key 请在管理员用户状态中为具体用户创建。</CardDescription>
        </CardHeader>
        <CardPanel>
          <EmptyState title="没有个人 API Key" description="Service API Key 是平台级凭证，不在个人 API Key 列表中创建或撤销。" />
        </CardPanel>
      </Card>
    );
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <KeyRoundIcon className="size-5" />
          我的 API Key
        </CardTitle>
        <CardDescription>创建和撤销自己的调用 key。明文只在创建后显示一次。</CardDescription>
        <CardAction>
          <Button loading={busy} onClick={createKey} size="sm">
            新建 Key
          </Button>
        </CardAction>
      </CardHeader>
      <CardPanel className="grid gap-4">
        {error && <ErrorAlert title="API Key 操作失败" message={error} />}
        <div className="grid gap-2 md:max-w-md">
          <Label htmlFor="api-key-name">名称</Label>
          <Input id="api-key-name" nativeInput onChange={(event) => setName(event.currentTarget.value)} value={name} />
        </div>
        {plaintext && (
          <div className="grid gap-2 rounded-lg border bg-muted/40 p-3">
            <Label>一次性明文</Label>
            <div className="flex gap-2">
              <Input className="min-w-0 flex-1" nativeInput readOnly value={plaintext} />
              <Button
                onClick={() => {
                  void navigator.clipboard?.writeText(plaintext);
                  pushToast("API Key 已复制");
                }}
                variant="outline"
              >
                复制
              </Button>
            </div>
          </div>
        )}
        {!items.length ? (
          <EmptyState title="暂无 API Key" description="创建后会显示 key prefix、角色和使用时间。" />
        ) : (
          <div className="overflow-hidden rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>名称</TableHead>
                  <TableHead>Prefix</TableHead>
                  <TableHead>角色</TableHead>
                  <TableHead>创建</TableHead>
                  <TableHead>最近使用</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((item) => (
                  <TableRow key={item.id}>
                    <TableCell>{item.name || "-"}</TableCell>
                    <TableCell className="oaix-tabular">{item.key_prefix || item.prefix || "-"}</TableCell>
                    <TableCell>{item.role || item.kind || "-"}</TableCell>
                    <TableCell>{formatDate(item.created_at)}</TableCell>
                    <TableCell>{formatDate(item.last_used_at)}</TableCell>
                    <TableCell className="text-right">
                      <Button disabled={Boolean(item.revoked_at)} onClick={() => void revokeKey(item.id)} size="xs" variant="destructive-outline">
                        撤销
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
        <div className="text-muted-foreground text-xs">合计 {formatNumber(items.length)} 个 key</div>
      </CardPanel>
    </Card>
  );
}
