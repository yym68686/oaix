import { DownloadIcon } from "lucide-react";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Input } from "@/registry/default/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import type { APIKeyItem, ImportBatch, PoolSummaryResponse, RequestItem, TokenItem } from "@/lib/api";
import { formatCurrency, formatDate, formatNumber } from "@/lib/format";
import { EmptyState, MiniMetric, SelectField } from "@/shared/components";

export type ResourceScope = { kind: "all" } | { kind: "user"; userId: number };

export function PoolSummaryCards({ summary }: { summary: PoolSummaryResponse }) {
  return (
    <div className="grid gap-3 md:grid-cols-4">
      <MiniMetric label="总数" value={summary.counts?.total || 0} />
      <MiniMetric label="有效" value={summary.counts?.available ?? summary.counts?.active ?? 0} />
      <MiniMetric label="冷却" value={summary.counts?.cooling || 0} />
      <MiniMetric label="禁用" value={summary.counts?.disabled || 0} />
    </div>
  );
}

export function TokenFilters({
  onPlanChange,
  onStatusChange,
  plan,
  planOptions,
  status,
  statusOptions,
}: {
  onPlanChange: (value: string) => void;
  onStatusChange: (value: string) => void;
  plan: string;
  planOptions: Array<{ label: string; value: string }>;
  status: string;
  statusOptions: Array<{ label: string; value: string }>;
}) {
  return (
    <div className="grid gap-3 md:grid-cols-2">
      <SelectField label="状态" onChange={onStatusChange} options={statusOptions} value={status} />
      <SelectField label="计划" onChange={onPlanChange} options={planOptions} value={plan} />
    </div>
  );
}

export function UserSelector({
  apiKeyID,
  model,
  onAPIKeyIDChange,
  onExport,
  onModelChange,
  onUserIDChange,
  userID,
}: {
  apiKeyID: string;
  model: string;
  onAPIKeyIDChange: (value: string) => void;
  onExport?: () => void;
  onModelChange: (value: string) => void;
  onUserIDChange: (value: string) => void;
  userID: string;
}) {
  return (
    <div className="grid gap-3 md:grid-cols-[1fr_1fr_1fr_auto]">
      <Input nativeInput onChange={(event) => onUserIDChange(event.currentTarget.value)} placeholder="User ID" value={userID} />
      <Input nativeInput onChange={(event) => onAPIKeyIDChange(event.currentTarget.value)} placeholder="API Key ID" value={apiKeyID} />
      <Input nativeInput onChange={(event) => onModelChange(event.currentTarget.value)} placeholder="模型" value={model} />
      {onExport && (
        <Button onClick={onExport} variant="outline">
          <DownloadIcon />
          导出
        </Button>
      )}
    </div>
  );
}

export function ApiKeyTable({ items, onRevoke, scope }: { items: APIKeyItem[]; onRevoke?: (id: number) => void; scope?: ResourceScope }) {
  void scope;
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
                {onRevoke && (
                  <Button disabled={Boolean(item.revoked_at)} onClick={() => onRevoke(item.id)} size="xs" variant="destructive-outline">
                    撤销
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export function ImportJobsTable({ items, scope }: { items: ImportBatch[]; scope?: ResourceScope }) {
  void scope;
  if (!items.length) {
    return <EmptyState title="暂无导入批次" description="没有匹配的导入任务。" />;
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
              <TableCell>
                <Badge variant={item.status === "completed" ? "success" : "secondary"}>{item.status}</Badge>
              </TableCell>
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

export function RequestLogsTable({ items, scope }: { items: RequestItem[]; scope?: ResourceScope }) {
  void scope;
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
            <TableHead>API Key</TableHead>
            <TableHead>模型</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>成本</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item, index) => (
            <TableRow key={item.request_id || item.id || item.started_at || index}>
              <TableCell>{formatDate(item.started_at)}</TableCell>
              <TableCell>{String(item.owner_user_id || "-")}</TableCell>
              <TableCell>{String(item.api_key_id || "-")}</TableCell>
              <TableCell>{item.model_name || item.model || "-"}</TableCell>
              <TableCell>
                <Badge variant={item.success === false ? "error" : "success"}>{item.status_code || "-"}</Badge>
              </TableCell>
              <TableCell>{formatCurrency(item.estimated_cost_usd || 0)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export function TokenTable({ items, scope }: { items: TokenItem[]; scope?: ResourceScope }) {
  void scope;
  if (!items.length) {
    return <EmptyState title="暂无 Key" description="没有匹配的 Key。" />;
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
