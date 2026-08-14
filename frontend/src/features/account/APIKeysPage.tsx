import { CopyIcon, KeyRoundIcon, PlusIcon, Trash2Icon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
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
import { api, getAuthContext, setServiceKey, type APIKeyItem } from "@/lib/api";
import { formatDate, formatNumber } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

export function AccountAPIKeysPage({
  pushToast,
  refreshNonce,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [items, setItems] = useState<APIKeyItem[]>([]);
  const [name, setName] = useState("web");
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [copyingID, setCopyingID] = useState<number | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<APIKeyItem | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState("");
  const currentAPIKeyID = Number(getAuthContext()?.api_key_id || 0);

  const activeItems = useMemo(() => items.filter((item) => !item.revoked_at), [items]);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await api.myAPIKeys();
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

  async function createKey() {
    const keyName = name.trim();
    if (!keyName) {
      pushToast("请填写 API Key 名称", "warning");
      return;
    }
    setCreating(true);
    setError("");
    try {
      const payload = await api.createMyAPIKey({ name: keyName });
      const created = payload.api_key;
      const plaintext = created?.plaintext_key || created?.value || "";
      await load();
      if (plaintext) {
        try {
          await copyToClipboard(plaintext);
          pushToast("API Key 已创建并复制");
        } catch {
          pushToast("API Key 已创建，但浏览器复制失败，请在列表中重试", "warning");
        }
      } else {
        pushToast("API Key 已创建");
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setCreating(false);
    }
  }

  async function copyKey(item: APIKeyItem) {
    if (!item.copy_available) {
      pushToast("旧 API Key 无法恢复明文，请创建一个新 Key", "warning");
      return;
    }
    setCopyingID(item.id);
    try {
      const payload = await api.revealMyAPIKey(item.id);
      const plaintext = payload.plaintext_key?.trim() || "";
      if (!plaintext) {
        throw new Error("服务端没有返回 API Key");
      }
      await copyToClipboard(plaintext);
      pushToast("API Key 已复制");
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setCopyingID(null);
    }
  }

  async function deleteKey() {
    if (!deleteTarget) {
      return;
    }
    setDeleting(true);
    try {
      const result = await api.revokeMyAPIKey(deleteTarget.id);
      setDeleteTarget(null);
      if (result.current_key_deleted) {
        setServiceKey("");
        pushToast("当前登录使用的 API Key 已删除，请重新登录", "info");
        window.setTimeout(() => window.location.reload(), 600);
        return;
      }
      pushToast("API Key 已删除");
      await load();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setDeleting(false);
    }
  }

  return (
    <div className="grid gap-4">
      <Alert variant="info">
        <KeyRoundIcon />
        <AlertTitle>调用范围已隔离</AlertTitle>
        <AlertDescription>这里创建的 API Key 只能调用你自己添加的 ChatGPT / Codex 账号，不会使用其他用户的账号池。</AlertDescription>
      </Alert>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <KeyRoundIcon className="size-5" />
            API Key
          </CardTitle>
          <CardDescription>创建、复制或删除自己的调用凭证。新创建的 Key 会加密保存，可以随时回来复制。</CardDescription>
          <CardAction>
            <Button disabled={creating} loading={creating} onClick={() => void createKey()} size="sm">
              <PlusIcon />
              创建 API Key
            </Button>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error && <ErrorAlert title="API Key 操作失败" message={error} />}
          <div className="grid gap-2 sm:max-w-md">
            <Label htmlFor="api-key-name">名称</Label>
            <Input
              id="api-key-name"
              maxLength={128}
              nativeInput
              onChange={(event) => setName(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  void createKey();
                }
              }}
              placeholder="例如 MacBook、CI、客户端"
              value={name}
            />
          </div>

          {loading && !items.length ? (
            <LoadingState label="正在载入 API Key" />
          ) : !activeItems.length ? (
            <EmptyState title="暂无 API Key" description="创建后即可使用 Bearer API Key 调用 OAIX。" />
          ) : (
            <div className="overflow-x-auto rounded-lg border oaix-scrollbar">
              <Table style={{ width: "max(100%, 58rem)" }}>
                <TableHeader>
                  <TableRow>
                    <TableHead>名称</TableHead>
                    <TableHead>Key</TableHead>
                    <TableHead>创建时间</TableHead>
                    <TableHead>最近使用</TableHead>
                    <TableHead className="text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {activeItems.map((item) => {
                    const isCurrent = currentAPIKeyID === item.id;
                    return (
                      <TableRow key={item.id}>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span>{item.name || "-"}</span>
                            {isCurrent && <Badge variant="secondary">当前登录</Badge>}
                          </div>
                        </TableCell>
                        <TableCell className="oaix-tabular">
                          <div className="flex items-center gap-2">
                            <span>{item.key_prefix || item.prefix || "-"}••••••••</span>
                            {!item.copy_available && <Badge variant="outline">旧 Key</Badge>}
                          </div>
                        </TableCell>
                        <TableCell>{formatDate(item.created_at)}</TableCell>
                        <TableCell>{formatDate(item.last_used_at)}</TableCell>
                        <TableCell className="text-right">
                          <div className="flex justify-end gap-2">
                            <Button
                              disabled={copyingID === item.id}
                              loading={copyingID === item.id}
                              onClick={() => void copyKey(item)}
                              size="xs"
                              title={item.copy_available ? "复制完整 API Key" : "旧 Key 未保存可恢复明文"}
                              variant="outline"
                            >
                              <CopyIcon />
                              复制
                            </Button>
                            <Button onClick={() => setDeleteTarget(item)} size="xs" variant="destructive-outline">
                              <Trash2Icon />
                              删除
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
          <div className="text-muted-foreground text-xs">共 {formatNumber(activeItems.length)} 个有效 API Key</div>
        </CardPanel>
      </Card>

      <Dialog open={Boolean(deleteTarget)} onOpenChange={(open) => !open && !deleting && setDeleteTarget(null)}>
        <DialogPopup className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>删除 API Key？</DialogTitle>
            <DialogDescription>
              {deleteTarget?.name || deleteTarget?.key_prefix || "这个 API Key"} 删除后立即失效，无法恢复。
              {deleteTarget?.id === currentAPIKeyID ? " 这是当前登录使用的 Key，删除后需要重新登录。" : ""}
            </DialogDescription>
          </DialogHeader>
          <DialogPanel />
          <DialogFooter>
            <DialogClose render={<Button disabled={deleting} variant="ghost" />}>取消</DialogClose>
            <Button loading={deleting} onClick={() => void deleteKey()} variant="destructive">
              <Trash2Icon />
              确认删除
            </Button>
          </DialogFooter>
        </DialogPopup>
      </Dialog>
    </div>
  );
}

async function copyToClipboard(value: string): Promise<void> {
  if (!navigator.clipboard?.writeText) {
    throw new Error("当前浏览器不支持剪贴板写入");
  }
  await navigator.clipboard.writeText(value);
}
