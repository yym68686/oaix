import { CheckIcon, LogInIcon, MailIcon, ShieldCheckIcon, Trash2Icon, UserPlusIcon, UserRoundIcon } from "lucide-react";
import { useMemo, useState } from "react";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Card, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
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
import {
  activateSavedAccount,
  getActiveSavedAccount,
  rememberPasswordAuthAccount,
  removeSavedAccount,
  savedAccountLabel,
  useSavedAccounts,
  type SavedAccount,
} from "@/lib/accounts";
import { api, type MeResponse } from "@/lib/api";
import { formatDate } from "@/lib/format";
import { EmptyState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

export function ProfilePage({
  me,
  onRefresh,
  pushToast,
}: {
  me: MeResponse | null;
  onRefresh: () => void;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
}) {
  const accounts = useSavedAccounts();
  const activeAccount = getActiveSavedAccount(accounts);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [adding, setAdding] = useState(false);
  const [removeTarget, setRemoveTarget] = useState<SavedAccount | null>(null);
  const [removing, setRemoving] = useState(false);
  const profile = activeAccount?.user || me?.user || null;
  const profileName = profile?.display_name?.trim() || profile?.email?.trim() || savedAccountLabel(activeAccount);
  const role = String(activeAccount?.role || profile?.role || me?.role || "").toLowerCase();

  const orderedAccounts = useMemo(() => {
    if (!activeAccount) {
      return accounts;
    }
    return [activeAccount, ...accounts.filter((account) => account.id !== activeAccount.id)];
  }, [accounts, activeAccount]);

  async function addAccount() {
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !password) {
      pushToast("请填写要添加账号的邮箱和密码", "warning");
      return;
    }
    setAdding(true);
    try {
      const result = await api.login({ email: normalizedEmail, password, name: "web" });
      const key = result.api_key?.plaintext_key || result.api_key?.value || "";
      if (!key) {
        throw new Error("服务端没有返回登录 API Key");
      }
      rememberPasswordAuthAccount(key, result.user, result.api_key?.id);
      setEmail("");
      setPassword("");
      pushToast(`已添加并切换到 ${result.user?.email || normalizedEmail}`);
      onRefresh();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setAdding(false);
    }
  }

  function switchAccount(account: SavedAccount) {
    if (account.id === activeAccount?.id) {
      return;
    }
    if (!activateSavedAccount(account.id)) {
      pushToast("保存的账号凭证已不存在，请重新添加", "error");
      return;
    }
    pushToast(`已切换到 ${savedAccountLabel(account)}`);
    onRefresh();
  }

  function confirmRemoveAccount() {
    if (!removeTarget) {
      return;
    }
    setRemoving(true);
    try {
      const label = savedAccountLabel(removeTarget);
      const result = removeSavedAccount(removeTarget.id);
      setRemoveTarget(null);
      pushToast(`已从本浏览器移除 ${label}`, "info");
      if (result.activeChanged) {
        onRefresh();
      }
    } finally {
      setRemoving(false);
    }
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(300px,.7fr)_minmax(0,1fr)]">
      <div className="grid content-start gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <UserRoundIcon className="size-5" />
              个人资料
            </CardTitle>
            <CardDescription>当前浏览器正在使用的 OAIX 身份。</CardDescription>
          </CardHeader>
          <CardPanel>
            {activeAccount || profile ? (
              <div className="flex items-center gap-4">
                <div className="flex size-14 shrink-0 items-center justify-center rounded-full border bg-muted font-heading text-xl font-semibold">
                  {profileName.slice(0, 1).toUpperCase() || <UserRoundIcon />}
                </div>
                <div className="min-w-0">
                  <div className="truncate font-heading text-lg font-semibold">{profileName}</div>
                  {profile?.email && profile.display_name && <div className="truncate text-muted-foreground text-sm">{profile.email}</div>}
                  <div className="mt-2 flex flex-wrap gap-2">
                    <Badge variant="secondary">{roleLabel(role)}</Badge>
                    {profile?.status && <Badge variant="outline">{profile.status}</Badge>}
                    {profile?.id && <Badge variant="outline">用户 #{profile.id}</Badge>}
                  </div>
                </div>
              </div>
            ) : (
              <EmptyState compact title="尚未登录" description="添加一个 OAIX 账号后即可在此浏览器中保存并切换。" />
            )}
          </CardPanel>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <UserPlusIcon className="size-5" />
              添加账号
            </CardTitle>
            <CardDescription>登录另一个已有 OAIX 账号。浏览器只保存登录返回的 API Key，不会保存密码。</CardDescription>
          </CardHeader>
          <CardPanel className="grid gap-4">
            <div className="grid gap-2">
              <Label htmlFor="add-account-email">邮箱</Label>
              <Input
                autoComplete="email"
                id="add-account-email"
                nativeInput
                onChange={(event) => setEmail(event.currentTarget.value)}
                placeholder="name@example.com"
                type="email"
                value={email}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="add-account-password">密码</Label>
              <Input
                autoComplete="current-password"
                id="add-account-password"
                nativeInput
                onChange={(event) => setPassword(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    void addAccount();
                  }
                }}
                type="password"
                value={password}
              />
            </div>
            <Button className="w-fit" loading={adding} onClick={() => void addAccount()}>
              <LogInIcon />
              添加并切换
            </Button>
          </CardPanel>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ShieldCheckIcon className="size-5" />
            本浏览器账号
          </CardTitle>
          <CardDescription>已保存 {accounts.length} 个账号；存在两个或更多账号时，也可以从右上角头像菜单直接切换。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-3">
          {!orderedAccounts.length ? (
            <EmptyState compact title="暂无保存的账号" description="使用左侧表单登录后会显示在这里。" />
          ) : (
            orderedAccounts.map((account) => {
              const current = account.id === activeAccount?.id;
              const accountEmail = account.user?.email?.trim() || "";
              const accountName = savedAccountLabel(account);
              return (
                <div className="flex flex-col gap-3 rounded-lg border bg-muted/25 p-4 sm:flex-row sm:items-center" key={account.id}>
                  <div className="flex min-w-0 flex-1 items-center gap-3">
                    <div className="flex size-10 shrink-0 items-center justify-center rounded-full border bg-background font-medium">
                      {accountName.slice(0, 1).toUpperCase() || <UserRoundIcon />}
                    </div>
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="truncate font-medium">{accountName}</span>
                        {current && <Badge variant="secondary">当前账号</Badge>}
                      </div>
                      {accountEmail && accountName !== accountEmail && (
                        <div className="flex items-center gap-1 truncate text-muted-foreground text-sm">
                          <MailIcon className="size-3.5" />
                          {accountEmail}
                        </div>
                      )}
                      <div className="mt-1 text-muted-foreground text-xs">最近使用 {formatDate(account.lastUsedAt)}</div>
                    </div>
                  </div>
                  <div className="flex shrink-0 gap-2">
                    {!current && (
                      <Button onClick={() => switchAccount(account)} size="sm" variant="outline">
                        <CheckIcon />
                        切换
                      </Button>
                    )}
                    <Button onClick={() => setRemoveTarget(account)} size="sm" variant="destructive-outline">
                      <Trash2Icon />
                      移除
                    </Button>
                  </div>
                </div>
              );
            })
          )}
        </CardPanel>
      </Card>

      <Dialog open={Boolean(removeTarget)} onOpenChange={(open) => !open && !removing && setRemoveTarget(null)}>
        <DialogPopup className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>从本浏览器移除账号？</DialogTitle>
            <DialogDescription>
              将移除 {savedAccountLabel(removeTarget)} 的本地登录信息，不会删除用户，也不会撤销服务端 API Key。
              {removeTarget?.id === activeAccount?.id && accounts.length > 1 ? " 移除后会自动切换到另一个已保存账号。" : ""}
            </DialogDescription>
          </DialogHeader>
          <DialogPanel />
          <DialogFooter>
            <DialogClose render={<Button disabled={removing} variant="ghost" />}>取消</DialogClose>
            <Button loading={removing} onClick={confirmRemoveAccount} variant="destructive">
              <Trash2Icon />
              确认移除
            </Button>
          </DialogFooter>
        </DialogPopup>
      </Dialog>
    </div>
  );
}

function roleLabel(role: string): string {
  switch (role) {
    case "admin":
      return "管理员";
    case "readonly_admin":
      return "只读管理员";
    case "service":
      return "服务凭证";
    case "user":
      return "普通用户";
    default:
      return "已登录";
  }
}
