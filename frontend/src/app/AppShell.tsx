import {
  ActivityIcon,
  DatabaseIcon,
  KeyRoundIcon,
  ListFilterIcon,
  RefreshCwIcon,
  SaveIcon,
  SendIcon,
  Settings2Icon,
  ShieldCheckIcon,
  UploadIcon,
  UserRoundIcon,
  UsersRoundIcon,
} from "lucide-react";
import { useEffect, useState } from "react";
import type * as React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { Dialog, DialogDescription, DialogFooter, DialogHeader, DialogPanel, DialogPopup, DialogTitle } from "@/registry/default/ui/dialog";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { cn } from "@/registry/default/lib/utils";
import { api, getServiceKey, isAdminPrincipal, isServicePrincipal, setServiceKey, type HealthResponse, type MeResponse, type TokenCounts } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import { ThemeButton } from "@/shared/components";
import type { RouteKey, ThemePreference } from "@/shared/types";
import { navigateTo } from "./router";

type NavItem = {
  adminOnly?: boolean;
  key: RouteKey;
  href: string;
  icon: React.ReactNode;
  label: string;
};

const NAV_GROUPS: Array<{ label: string; items: NavItem[] }> = [
  {
    label: "账号",
    items: [
      { key: "account", href: "/account", icon: <UserRoundIcon />, label: "我的账号" },
      { key: "account_api_keys", href: "/account/api-keys", icon: <ShieldCheckIcon />, label: "我的 API Key" },
    ],
  },
  {
    label: "用户",
    items: [
      { key: "keys", href: "/keys?status=available", icon: <KeyRoundIcon />, label: "Key" },
      { key: "imports", href: "/imports", icon: <UploadIcon />, label: "导入" },
      { key: "requests", href: "/requests", icon: <ListFilterIcon />, label: "请求" },
    ],
  },
  {
    label: "管理员",
    items: [
      { adminOnly: true, key: "admin_users", href: "/admin/users", icon: <UsersRoundIcon />, label: "用户状态" },
      { adminOnly: true, key: "admin_pools", href: "/admin/pools", icon: <DatabaseIcon />, label: "号池总览" },
      { adminOnly: true, key: "admin_requests", href: "/admin/requests", icon: <ListFilterIcon />, label: "全局请求" },
      { adminOnly: true, key: "admin_audit", href: "/admin/audit", icon: <ShieldCheckIcon />, label: "审计" },
      { adminOnly: true, key: "admin_sub2api", href: "/admin/sub2api", icon: <SendIcon />, label: "Sub2API" },
      { adminOnly: true, key: "settings", href: "/settings", icon: <Settings2Icon />, label: "设置" },
      { adminOnly: true, key: "runtime", href: "/runtime", icon: <ActivityIcon />, label: "运行" },
    ],
  },
];

export function AppShell({
  authBlocked,
  children,
  counts,
  health,
  loading,
  me,
  onRefresh,
  onThemeChange,
  protectedMode,
  routeKey,
  syncText,
  theme,
  webVersion,
}: {
  authBlocked: boolean;
  children: React.ReactNode;
  counts: TokenCounts;
  health: HealthResponse | null;
  loading: boolean;
  me: MeResponse | null;
  onRefresh: () => void;
  onThemeChange: (theme: ThemePreference) => void;
  protectedMode: boolean;
  routeKey: RouteKey;
  syncText: string;
  theme: ThemePreference;
  webVersion?: { hash: string; time: string };
}) {
  const available = counts.available ?? counts.active ?? 0;
  const [credentialDialogOpen, setCredentialDialogOpen] = useState(false);
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [serviceKeyError, setServiceKeyError] = useState("");
  const [authMode, setAuthMode] = useState<"api_key" | "login" | "register">("api_key");
  const [emailDraft, setEmailDraft] = useState("");
  const [passwordDraft, setPasswordDraft] = useState("");
  const [displayNameDraft, setDisplayNameDraft] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const credentialRequired = authBlocked && protectedMode;
  const credentialOpen = credentialRequired || credentialDialogOpen;
  const admin = isAdminPrincipal(me);
  const serviceOnly = Boolean(me && isServicePrincipal(me) && !me.user?.id);
  const principalSubtitle = me?.user?.email || (serviceOnly ? "Service API Key" : protectedMode ? "API Key required" : "local");

  useEffect(() => {
    if (credentialRequired) {
      setServiceKeyDraft(getServiceKey());
      setServiceKeyError("");
      setCredentialDialogOpen(true);
    }
  }, [credentialRequired]);

  function changeCredentialDialogOpen(open: boolean) {
    if (credentialRequired) {
      setCredentialDialogOpen(true);
      return;
    }
    setCredentialDialogOpen(open);
  }

  function saveCredential() {
    const key = serviceKeyDraft.trim();
    if (!key) {
      setServiceKeyError("请先填写 API Key。");
      return;
    }
    setServiceKey(key);
    setServiceKeyError("");
    setCredentialDialogOpen(false);
    onRefresh();
  }

  async function submitPasswordAuth(kind: "login" | "register") {
    if (!emailDraft.trim() || !passwordDraft) {
      setServiceKeyError("请填写邮箱和密码。");
      return;
    }
    setAuthBusy(true);
    setServiceKeyError("");
    try {
      const result = kind === "login"
        ? await api.login({ email: emailDraft.trim(), password: passwordDraft, name: "web" })
        : await api.register({ email: emailDraft.trim(), password: passwordDraft, display_name: displayNameDraft.trim() });
      const key = result.api_key?.plaintext_key || result.api_key?.value || "";
      if (!key) {
        throw new Error("服务端没有返回一次性 API Key");
      }
      setServiceKey(key);
      setCredentialDialogOpen(false);
      onRefresh();
    } catch (caught) {
      setServiceKeyError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setAuthBusy(false);
    }
  }

  return (
    <div className="min-h-screen text-foreground">
      <div className="mx-auto grid min-h-screen w-full max-w-[1600px] grid-cols-1 gap-0 px-3 py-3 lg:grid-cols-[220px_minmax(0,1fr)] lg:px-4">
        <aside className="sticky top-3 z-20 mb-3 h-fit rounded-lg border bg-card/90 p-3 shadow-xs/5 backdrop-blur lg:mb-0 lg:min-h-[calc(100vh-1.5rem)]">
          <div className="flex items-center gap-2 px-2 py-2">
            <div className="flex size-9 items-center justify-center rounded-md border bg-muted">
              <DatabaseIcon className="size-4" />
            </div>
            <div className="min-w-0">
              <div className="font-heading text-lg font-semibold leading-none">oaix</div>
              <div className="mt-1 truncate text-muted-foreground text-xs">{principalSubtitle}</div>
            </div>
          </div>
          <nav className="mt-4 grid gap-4">
            {NAV_GROUPS.map((group) => {
              const items = group.items.filter((item) => !item.adminOnly || admin).filter((item) => !(serviceOnly && item.key === "account_api_keys"));
              if (!items.length) {
                return null;
              }
              return (
                <div className="grid gap-1" key={group.label}>
                  <div className="px-2 text-muted-foreground text-xs">{serviceOnly && group.label === "账号" ? "平台" : group.label}</div>
                  <div className="flex gap-1 overflow-x-auto lg:grid lg:overflow-visible">
                    {items.map((item) => {
                      const active =
                        routeKey === item.key ||
                        (routeKey === "key_detail" && item.key === "keys") ||
                        (routeKey === "import_new" && item.key === "imports") ||
                        (routeKey === "admin_user_detail" && item.key === "admin_users");
                      return (
                        <Button
                          className={cn("justify-start", active && "bg-secondary")}
                          key={item.href}
                          onClick={() => navigateTo(item.href)}
                          size="sm"
                          variant={active ? "secondary" : "ghost"}
                        >
                          {item.icon}
                          {serviceOnly && item.key === "account" ? "平台凭证" : item.label}
                        </Button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </nav>
          <div className="mt-4 hidden gap-2 border-t pt-4 text-xs text-muted-foreground lg:grid">
            <div className="flex items-center justify-between">
              <span>有效</span>
              <span className="oaix-tabular">{formatNumber(available)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span>冷却</span>
              <span className="oaix-tabular">{formatNumber(counts.cooling)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span>禁用</span>
              <span className="oaix-tabular">{formatNumber(counts.disabled)}</span>
            </div>
          </div>
        </aside>

        <div className="min-w-0 lg:pl-4">
          <header className="mb-4 rounded-lg border bg-card/90 p-4 shadow-xs/5 backdrop-blur">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div className="min-w-0">
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <Badge variant={protectedMode ? "success" : "warning"}>
                    {protectedMode ? "API Key 已启用" : "未启用服务侧凭证"}
                  </Badge>
                  <Badge variant={health?.ok === false ? "warning" : "secondary"}>{syncText}</Badge>
                  <Badge variant="outline">总量 {formatNumber(counts.total)}</Badge>
                </div>
                <h1 className="font-heading text-2xl font-semibold tracking-normal">Key 池控制台</h1>
              </div>
              <div className="flex flex-wrap items-center gap-2 md:justify-end">
                {protectedMode && (
                  <Button
                    onClick={() => {
                      setServiceKeyDraft(getServiceKey());
                      setServiceKeyError("");
                      setCredentialDialogOpen(true);
                    }}
                    size="sm"
                    variant="outline"
                  >
                    <ShieldCheckIcon />
                    API Key
                  </Button>
                )}
                <ThemeButton value="auto" current={theme} onSelect={onThemeChange} />
                <ThemeButton value="light" current={theme} onSelect={onThemeChange} />
                <ThemeButton value="dark" current={theme} onSelect={onThemeChange} />
                <Button onClick={onRefresh} variant="outline">
                  <RefreshCwIcon className={cn(loading && "animate-spin")} />
                  刷新
                </Button>
              </div>
            </div>
          </header>

          {authBlocked && (
            <Alert className="mb-4" variant="warning">
              <ShieldCheckIcon />
              <AlertTitle>需要 API Key</AlertTitle>
              <AlertDescription>请先填写 API Key，或使用邮箱密码登录/注册后继续。</AlertDescription>
            </Alert>
          )}

          <main className="min-w-0">{children}</main>

          <footer className="flex flex-wrap items-center justify-between gap-2 py-4 text-muted-foreground text-xs">
            <span>oaix platform</span>
            <span title={`资源版本 ${webVersion?.hash || "-"}`}>前端版本 {webVersion?.time || "-"}</span>
          </footer>
        </div>
      </div>
      <Dialog open={credentialOpen} onOpenChange={changeCredentialDialogOpen}>
        <DialogPopup className="sm:max-w-md" showCloseButton={!credentialRequired}>
          <DialogHeader>
            <DialogTitle>连接 oaix</DialogTitle>
            <DialogDescription>
              使用已有 API Key，或用邮箱密码登录/注册。注册只需要邮箱和密码。
            </DialogDescription>
          </DialogHeader>
          <DialogPanel className="grid gap-2">
            <div className="flex rounded-lg bg-muted p-1">
              {[
                ["api_key", "API Key"],
                ["login", "登录"],
                ["register", "注册"],
              ].map(([value, label]) => (
                <Button className="flex-1" key={value} onClick={() => setAuthMode(value as typeof authMode)} size="sm" variant={authMode === value ? "secondary" : "ghost"}>
                  {label}
                </Button>
              ))}
            </div>
            {authMode === "api_key" ? (
              <>
                <Label htmlFor="global-service-key">API Key</Label>
                <Input
                  autoFocus
                  id="global-service-key"
                  nativeInput
                  onChange={(event) => {
                    setServiceKeyDraft(event.currentTarget.value);
                    if (serviceKeyError) {
                      setServiceKeyError("");
                    }
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      saveCredential();
                    }
                  }}
                  placeholder="oaix_user_... / oaix_service_..."
                  type="password"
                  value={serviceKeyDraft}
                />
              </>
            ) : (
              <div className="grid gap-3">
                <div className="grid gap-2">
                  <Label htmlFor="auth-email">邮箱</Label>
                  <Input id="auth-email" nativeInput onChange={(event) => setEmailDraft(event.currentTarget.value)} type="email" value={emailDraft} />
                </div>
                {authMode === "register" && (
                  <div className="grid gap-2">
                    <Label htmlFor="auth-name">显示名</Label>
                    <Input id="auth-name" nativeInput onChange={(event) => setDisplayNameDraft(event.currentTarget.value)} value={displayNameDraft} />
                  </div>
                )}
                <div className="grid gap-2">
                  <Label htmlFor="auth-password">密码</Label>
                  <Input id="auth-password" nativeInput onChange={(event) => setPasswordDraft(event.currentTarget.value)} type="password" value={passwordDraft} />
                </div>
              </div>
            )}
            {serviceKeyError && <div className="text-destructive-foreground text-sm">{serviceKeyError}</div>}
          </DialogPanel>
          <DialogFooter>
            {!credentialRequired && (
              <Button onClick={() => setCredentialDialogOpen(false)} variant="ghost">
                取消
              </Button>
            )}
            {authMode === "api_key" ? (
            <Button onClick={saveCredential}>
              <SaveIcon />
              保存并同步
            </Button>
            ) : (
              <Button loading={authBusy} onClick={() => void submitPasswordAuth(authMode)}>
                <SaveIcon />
                {authMode === "login" ? "登录并同步" : "注册并同步"}
              </Button>
            )}
          </DialogFooter>
        </DialogPopup>
      </Dialog>
    </div>
  );
}
