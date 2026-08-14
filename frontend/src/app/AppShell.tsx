import {
  ActivityIcon,
  DatabaseIcon,
  KeyRoundIcon,
  ListFilterIcon,
  LogInIcon,
  LogOutIcon,
  PanelLeftCloseIcon,
  PanelLeftOpenIcon,
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
import { Menu, MenuGroupLabel, MenuItem, MenuPopup, MenuRadioGroup, MenuRadioItem, MenuSeparator, MenuTrigger } from "@/registry/default/ui/menu";
import { cn } from "@/registry/default/lib/utils";
import { api, getServiceKey, isAdminPrincipal, isServicePrincipal, setServiceKey, type HealthResponse, type MeResponse, type TokenCounts } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import { readSidebarCollapsed, writeSidebarCollapsed } from "@/shared/domain";
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
    label: "用户",
    items: [
      { key: "keys", href: "/keys?status=available", icon: <KeyRoundIcon />, label: "Key" },
      { key: "account_api_keys", href: "/account/api-keys", icon: <ShieldCheckIcon />, label: "API Key" },
      { key: "imports", href: "/imports", icon: <UploadIcon />, label: "导入" },
      { key: "requests", href: "/requests", icon: <ListFilterIcon />, label: "请求" },
      { key: "user_settings", href: "/account/settings", icon: <Settings2Icon />, label: "设置" },
    ],
  },
  {
    label: "管理员",
    items: [
      { adminOnly: true, key: "admin_users", href: "/admin/users", icon: <UsersRoundIcon />, label: "用户状态" },
      { adminOnly: true, key: "admin_pools", href: "/admin/pools", icon: <DatabaseIcon />, label: "号池总览" },
      { adminOnly: true, key: "admin_imports", href: "/admin/imports", icon: <UploadIcon />, label: "全局导入" },
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
  const [collapsed, setCollapsed] = useState(() => readSidebarCollapsed());
  const [credentialDialogOpen, setCredentialDialogOpen] = useState(false);
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [serviceKeyError, setServiceKeyError] = useState("");
  const [authMode, setAuthMode] = useState<"api_key" | "login" | "register">("login");
  const [emailDraft, setEmailDraft] = useState("");
  const [passwordDraft, setPasswordDraft] = useState("");
  const [displayNameDraft, setDisplayNameDraft] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const credentialRequired = authBlocked && protectedMode;
  const credentialOpen = credentialRequired || credentialDialogOpen;
  const admin = isAdminPrincipal(me);
  const serviceOnly = Boolean(me && isServicePrincipal(me) && !me.user?.id);

  useEffect(() => {
    if (credentialRequired) {
      setServiceKeyDraft(getServiceKey());
      setServiceKeyError("");
      setAuthMode("login");
      setCredentialDialogOpen(true);
    }
  }, [credentialRequired]);

  function toggleSidebar() {
    const next = !collapsed;
    setCollapsed(next);
    writeSidebarCollapsed(next);
  }

  function openLogin() {
    setServiceKeyDraft(getServiceKey());
    setServiceKeyError("");
    setAuthMode("login");
    setCredentialDialogOpen(true);
  }

  // 服务端没有登出接口，这里只能清掉本地保存的 API Key；
  // 该 Key 在服务端依然有效，需要作废请到设置页轮换。
  function logout() {
    setServiceKey("");
    onRefresh();
  }

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
      <div
        className="mx-auto grid min-h-screen w-full max-w-[1600px] grid-cols-1 gap-0 px-3 py-3 transition-[grid-template-columns] duration-200 ease-out lg:grid-cols-[var(--oaix-sidebar-width)_minmax(0,1fr)] lg:px-4"
        style={{ "--oaix-sidebar-width": collapsed ? "68px" : "220px" } as React.CSSProperties}
      >
        <aside className="sticky top-3 z-20 mb-3 h-fit rounded-lg border bg-card/90 p-3 shadow-xs/5 backdrop-blur lg:mb-0 lg:min-h-[calc(100vh-1.5rem)]">
          <div className={cn("flex items-center gap-2 px-2 py-2", collapsed && "lg:flex-col lg:gap-3 lg:px-0")}>
            <div className="flex size-9 shrink-0 items-center justify-center rounded-md border bg-muted">
              <DatabaseIcon className="size-4" />
            </div>
            <div className={cn("min-w-0 flex-1 font-heading text-lg font-semibold leading-none", collapsed && "lg:hidden")}>oaix</div>
            <Button
              aria-expanded={!collapsed}
              aria-label={collapsed ? "展开侧边栏" : "折叠侧边栏"}
              className="hidden lg:inline-flex"
              onClick={toggleSidebar}
              size="icon-sm"
              title={collapsed ? "展开侧边栏" : "折叠侧边栏"}
              variant="ghost"
            >
              {collapsed ? <PanelLeftOpenIcon /> : <PanelLeftCloseIcon />}
            </Button>
          </div>
          <nav className="mt-4 grid gap-4">
            {NAV_GROUPS.map((group) => {
              const items = group.items.filter((item) => !item.adminOnly || admin);
              if (!items.length) {
                return null;
              }
              return (
                <div className="grid gap-1" key={group.label}>
                  <div className={cn("px-2 text-muted-foreground text-xs", collapsed && "lg:hidden")}>{group.label}</div>
                  {collapsed && <div className="mx-auto mb-1 hidden h-px w-6 bg-border lg:block" />}
                  <div className={cn("flex gap-1 overflow-x-auto lg:grid lg:overflow-visible", collapsed && "lg:justify-items-center")}>
                    {items.map((item) => {
                      const active =
                        routeKey === item.key ||
                        (routeKey === "key_detail" && item.key === "keys") ||
                        (routeKey === "import_new" && item.key === "imports") ||
                        (routeKey === "admin_pool_detail" && item.key === "admin_pools") ||
                        (routeKey === "admin_user_detail" && item.key === "admin_users");
                      return (
                        <Button
                          className={cn("justify-start", active && "bg-secondary", collapsed && "lg:size-9 lg:justify-center lg:px-0")}
                          key={item.href}
                          onClick={() => navigateTo(item.href)}
                          size="sm"
                          title={collapsed ? item.label : undefined}
                          variant={active ? "secondary" : "ghost"}
                        >
                          {item.icon}
                          <span className={cn(collapsed && "lg:hidden")}>{item.label}</span>
                        </Button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </nav>
          <div className={cn("mt-4 hidden gap-2 border-t pt-4 text-xs text-muted-foreground lg:grid", collapsed && "lg:gap-2.5")}>
            {[
              { label: "有效", value: available },
              { label: "冷却", value: counts.cooling },
              { label: "禁用", value: counts.disabled },
            ].map((row) => (
              <div
                className={cn("flex items-center justify-between", collapsed && "lg:flex-col lg:justify-center lg:gap-0.5 lg:text-[11px]")}
                key={row.label}
                title={collapsed ? `${row.label} ${formatNumber(row.value)}` : undefined}
              >
                <span>{row.label}</span>
                <span className="oaix-tabular">{formatNumber(row.value)}</span>
              </div>
            ))}
          </div>
        </aside>

        <div className="min-w-0 lg:pl-4">
          <header className="mb-4 rounded-lg border bg-card/90 p-4 shadow-xs/5 backdrop-blur">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div className="min-w-0">
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  {!protectedMode && <Badge variant="warning">未启用服务侧凭证</Badge>}
                  <Badge variant={health?.ok === false ? "warning" : "secondary"}>{syncText}</Badge>
                  <Badge variant="outline">总量 {formatNumber(counts.total)}</Badge>
                </div>
                <h1 className="font-heading text-2xl font-semibold tracking-normal">Key 池控制台</h1>
              </div>
              <div className="flex flex-wrap items-center gap-2 md:justify-end">
                <Button onClick={onRefresh} variant="outline">
                  <RefreshCwIcon className={cn(loading && "animate-spin")} />
                  刷新
                </Button>
                <AccountMenu
                  me={me}
                  onLogin={openLogin}
                  onLogout={logout}
                  onThemeChange={onThemeChange}
                  serviceOnly={serviceOnly}
                  theme={theme}
                />
              </div>
            </div>
          </header>

          {authBlocked && (
            <Alert className="mb-4" variant="warning">
              <ShieldCheckIcon />
              <AlertTitle>需要登录</AlertTitle>
              <AlertDescription>普通用户请登录或注册；管理员可在弹窗里切换到管理员入口。</AlertDescription>
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
            <DialogTitle>登录 oaix</DialogTitle>
            <DialogDescription>
              普通用户使用邮箱密码登录或注册；管理员可使用 API Key 进入。
            </DialogDescription>
          </DialogHeader>
          <DialogPanel className="grid gap-2">
            <div className="flex rounded-lg bg-muted p-1">
              {[
                ["login", "登录"],
                ["register", "注册"],
                ["api_key", "管理员入口"],
              ].map(([value, label]) => (
                <Button
                  className="flex-1"
                  key={value}
                  onClick={() => {
                    setServiceKeyError("");
                    setAuthMode(value as typeof authMode);
                  }}
                  size="sm"
                  variant={authMode === value ? "secondary" : "ghost"}
                >
                  {label}
                </Button>
              ))}
            </div>
            {authMode === "api_key" ? (
              <>
                <Label htmlFor="global-service-key">管理员 API Key</Label>
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
                  placeholder="oaix_service_..."
                  type="password"
                  value={serviceKeyDraft}
                />
              </>
            ) : (
              <div className="grid gap-3">
                <div className="grid gap-2">
                  <Label htmlFor="auth-email">邮箱</Label>
                  <Input autoFocus id="auth-email" nativeInput onChange={(event) => setEmailDraft(event.currentTarget.value)} type="email" value={emailDraft} />
                </div>
                {authMode === "register" && (
                  <div className="grid gap-2">
                    <Label htmlFor="auth-name">显示名</Label>
                    <Input id="auth-name" nativeInput onChange={(event) => setDisplayNameDraft(event.currentTarget.value)} value={displayNameDraft} />
                  </div>
                )}
                <div className="grid gap-2">
                  <Label htmlFor="auth-password">密码</Label>
                  <Input
                    id="auth-password"
                    nativeInput
                    onChange={(event) => setPasswordDraft(event.currentTarget.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        void submitPasswordAuth(authMode);
                      }
                    }}
                    type="password"
                    value={passwordDraft}
                  />
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
                使用 API Key 进入
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

function principalRoleLabel(me: MeResponse | null, serviceOnly: boolean): string {
  const role = String(me?.role || me?.user?.role || "").toLowerCase();
  if (role === "admin") {
    return "管理员";
  }
  if (role === "readonly_admin") {
    return "只读管理员";
  }
  if (role === "service" || serviceOnly) {
    return "服务凭证";
  }
  if (role === "user") {
    return "普通用户";
  }
  return "已登录";
}

/**
 * 头像菜单：身份、主题、登录/退出登录都收在这里，避免头部平铺一排按钮。
 * 未登录时依然渲染（否则主题切换会没有入口），此时展示登录项。
 */
function AccountMenu({
  me,
  onLogin,
  onLogout,
  onThemeChange,
  serviceOnly,
  theme,
}: {
  me: MeResponse | null;
  onLogin: () => void;
  onLogout: () => void;
  onThemeChange: (theme: ThemePreference) => void;
  serviceOnly: boolean;
  theme: ThemePreference;
}) {
  const email = me?.user?.email?.trim() || "";
  const name = email || (serviceOnly ? "Service API Key" : "当前会话");
  const initial = email.slice(0, 1).toUpperCase();
  return (
    <Menu>
      <MenuTrigger
        aria-label={me ? `账户菜单：${name}` : "账户菜单"}
        render={<Button className="rounded-full before:rounded-full" size="icon" variant="outline" />}
        title={me ? name : "未登录"}
      >
        {me && initial ? <span className="font-medium text-sm">{initial}</span> : me ? <ShieldCheckIcon /> : <UserRoundIcon />}
      </MenuTrigger>
      <MenuPopup>
        {me && (
          <>
            <div className="grid gap-0.5 px-2 py-1.5">
              <div className="truncate font-medium text-sm" title={name}>
                {name}
              </div>
              <div className="text-muted-foreground text-xs">{principalRoleLabel(me, serviceOnly)}</div>
            </div>
            <MenuSeparator />
          </>
        )}
        <MenuRadioGroup onValueChange={(value) => onThemeChange(value as ThemePreference)} value={theme}>
          <MenuGroupLabel>主题</MenuGroupLabel>
          <MenuRadioItem value="auto">自动</MenuRadioItem>
          <MenuRadioItem value="light">亮色</MenuRadioItem>
          <MenuRadioItem value="dark">暗色</MenuRadioItem>
        </MenuRadioGroup>
        <MenuSeparator />
        {me ? (
          <MenuItem className="text-destructive-foreground" onClick={onLogout}>
            <LogOutIcon />
            退出登录
          </MenuItem>
        ) : (
          <MenuItem onClick={onLogin}>
            <LogInIcon />
            登录
          </MenuItem>
        )}
      </MenuPopup>
    </Menu>
  );
}
