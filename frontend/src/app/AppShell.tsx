import {
  ActivityIcon,
  DatabaseIcon,
  KeyRoundIcon,
  LayoutDashboardIcon,
  ListFilterIcon,
  LogInIcon,
  LogOutIcon,
  MenuIcon,
  ChevronsUpDownIcon,
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
import {
  activateSavedAccount,
  getActiveSavedAccount,
  rememberPasswordAuthAccount,
  removeSavedAccount,
  savedAccountLabel,
  useSavedAccounts,
  type SavedAccount,
} from "@/lib/accounts";
import { api, getServiceKey, isAdminPrincipal, isServicePrincipal, setServiceKey, type MeResponse } from "@/lib/api";
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
      { key: "dashboard", href: "/dashboard", icon: <LayoutDashboardIcon />, label: "仪表盘" },
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
  loading,
  me,
  onRefresh,
  onThemeChange,
  protectedMode,
  routeKey,
  theme,
}: {
  authBlocked: boolean;
  children: React.ReactNode;
  loading: boolean;
  me: MeResponse | null;
  onRefresh: () => void;
  onThemeChange: (theme: ThemePreference) => void;
  protectedMode: boolean;
  routeKey: RouteKey;
  theme: ThemePreference;
}) {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(() => readSidebarCollapsed());
  const [credentialDialogOpen, setCredentialDialogOpen] = useState(false);
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [serviceKeyError, setServiceKeyError] = useState("");
  const [authMode, setAuthMode] = useState<"api_key" | "login" | "register">("login");
  const [emailDraft, setEmailDraft] = useState("");
  const [passwordDraft, setPasswordDraft] = useState("");
  const [displayNameDraft, setDisplayNameDraft] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const savedAccounts = useSavedAccounts();
  const activeSavedAccount = getActiveSavedAccount();
  // If a saved credential expires, keep the shell usable so the user can switch
  // to another saved account or open the profile page to replace it.
  const credentialRequired = authBlocked && protectedMode && savedAccounts.length === 0;
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

  // 服务端没有登出接口，这里只移除当前账号在本浏览器保存的 API Key；
  // 该 Key 在服务端依然有效，需要作废请到 API Key 页面删除。
  function logout() {
    if (activeSavedAccount) {
      removeSavedAccount(activeSavedAccount.id);
    } else {
      setServiceKey("");
    }
    onRefresh();
  }

  function switchAccount(id: string) {
    if (id === activeSavedAccount?.id || !activateSavedAccount(id)) {
      return;
    }
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
      rememberPasswordAuthAccount(key, result.user, result.api_key?.id);
      setPasswordDraft("");
      setCredentialDialogOpen(false);
      onRefresh();
    } catch (caught) {
      setServiceKeyError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setAuthBusy(false);
    }
  }

  function navigation(mobile = false) {
    return (
      <nav aria-label={mobile ? "移动导航" : "主导航"} className="grid content-start gap-6">
        {NAV_GROUPS.map((group) => {
          const items = group.items.filter((item) => !item.adminOnly || admin);
          if (!items.length) return null;
          const compact = collapsed && !mobile;
          return (
            <div className="grid gap-1" key={group.label}>
              <div className={cn("mb-1 px-3 text-muted-foreground text-xs", compact && "sr-only")}>{group.label}</div>
              {compact && <div className="mx-auto mb-2 h-px w-5 bg-border" />}
              {items.map((item) => {
                const active = routeKey === item.key ||
                  (routeKey === "key_detail" && item.key === "keys") ||
                  (routeKey === "import_new" && item.key === "imports") ||
                  (routeKey === "admin_pool_detail" && item.key === "admin_pools") ||
                  (routeKey === "admin_user_detail" && item.key === "admin_users");
                return (
                  <Button
                    aria-current={active ? "page" : undefined}
                    aria-label={item.label}
                    className={cn("h-9 justify-start gap-3 rounded-lg px-3 text-muted-foreground hover:text-foreground", active && "bg-sidebar-accent text-foreground", compact && "size-9 justify-center px-0")}
                    key={item.href}
                    onClick={() => { navigateTo(item.href); setMobileNavOpen(false); }}
                    size="sm"
                    title={compact ? item.label : undefined}
                    variant="ghost"
                  >
                    {item.icon}
                    {!compact && <span>{item.label}</span>}
                  </Button>
                );
              })}
            </div>
          );
        })}
      </nav>
    );
  }

  function accountMenu(compact: boolean) {
    return <AccountMenu accounts={savedAccounts} activeAccount={activeSavedAccount} compact={compact}
      me={me} onLogin={openLogin} onLogout={logout} onSwitchAccount={switchAccount}
      onThemeChange={onThemeChange} serviceOnly={serviceOnly} theme={theme} />;
  }

  return (
    <div className="min-h-screen bg-background text-foreground">
      <a className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-background focus:p-3 focus:ring-2" href="#main-content">跳转到内容</a>
      <div className="grid min-h-screen grid-cols-1 lg:grid-cols-[var(--oaix-sidebar-width)_minmax(0,1fr)]"
        style={{ "--oaix-sidebar-width": collapsed ? "68px" : "232px" } as React.CSSProperties}>
        <aside aria-label="侧边栏" className="sticky top-0 hidden h-dvh min-h-0 flex-col border-r bg-sidebar lg:flex">
          <div className={cn("flex h-20 shrink-0 items-center gap-3 px-5", collapsed && "h-auto flex-col gap-2 px-4 py-5")}>
            <a aria-label="oaix 首页" className="flex min-w-0 flex-1 items-center gap-3 rounded-md outline-none focus-visible:ring-2 focus-visible:ring-ring" href="/dashboard"
              onClick={(event) => { event.preventDefault(); navigateTo("/dashboard"); }}>
              <DatabaseIcon className="size-6 shrink-0" />
              {!collapsed && <span className="font-heading text-xl font-semibold tracking-tight">oaix</span>}
            </a>
            <Button aria-expanded={!collapsed} aria-label={collapsed ? "展开侧边栏" : "折叠侧边栏"}
              onClick={toggleSidebar} size="icon-sm" title={collapsed ? "展开侧边栏" : "折叠侧边栏"} variant="ghost">
              {collapsed ? <PanelLeftOpenIcon /> : <PanelLeftCloseIcon />}
            </Button>
          </div>
          <div className={cn("min-h-0 flex-1 overflow-y-auto px-3 pb-5 oaix-scrollbar", collapsed && "px-4")}>{navigation()}</div>
          <div className={cn("grid shrink-0 gap-2 border-t bg-background/30 p-3", collapsed && "justify-items-center px-4")}>
            <Button aria-label="刷新数据" className={cn("justify-start gap-3 text-muted-foreground", collapsed && "size-9 justify-center px-0")}
              onClick={onRefresh} size="sm" title="刷新数据" variant="ghost">
              <RefreshCwIcon className={cn(loading && "animate-spin motion-reduce:animate-none")} />
              {!collapsed && "刷新数据"}
            </Button>
            {accountMenu(collapsed)}
          </div>
        </aside>
        <div className="min-w-0">
          <header className="sticky top-0 z-20 flex h-16 items-center gap-3 border-b bg-background/95 px-4 backdrop-blur lg:hidden">
            <Button aria-label="打开导航" aria-expanded={mobileNavOpen} onClick={() => setMobileNavOpen(true)} size="icon-sm" variant="ghost"><MenuIcon /></Button>
            <a className="mr-auto font-heading text-lg font-semibold" href="/dashboard" onClick={(event) => { event.preventDefault(); navigateTo("/dashboard"); }}>oaix</a>
            <Button aria-label="刷新数据" onClick={onRefresh} size="icon-sm" variant="ghost"><RefreshCwIcon className={cn(loading && "animate-spin motion-reduce:animate-none")} /></Button>
            {accountMenu(true)}
          </header>
          <main id="main-content" tabIndex={-1} className="mx-auto min-w-0 max-w-[1600px] px-4 py-6 outline-none sm:px-6 lg:px-10 lg:py-9">
            {!protectedMode && me && <Badge className="mb-5" variant="warning">未启用服务侧凭证</Badge>}
            {authBlocked && (
              <Alert className="mb-6" variant="warning">
                <ShieldCheckIcon />
                <AlertTitle>需要登录</AlertTitle>
                <AlertDescription>普通用户请登录或注册；管理员可在弹窗里切换到管理员入口。</AlertDescription>
              </Alert>
            )}
            {children}
          </main>
        </div>
      </div>
      <Dialog open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
        <DialogPopup className="max-w-sm">
          <DialogHeader><DialogTitle>oaix 导航</DialogTitle></DialogHeader>
          <DialogPanel>{navigation(true)}</DialogPanel>
        </DialogPopup>
      </Dialog>
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

function principalRoleLabel(me: MeResponse | null, serviceOnly: boolean, activeAccount?: SavedAccount | null): string {
  const role = String(activeAccount?.role || activeAccount?.user?.role || me?.role || me?.user?.role || "").toLowerCase();
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
 * 头像菜单：身份、主题、登录/退出登录都收在这里，统一收在侧边栏底部和移动端工具栏。
 * 未登录时依然渲染（否则主题切换会没有入口），此时展示登录项。
 */
function AccountMenu({
  accounts,
  activeAccount,
  compact = true,
  me,
  onLogin,
  onLogout,
  onSwitchAccount,
  onThemeChange,
  serviceOnly,
  theme,
}: {
  accounts: SavedAccount[];
  activeAccount: SavedAccount | null;
  compact?: boolean;
  me: MeResponse | null;
  onLogin: () => void;
  onLogout: () => void;
  onSwitchAccount: (id: string) => void;
  onThemeChange: (theme: ThemePreference) => void;
  serviceOnly: boolean;
  theme: ThemePreference;
}) {
  const email = activeAccount?.user?.email?.trim() || me?.user?.email?.trim() || "";
  const name =
    activeAccount?.user?.display_name?.trim() ||
    me?.user?.display_name?.trim() ||
    email ||
    (activeAccount ? savedAccountLabel(activeAccount) : serviceOnly ? "Service API Key" : "当前会话");
  const initial = name.slice(0, 1).toUpperCase();
  const signedIn = Boolean(me || activeAccount);
  return (
    <Menu>
      <MenuTrigger
        aria-label={signedIn ? `账户菜单：${name}` : "账户菜单"}
        render={<Button className={cn("shrink-0 transition-colors", compact ? "rounded-full" : "h-auto w-full justify-start gap-3 rounded-lg border border-transparent bg-background/45 px-2 py-2 hover:border-border hover:bg-background/80")} size={compact ? "icon" : "default"} variant="ghost" />}
        title={signedIn ? name : "未登录"}
      >
        <span className="flex size-8 shrink-0 items-center justify-center rounded-full border bg-primary/10 text-sm font-semibold text-primary">
          {signedIn && initial ? initial : <UserRoundIcon className="size-4" />}
        </span>
        {!compact && <><span className="grid min-w-0 flex-1 gap-0.5 text-left"><span className="truncate text-sm font-medium">{signedIn ? name : "登录账户"}</span><span className="text-muted-foreground text-xs">{signedIn ? principalRoleLabel(me, serviceOnly, activeAccount) : "主题与账户"}</span></span><ChevronsUpDownIcon className="size-4 shrink-0 text-muted-foreground" /></>}
      </MenuTrigger>
      <MenuPopup>
        {signedIn && (
          <>
            <div className="grid gap-0.5 px-2 py-1.5">
              <div className="truncate font-medium text-sm" title={name}>
                {name}
              </div>
              <div className="text-muted-foreground text-xs">{principalRoleLabel(me, serviceOnly, activeAccount)}</div>
            </div>
            <MenuSeparator />
          </>
        )}
        {accounts.length > 1 && (
          <>
            <MenuRadioGroup onValueChange={onSwitchAccount} value={activeAccount?.id || ""}>
              <MenuGroupLabel>切换账号</MenuGroupLabel>
              {accounts.map((account) => (
                <MenuRadioItem key={account.id} value={account.id}>
                  <span className="grid max-w-56" title={account.user?.email || savedAccountLabel(account)}>
                    <span className="truncate">{savedAccountLabel(account)}</span>
                    {account.user?.email && account.user.email !== savedAccountLabel(account) && (
                      <span className="truncate text-muted-foreground text-xs">{account.user.email}</span>
                    )}
                  </span>
                </MenuRadioItem>
              ))}
            </MenuRadioGroup>
            <MenuSeparator />
          </>
        )}
        <MenuItem onClick={() => navigateTo("/account/profile")}>
          <UserRoundIcon />
          个人资料
        </MenuItem>
        <MenuSeparator />
        <MenuRadioGroup onValueChange={(value) => onThemeChange(value as ThemePreference)} value={theme}>
          <MenuGroupLabel>主题</MenuGroupLabel>
          <MenuRadioItem value="auto">自动</MenuRadioItem>
          <MenuRadioItem value="light">亮色</MenuRadioItem>
          <MenuRadioItem value="dark">暗色</MenuRadioItem>
        </MenuRadioGroup>
        <MenuSeparator />
        {signedIn ? (
          <MenuItem className="text-destructive-foreground" onClick={onLogout}>
            <LogOutIcon />
            退出当前账号
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
