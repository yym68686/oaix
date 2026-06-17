import {
  ActivityIcon,
  DatabaseIcon,
  KeyRoundIcon,
  ListFilterIcon,
  RefreshCwIcon,
  Settings2Icon,
  ShieldCheckIcon,
  UploadIcon,
} from "lucide-react";
import type * as React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Badge } from "@/registry/default/ui/badge";
import { Button } from "@/registry/default/ui/button";
import { cn } from "@/registry/default/lib/utils";
import type { HealthResponse, TokenCounts } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import { ThemeButton } from "@/shared/components";
import type { RouteKey, ThemePreference } from "@/shared/types";
import { navigateTo } from "./router";

type NavItem = {
  key: RouteKey;
  href: string;
  icon: React.ReactNode;
  label: string;
};

const NAV_ITEMS: NavItem[] = [
  { key: "keys", href: "/keys?status=available", icon: <KeyRoundIcon />, label: "Key" },
  { key: "imports", href: "/imports", icon: <UploadIcon />, label: "导入" },
  { key: "requests", href: "/requests", icon: <ListFilterIcon />, label: "请求" },
  { key: "settings", href: "/settings", icon: <Settings2Icon />, label: "设置" },
  { key: "runtime", href: "/runtime", icon: <ActivityIcon />, label: "运行" },
];

export function AppShell({
  authBlocked,
  children,
  counts,
  health,
  loading,
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
  onRefresh: () => void;
  onThemeChange: (theme: ThemePreference) => void;
  protectedMode: boolean;
  routeKey: RouteKey;
  syncText: string;
  theme: ThemePreference;
  webVersion?: { hash: string; time: string };
}) {
  const available = counts.available ?? counts.active ?? 0;
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
              <div className="mt-1 truncate text-muted-foreground text-xs">admin console</div>
            </div>
          </div>
          <nav className="mt-4 flex gap-1 overflow-x-auto lg:grid lg:overflow-visible">
            {NAV_ITEMS.map((item) => {
              const active = routeKey === item.key || (routeKey === "key_detail" && item.key === "keys") || (routeKey === "import_new" && item.key === "imports");
              return (
                <Button
                  className={cn("justify-start", active && "bg-secondary")}
                  key={item.href}
                  onClick={() => navigateTo(item.href)}
                  size="sm"
                  variant={active ? "secondary" : "ghost"}
                >
                  {item.icon}
                  {item.label}
                </Button>
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
                    {protectedMode ? "Service API Key 已启用" : "未启用服务侧凭证"}
                  </Badge>
                  <Badge variant={health?.ok === false ? "warning" : "secondary"}>{syncText}</Badge>
                  <Badge variant="outline">总量 {formatNumber(counts.total)}</Badge>
                </div>
                <h1 className="font-heading text-2xl font-semibold tracking-normal">Key 池控制台</h1>
              </div>
              <div className="flex flex-wrap items-center gap-2 md:justify-end">
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
              <AlertTitle>管理接口需要 Service API Key</AlertTitle>
              <AlertDescription>请进入导入页保存凭证，然后刷新管理数据。</AlertDescription>
            </Alert>
          )}

          <main className="min-w-0">{children}</main>

          <footer className="flex flex-wrap items-center justify-between gap-2 py-4 text-muted-foreground text-xs">
            <span>oaix admin console</span>
            <span title={`资源版本 ${webVersion?.hash || "-"}`}>前端版本 {webVersion?.time || "-"}</span>
          </footer>
        </div>
      </div>
    </div>
  );
}
