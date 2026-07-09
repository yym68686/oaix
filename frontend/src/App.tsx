import { useCallback, useEffect, useState } from "react";
import { AppShell } from "@/app/AppShell";
import { useRouteState } from "@/app/router";
import { ImportsPage } from "@/features/imports/ImportsPage";
import { KeyDetailPage, KeysPage } from "@/features/keys/KeysPage";
import { RequestsPage } from "@/features/requests/RequestsPage";
import { RuntimePage } from "@/features/runtime/RuntimePage";
import { SettingsPage, UserSettingsPage } from "@/features/settings/SettingsPage";
import { AdminAuditPage, AdminImportsPage, AdminPoolsPage, AdminRequestsPage, AdminUserDetailPage, AdminUsersPage } from "@/features/admin/AdminPages";
import { AdminSub2APIPage } from "@/features/admin/Sub2APIPage";
import { api, hasServiceKey, isAdminPrincipal, isSelfUserMode, setAuthContext, type HealthResponse, type MeResponse, type TokenCounts } from "@/lib/api";
import { applyTheme, errorMessage, readThemePreference } from "@/shared/domain";
import { EmptyState, ToastStack } from "@/shared/components";
import type { ThemePreference, ToastMessage } from "@/shared/types";

declare global {
  interface Window {
    __OAIX_WEB_VERSION__?: {
      hash: string;
      time: string;
    };
  }
}

export function App(): React.ReactElement {
  const route = useRouteState();
  const [theme, setTheme] = useState<ThemePreference>(() => readThemePreference());
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [counts, setCounts] = useState<TokenCounts>({});
  const [protectedMode, setProtectedMode] = useState(false);
  const [authBlocked, setAuthBlocked] = useState(false);
  const [me, setMe] = useState<MeResponse | null>(null);
  const [syncText, setSyncText] = useState("等待同步");
  const [loading, setLoading] = useState(true);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [streamCap, setStreamCap] = useState(10);
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const webVersion = window.__OAIX_WEB_VERSION__;

  const pushToast = useCallback((title: string, variant: ToastMessage["variant"] = "success") => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setToasts((items) => [...items.slice(-3), { id, title, variant }]);
    window.setTimeout(() => {
      setToasts((items) => items.filter((item) => item.id !== id));
    }, 4200);
  }, []);

  const refreshAll = useCallback(async () => {
    setLoading(true);
    try {
      const healthPayload = await api.health();
      setHealth(healthPayload);
      setCounts(healthPayload.counts || {});
      setProtectedMode(Boolean(healthPayload.service_key_protected));
      if (healthPayload.service_key_protected && !hasServiceKey()) {
        setAuthContext(null);
        setMe(null);
        setAuthBlocked(true);
        setSyncText(`需要凭证 · ${new Date().toLocaleTimeString("zh-CN")}`);
        return;
      }
      let mePayload: MeResponse | null = null;
      if (hasServiceKey()) {
        mePayload = await api.me();
        setAuthContext(mePayload);
        setMe(mePayload);
      } else {
        setAuthContext(null);
        setMe(null);
      }
      setAuthBlocked(false);
      if (mePayload && !isAdminPrincipal(mePayload) && isSelfUserMode()) {
        try {
          const pool = await api.myPoolSummary();
          setCounts(pool.counts || {});
        } catch {}
      }
      if (!mePayload || isAdminPrincipal(mePayload)) {
        try {
          const selection = await api.tokenSelection();
          const cap = Number(selection.active_stream_cap || 10);
          setStreamCap(Number.isFinite(cap) ? Math.max(1, Math.min(50, cap)) : 10);
        } catch {}
      }
      setRefreshNonce((value) => value + 1);
      setSyncText(`已同步 ${new Date().toLocaleTimeString("zh-CN")}`);
    } catch (caught) {
      setHealth(null);
      setAuthContext(null);
      setMe(null);
      if (hasServiceKey()) {
        setAuthBlocked(true);
      }
      pushToast(errorMessage(caught), "error");
      setSyncText(`同步失败 ${new Date().toLocaleTimeString("zh-CN")}`);
    } finally {
      setLoading(false);
    }
  }, [pushToast]);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => void refreshAll(), 30_000);
    return () => window.clearInterval(timer);
  }, [refreshAll]);

  let page: React.ReactElement;
  const adminRoute = route.key.startsWith("admin_") || route.key === "settings" || route.key === "runtime";
  const admin = isAdminPrincipal(me);
  if (adminRoute && !admin && !loading) {
    page = <EmptyState title="无权限" description="当前 API Key 没有管理员权限。" />;
  } else if (route.key === "admin_users") {
    page = <AdminUsersPage pushToast={pushToast} refreshNonce={refreshNonce} />;
  } else if (route.key === "admin_user_detail") {
    page = <AdminUserDetailPage pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
  } else if (route.key === "admin_pools") {
    page = <AdminPoolsPage activeStreamCap={streamCap} pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
  } else if (route.key === "admin_pool_detail") {
    page = (
      <KeyDetailPage
        activeStreamCap={streamCap}
        apiScope="admin"
        backHref={`/admin/pools${route.search || ""}`}
        backLabel="返回号池"
        id={Number(route.params.id)}
        pushToast={pushToast}
        refreshNonce={refreshNonce}
      />
    );
  } else if (route.key === "admin_requests") {
    page = <AdminRequestsPage refreshNonce={refreshNonce} />;
  } else if (route.key === "admin_imports") {
    page = <AdminImportsPage refreshNonce={refreshNonce} />;
  } else if (route.key === "admin_audit") {
    page = <AdminAuditPage refreshNonce={refreshNonce} />;
  } else if (route.key === "admin_sub2api") {
    page = <AdminSub2APIPage pushToast={pushToast} refreshNonce={refreshNonce} />;
  } else if (route.key === "imports" || route.key === "import_new") {
    page = <ImportsPage pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
  } else if (route.key === "requests") {
    page = <RequestsPage refreshNonce={refreshNonce} />;
  } else if (route.key === "settings") {
    page = <SettingsPage onStreamCapChange={setStreamCap} pushToast={pushToast} refreshNonce={refreshNonce} streamCap={streamCap} />;
  } else if (route.key === "user_settings") {
    page = <UserSettingsPage pushToast={pushToast} refreshNonce={refreshNonce} />;
  } else if (route.key === "runtime") {
    page = <RuntimePage counts={counts} health={health} refreshNonce={refreshNonce} />;
  } else {
    page = <KeysPage activeStreamCap={streamCap} onCountsChange={setCounts} pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
  }

  return (
    <AppShell
      authBlocked={authBlocked}
      counts={counts}
      health={health}
      loading={loading}
      me={me}
      onRefresh={() => void refreshAll()}
      onThemeChange={setTheme}
      protectedMode={protectedMode}
      routeKey={route.key}
      syncText={syncText}
      theme={theme}
      webVersion={webVersion}
    >
      {page}
      <ToastStack items={toasts} />
    </AppShell>
  );
}
