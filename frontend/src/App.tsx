import { useCallback, useEffect, useState } from "react";
import { AppShell } from "@/app/AppShell";
import { useRouteState } from "@/app/router";
import { ImportsPage } from "@/features/imports/ImportsPage";
import { KeysPage } from "@/features/keys/KeysPage";
import { RequestsPage } from "@/features/requests/RequestsPage";
import { RuntimePage } from "@/features/runtime/RuntimePage";
import { SettingsPage } from "@/features/settings/SettingsPage";
import { api, hasServiceKey, type HealthResponse, type TokenCounts } from "@/lib/api";
import { applyTheme, errorMessage, readThemePreference } from "@/shared/domain";
import { ToastStack } from "@/shared/components";
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
        setAuthBlocked(true);
        setSyncText(`需要凭证 · ${new Date().toLocaleTimeString("zh-CN")}`);
        return;
      }
      setAuthBlocked(false);
      try {
        const selection = await api.tokenSelection();
        const cap = Number(selection.active_stream_cap || 10);
        setStreamCap(Number.isFinite(cap) ? Math.max(1, Math.min(10, cap)) : 10);
      } catch {}
      setRefreshNonce((value) => value + 1);
      setSyncText(`已同步 ${new Date().toLocaleTimeString("zh-CN")}`);
    } catch (caught) {
      setHealth(null);
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
  if (route.key === "imports" || route.key === "import_new") {
    page = <ImportsPage pushToast={pushToast} refreshNonce={refreshNonce} route={route} />;
  } else if (route.key === "requests") {
    page = <RequestsPage refreshNonce={refreshNonce} />;
  } else if (route.key === "settings") {
    page = <SettingsPage onStreamCapChange={setStreamCap} pushToast={pushToast} refreshNonce={refreshNonce} streamCap={streamCap} />;
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
