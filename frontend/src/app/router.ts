import { useEffect, useMemo, useState } from "react";
import type { RouteKey } from "@/shared/types";

export type RouteState = {
  key: RouteKey;
  params: Record<string, string>;
  path: string;
  search: string;
};

export function currentRoute(): RouteState {
  return parseRoute(window.location.pathname, window.location.search);
}

export function parseRoute(pathname: string, search = ""): RouteState {
  const path = normalizePath(pathname);
  const segments = path.split("/").filter(Boolean);
  if (segments[0] === "admin" && segments[1] === "users" && segments[2]) {
    return { key: "admin_user_detail", params: { id: segments[2] }, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "users") {
    return { key: "admin_users", params: {}, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "pools" && segments[2]) {
    return { key: "admin_pool_detail", params: { id: segments[2] }, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "pools") {
    return { key: "admin_pools", params: {}, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "requests") {
    return { key: "admin_requests", params: {}, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "audit") {
    return { key: "admin_audit", params: {}, path, search };
  }
  if (segments[0] === "admin" && segments[1] === "sub2api") {
    return { key: "admin_sub2api", params: {}, path, search };
  }
  if (segments[0] === "keys" && segments[1]) {
    return { key: "key_detail", params: { id: segments[1] }, path, search };
  }
  if (segments[0] === "imports" && segments[1] === "new") {
    return { key: "import_new", params: {}, path, search };
  }
  if (segments[0] === "imports") {
    return { key: "imports", params: {}, path, search };
  }
  if (segments[0] === "requests") {
    return { key: "requests", params: {}, path, search };
  }
  if (segments[0] === "settings") {
    return { key: "settings", params: {}, path, search };
  }
  if (segments[0] === "runtime") {
    return { key: "runtime", params: {}, path, search };
  }
  return { key: "keys", params: {}, path: "/keys", search: search || "?status=available" };
}

export function navigateTo(path: string, options: { replace?: boolean } = {}): void {
  const next = path.startsWith("/") ? path : `/${path}`;
  if (next === `${window.location.pathname}${window.location.search}`) {
    return;
  }
  if (options.replace) {
    window.history.replaceState(null, "", next);
  } else {
    window.history.pushState(null, "", next);
  }
  window.dispatchEvent(new Event("oaix:navigate"));
}

export function useRouteState(): RouteState {
  const [route, setRoute] = useState(() => currentRoute());
  useEffect(() => {
    const sync = () => setRoute(currentRoute());
    window.addEventListener("popstate", sync);
    window.addEventListener("oaix:navigate", sync);
    return () => {
      window.removeEventListener("popstate", sync);
      window.removeEventListener("oaix:navigate", sync);
    };
  }, []);
  useEffect(() => {
    if (window.location.pathname === "/" || window.location.pathname === "/account" || window.location.pathname === "/account/api-keys") {
      navigateTo("/keys?status=available", { replace: true });
    }
  }, []);
  return route;
}

export function useRouteSearch(route: RouteState): URLSearchParams {
  return useMemo(() => new URLSearchParams(route.search), [route.search]);
}

function normalizePath(pathname: string): string {
  const path = pathname.trim() || "/";
  if (path.length > 1 && path.endsWith("/")) {
    return path.slice(0, -1);
  }
  return path;
}
