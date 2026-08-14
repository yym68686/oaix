import { useEffect, useState } from "react";
import { getServiceKey, setServiceKey, type MeResponse, type PlatformUser } from "@/lib/api";

const ACCOUNTS_STORAGE = "oaix.savedAccounts.v1";
const ACCOUNTS_CHANGED_EVENT = "oaix:accounts-changed";
const MAX_SAVED_ACCOUNTS = 12;

export type SavedAccount = {
  id: string;
  apiKey: string;
  apiKeyID?: number | null;
  principalType?: string;
  role?: string;
  user?: PlatformUser | null;
  addedAt: string;
  lastUsedAt: string;
};

export function getSavedAccounts(): SavedAccount[] {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(ACCOUNTS_STORAGE) || "[]");
    if (!Array.isArray(parsed)) {
      return [];
    }
    const unique = new Map<string, SavedAccount>();
    for (const value of parsed) {
      const account = normalizeSavedAccount(value);
      if (account) {
        unique.set(account.id, account);
      }
    }
    return [...unique.values()]
      .sort((left, right) => right.lastUsedAt.localeCompare(left.lastUsedAt))
      .slice(0, MAX_SAVED_ACCOUNTS);
  } catch {
    return [];
  }
}

export function getActiveSavedAccount(accounts = getSavedAccounts()): SavedAccount | null {
  const activeKey = getServiceKey();
  return accounts.find((account) => account.apiKey === activeKey) || null;
}

export function rememberSavedAccount(apiKey: string, me: MeResponse, activate = true): SavedAccount | null {
  const key = apiKey.trim();
  if (!key) {
    return null;
  }
  const now = new Date().toISOString();
  const accounts = getSavedAccounts();
  const id = savedAccountID(me, key);
  const previous = accounts.find((account) => account.id === id);
  const account: SavedAccount = {
    id,
    apiKey: key,
    apiKeyID: me.api_key_id,
    principalType: me.principal_type,
    role: me.role || me.user?.role,
    user: me.user || null,
    addedAt: previous?.addedAt || now,
    lastUsedAt: activate ? now : previous?.lastUsedAt || now,
  };
  writeSavedAccounts([account, ...accounts.filter((item) => item.id !== id)]);
  if (activate) {
    setServiceKey(key);
  }
  notifyAccountsChanged();
  return account;
}

export function rememberPasswordAuthAccount(
  apiKey: string,
  user: PlatformUser | null | undefined,
  apiKeyID?: number | null,
): SavedAccount | null {
  return rememberSavedAccount(apiKey, {
    api_key_id: apiKeyID,
    principal_type: "user",
    role: user?.role || "user",
    user: user || null,
  });
}

export function activateSavedAccount(id: string): SavedAccount | null {
  const accounts = getSavedAccounts();
  const account = accounts.find((item) => item.id === id);
  if (!account) {
    return null;
  }
  const next = { ...account, lastUsedAt: new Date().toISOString() };
  writeSavedAccounts([next, ...accounts.filter((item) => item.id !== id)]);
  setServiceKey(next.apiKey);
  notifyAccountsChanged();
  return next;
}

export function removeSavedAccount(id: string): { activeChanged: boolean; next: SavedAccount | null } {
  const accounts = getSavedAccounts();
  const target = accounts.find((account) => account.id === id);
  if (!target) {
    return { activeChanged: false, next: getActiveSavedAccount(accounts) };
  }
  const activeChanged = target.apiKey === getServiceKey();
  const remaining = accounts.filter((account) => account.id !== id);
  let next: SavedAccount | null = getActiveSavedAccount(remaining);
  if (activeChanged) {
    next = remaining[0] || null;
    setServiceKey(next?.apiKey || "");
    if (next) {
      next = { ...next, lastUsedAt: new Date().toISOString() };
      const nextIndex = remaining.findIndex((account) => account.id === next?.id);
      if (nextIndex >= 0) {
        remaining.splice(nextIndex, 1);
      }
      remaining.unshift(next);
    }
  }
  writeSavedAccounts(remaining);
  notifyAccountsChanged();
  return { activeChanged, next };
}

export function restoreSavedAccount(): SavedAccount | null {
  if (getServiceKey()) {
    return getActiveSavedAccount();
  }
  const account = getSavedAccounts()[0] || null;
  if (account) {
    setServiceKey(account.apiKey);
    notifyAccountsChanged();
  }
  return account;
}

export function savedAccountLabel(account: SavedAccount | null | undefined): string {
  if (!account) {
    return "未登录";
  }
  return account.user?.display_name?.trim() || account.user?.email?.trim() || (account.principalType === "service" ? "Service API Key" : "当前会话");
}

export function useSavedAccounts(): SavedAccount[] {
  const [accounts, setAccounts] = useState<SavedAccount[]>(() => getSavedAccounts());
  useEffect(() => {
    const sync = () => setAccounts(getSavedAccounts());
    window.addEventListener(ACCOUNTS_CHANGED_EVENT, sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener(ACCOUNTS_CHANGED_EVENT, sync);
      window.removeEventListener("storage", sync);
    };
  }, []);
  return accounts;
}

function savedAccountID(me: MeResponse, apiKey: string): string {
  if (me.user?.id) {
    return `user:${me.user.id}`;
  }
  if (me.api_key_id) {
    return `credential:${me.api_key_id}`;
  }
  return `credential:${apiKey.slice(0, 24)}`;
}

function normalizeSavedAccount(value: unknown): SavedAccount | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const candidate = value as Partial<SavedAccount>;
  const id = String(candidate.id || "").trim();
  const apiKey = String(candidate.apiKey || "").trim();
  if (!id || !apiKey) {
    return null;
  }
  const now = new Date().toISOString();
  return {
    id,
    apiKey,
    apiKeyID: typeof candidate.apiKeyID === "number" ? candidate.apiKeyID : null,
    principalType: typeof candidate.principalType === "string" ? candidate.principalType : "",
    role: typeof candidate.role === "string" ? candidate.role : "",
    user: candidate.user && typeof candidate.user === "object" ? candidate.user : null,
    addedAt: typeof candidate.addedAt === "string" ? candidate.addedAt : now,
    lastUsedAt: typeof candidate.lastUsedAt === "string" ? candidate.lastUsedAt : now,
  };
}

function writeSavedAccounts(accounts: SavedAccount[]): void {
  try {
    window.localStorage.setItem(ACCOUNTS_STORAGE, JSON.stringify(accounts.slice(0, MAX_SAVED_ACCOUNTS)));
  } catch {}
}

function notifyAccountsChanged(): void {
  window.dispatchEvent(new Event(ACCOUNTS_CHANGED_EVENT));
}
