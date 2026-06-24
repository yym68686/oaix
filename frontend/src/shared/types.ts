import type { TokenItem } from "@/lib/api";

export type ThemePreference = "auto" | "light" | "dark";
export type TokenStatus = "all" | "available" | "cooling" | "disabled";
export type RouteKey =
  | "account"
  | "account_api_keys"
  | "keys"
  | "key_detail"
  | "imports"
  | "import_new"
  | "requests"
  | "settings"
  | "runtime"
  | "admin_users"
  | "admin_user_detail"
  | "admin_pools"
  | "admin_pool_detail"
  | "admin_requests"
  | "admin_audit"
  | "admin_sub2api";

export type ToastMessage = {
  id: number;
  title: string;
  variant: "success" | "warning" | "error" | "info";
};

export type DeleteTarget = {
  ids: number[];
  title: string;
  description: string;
};

export type RemarkTarget = {
  id: number;
  title: string;
  meta: string;
  remark: string;
};

export type ImportEntry = string | Record<string, unknown>;

export type TokenListMutation = {
  reload: () => Promise<void>;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
};

export type TokenActionHandlers = {
  onDelete: (target: DeleteTarget) => void;
  onProbe: (id: number) => void;
  onRemark: (target: RemarkTarget) => void;
  onSelectedChange?: (selected: Set<number>) => void;
  onToggleActivation: (id: number, active: boolean) => void;
};

export type TokenRowView = {
  item: TokenItem;
  selected?: boolean;
  selection?: Set<number>;
};
