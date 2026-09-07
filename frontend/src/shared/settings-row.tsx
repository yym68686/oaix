import type { ReactNode } from "react";
import { Label } from "@/registry/default/ui/label";

export function AccountDetailPanel({ id, title, children }: {
  id: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <section aria-labelledby={id} data-slot="account-detail-panel" className="min-w-0 rounded-xl border bg-muted/24">
      <header className="border-b px-4 py-4 sm:px-5">
        <h2 id={id} className="text-sm font-semibold leading-6">{title}</h2>
      </header>
      {children}
    </section>
  );
}

/** Match the account information panel's two columns and shared inset. */
export function SettingsRow({ controlId, title, description, children }: {
  controlId: string;
  title: string;
  description: ReactNode;
  children: ReactNode;
}) {
  return (
    <div data-slot="settings-row" className="grid min-w-0 gap-x-8 gap-y-3 px-4 py-5 sm:grid-cols-2 sm:px-5">
      <div className="grid min-w-0 content-start gap-1.5 sm:pt-1">
        <Label className="text-sm leading-6" htmlFor={controlId}>{title}</Label>
        <p className="max-w-md text-muted-foreground text-xs leading-5">{description}</p>
      </div>
      <div className="grid min-w-0 content-start gap-2.5">{children}</div>
    </div>
  );
}
