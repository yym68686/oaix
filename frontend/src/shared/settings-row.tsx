import type { ReactNode } from "react";
import { Label } from "@/registry/default/ui/label";

/** A shared label/control grid keeps account settings aligned at every width. */
export function SettingsRow({ controlId, title, description, children }: {
  controlId: string;
  title: string;
  description: ReactNode;
  children: ReactNode;
}) {
  return (
    <div data-slot="settings-row" className="grid min-w-0 gap-x-10 gap-y-3 py-6 sm:grid-cols-[13rem_minmax(0,1fr)]">
      <div className="grid min-w-0 content-start gap-1.5 sm:pt-1.5">
        <Label className="text-sm leading-6" htmlFor={controlId}>{title}</Label>
        <p className="text-muted-foreground text-xs leading-5">{description}</p>
      </div>
      <div className="grid min-w-0 content-start gap-2.5">{children}</div>
    </div>
  );
}
