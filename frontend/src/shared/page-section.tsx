import type { ComponentProps } from "react";
import { cn } from "@/registry/default/lib/utils";

/** Flat page sections; inset cards remain available for metrics and dialogs. */
export function PageSection({ className, ...props }: ComponentProps<"section">) {
  return <section data-slot="page-section" className={cn("flex min-w-0 flex-col gap-6 border-b pb-8 last:border-b-0 last:pb-0", className)} {...props} />;
}

export function PageSectionHeader({ className, ...props }: ComponentProps<"header">) {
  return <header className={cn("grid min-w-0 auto-rows-min items-start gap-x-5 gap-y-2 sm:has-[[data-slot=page-action]]:grid-cols-[minmax(0,1fr)_auto]", className)} {...props} />;
}

export function PageSectionTitle({ className, ...props }: ComponentProps<"h2">) {
  return <h2 className={cn("min-w-0 font-semibold text-lg leading-7 [&>svg]:shrink-0 [&>svg]:text-muted-foreground", className)} {...props} />;
}

export function PageSectionDescription({ className, ...props }: ComponentProps<"p">) {
  return <p className={cn("min-w-0 text-muted-foreground text-sm leading-relaxed", className)} {...props} />;
}

export function PageSectionAction({ className, ...props }: ComponentProps<"div">) {
  return <div data-slot="page-action" className={cn("flex min-w-0 flex-wrap items-center gap-2 pt-1 sm:col-start-2 sm:row-span-2 sm:row-start-1 sm:justify-end sm:pt-0", className)} {...props} />;
}

export function PageSectionPanel({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("min-w-0 flex-1", className)} {...props} />;
}
