import { useCallback, useEffect, useState } from "react";
import { api, type CodexTicketSettings } from "@/lib/api";
import { Button } from "@/registry/default/ui/button";
import { Checkbox } from "@/registry/default/ui/checkbox";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { ErrorAlert } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import { PageSection, PageSectionHeader, PageSectionTitle, PageSectionDescription, PageSectionPanel } from "@/shared/page-section";

export function CodexTicketsPanel() {
  const [settings, setSettings] = useState<CodexTicketSettings | null>(null);
  const [enabled, setEnabled] = useState(true);
  const [failClosed, setFailClosed] = useState(false);
  const [models, setModels] = useState("");
  const [proxy, setProxy] = useState("");
  const [clearProxy, setClearProxy] = useState(false);
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const load = useCallback((value: CodexTicketSettings) => {
    setSettings(value); setEnabled(value.policy.enabled); setFailClosed(value.policy.fail_closed);
    setModels(value.policy.models.join(", ")); setProxy(""); setClearProxy(false);
  }, []);
  useEffect(() => { let active = true; api.codexTickets().then(value => { if (active) load(value); }).catch(caught => { if (active) setError(errorMessage(caught)); }); return () => { active = false; }; }, [load]);
  async function save() {
    setSaving(true); setError("");
    try { load(await api.updateCodexTickets({ enabled, fail_closed: failClosed, models: models.split(",").map(m => m.trim()).filter(Boolean), harvest_proxy_url: proxy, clear_harvest_proxy: clearProxy })); }
    catch (caught) { setError(errorMessage(caught)); }
    finally { setSaving(false); }
  }
  return <PageSection>
    <PageSectionHeader><PageSectionTitle>Codex 票据</PageSectionTitle><PageSectionDescription>按账号和模型自动缓存票据，有效期 1 小时，提前 10 分钟刷新。默认使用账号现有代理。</PageSectionDescription></PageSectionHeader>
    <PageSectionPanel className="grid gap-4">
      {error && <ErrorAlert title="票据设置" message={error} />}
      {settings && <p className="text-sm text-muted-foreground">有效票据 {settings.stats?.ready_tickets ?? 0} · 已注入 {settings.stats?.injected ?? 0} 次 · 正在采集 {settings.stats?.active_probes ?? 0}</p>}
      <Label className="flex items-center gap-2"><Checkbox checked={enabled} disabled={!settings || saving} onCheckedChange={value => setEnabled(Boolean(value))} />自动采集和注入票据（默认开启）</Label>
      <Label className="flex items-center gap-2"><Checkbox checked={failClosed} disabled={!settings || saving} onCheckedChange={value => setFailClosed(Boolean(value))} />缺票时暂停该账号的对应模型</Label>
      <p className="text-sm text-muted-foreground">默认缺票时继续转发。开启暂停后，只有已有有效票据的账号才可处理这些模型。</p>
      <div className="grid gap-2"><Label htmlFor="ticket-models">模型（逗号分隔）</Label><Input id="ticket-models" nativeInput value={models} disabled={!settings || saving} onChange={e => setModels(e.currentTarget.value)} /></div>
      <div className="grid gap-2"><Label htmlFor="ticket-proxy">独立采集代理（可选）</Label><Input id="ticket-proxy" type="password" autoComplete="new-password" nativeInput value={proxy} disabled={!settings || saving || clearProxy} placeholder={settings?.harvest_proxy_configured ? "已设置；留空保留" : "留空使用账号现有代理"} onChange={e => setProxy(e.currentTarget.value)} /></div>
      {settings?.harvest_proxy_configured && <Label className="flex items-center gap-2"><Checkbox checked={clearProxy} disabled={saving} onCheckedChange={value => setClearProxy(Boolean(value))} />清除独立代理，恢复账号现有代理</Label>}
      <div className="flex flex-wrap gap-2"><Button disabled={!settings || saving} loading={saving} onClick={() => void save()}>保存票据设置</Button><Button variant="outline" disabled={saving} onClick={() => void api.codexTickets().then(value => { if (settings) setSettings(value); else load(value); setError(""); }).catch(caught => setError(errorMessage(caught)))}>刷新状态</Button></div>
    </PageSectionPanel>
  </PageSection>;
}
