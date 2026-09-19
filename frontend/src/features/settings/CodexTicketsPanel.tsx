import { useCallback, useEffect, useState } from "react";
import { api, type CodexTicketSettings } from "@/lib/api";
import { Button } from "@/registry/default/ui/button";
import { Checkbox } from "@/registry/default/ui/checkbox";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { ErrorAlert, SelectField } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import { PageSection, PageSectionHeader, PageSectionTitle, PageSectionDescription, PageSectionPanel } from "@/shared/page-section";

export function CodexTicketsPanel() {
  const [settings, setSettings] = useState<CodexTicketSettings | null>(null);
  const [enabled, setEnabled] = useState(true);
  const [failClosed, setFailClosed] = useState(false);
  const [models, setModels] = useState("");
  const [proxy, setProxy] = useState("");
  const [route, setRoute] = useState("account");
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const load = useCallback((value: CodexTicketSettings) => {
    setSettings(value); setEnabled(value.policy.enabled); setFailClosed(value.policy.fail_closed);
    setModels(value.policy.models.join(", ")); setProxy(""); setRoute(value.policy.harvest_proxy_channel_id ? `channel:${value.policy.harvest_proxy_channel_id}` : value.harvest_proxy_configured ? "custom" : "account");
  }, []);
  useEffect(() => { let active = true; api.codexTickets().then(value => { if (active) load(value); }).catch(caught => { if (active) setError(errorMessage(caught)); }); return () => { active = false; }; }, [load]);
  async function save() {
    setSaving(true); setError("");
    try {
      if (route === "custom" && !proxy.trim() && (!settings?.harvest_proxy_configured || settings.policy.harvest_proxy_channel_id)) {
        throw new Error("请输入独立采集代理地址，或选择现有代理渠道");
      }
      load(await api.updateCodexTickets({
        enabled, fail_closed: failClosed,
        models: models.split(",").map(m => m.trim()).filter(Boolean),
        harvest_proxy_url: route === "custom" ? proxy : "",
        harvest_proxy_channel_id: route.startsWith("channel:") ? Number(route.slice(8)) : 0,
        clear_harvest_proxy: route === "account",
      }));
    }
    catch (caught) { setError(errorMessage(caught)); }
    finally { setSaving(false); }
  }
  return <PageSection>
    <PageSectionHeader><PageSectionTitle>Codex 票据</PageSectionTitle><PageSectionDescription>按账号和模型自动缓存票据，有效期 1 小时，提前 10 分钟刷新。可使用账号代理、已有代理渠道或独立代理。</PageSectionDescription></PageSectionHeader>
    <PageSectionPanel className="grid gap-4">
      {error && <ErrorAlert title="票据设置" message={error} />}
      {settings && <p className="text-sm text-muted-foreground">有效票据 {settings.stats?.ready_tickets ?? 0} · 已注入 {settings.stats?.injected ?? 0} 次 · 正在采集 {settings.stats?.active_probes ?? 0}</p>}
      <Label className="flex items-center gap-2"><Checkbox checked={enabled} disabled={!settings || saving} onCheckedChange={value => setEnabled(Boolean(value))} />自动采集和注入票据（默认开启）</Label>
      <Label className="flex items-center gap-2"><Checkbox checked={failClosed} disabled={!settings || saving} onCheckedChange={value => setFailClosed(Boolean(value))} />缺票时暂停该账号的对应模型</Label>
      <p className="text-sm text-muted-foreground">默认缺票时继续转发。开启暂停后，只有已有有效票据的账号才可处理这些模型。</p>
      <div className="grid gap-2"><Label htmlFor="ticket-models">模型（逗号分隔）</Label><Input id="ticket-models" nativeInput value={models} disabled={!settings || saving} onChange={e => setModels(e.currentTarget.value)} /></div>
      <SelectField label="票据采集出口" value={route} onChange={setRoute} options={[
        { label: "使用每个账号现有代理", value: "account" },
        ...(settings?.proxy_channels || []).map(channel => ({ label: `${channel.name}（渠道 #${channel.id}）`, value: `channel:${channel.id}` })),
        ...(settings?.policy.harvest_proxy_channel_id && !(settings.proxy_channels || []).some(c => c.id === settings.policy.harvest_proxy_channel_id) ? [{ label: `当前渠道 #${settings.policy.harvest_proxy_channel_id}`, value: `channel:${settings.policy.harvest_proxy_channel_id}` }] : []),
        { label: "填写独立采集代理", value: "custom" },
      ]} />
      {route.startsWith("channel:") && <p className="text-sm text-muted-foreground">采集使用所选渠道的最新配置。业务请求继续使用账号自己的代理。每次采集新建连接，出口轮换由代理提供方控制。</p>}
      {route === "custom" && <div className="grid gap-2"><Label htmlFor="ticket-proxy">独立采集代理</Label><Input id="ticket-proxy" type="password" autoComplete="new-password" nativeInput value={proxy} disabled={!settings || saving} placeholder={settings?.harvest_proxy_configured && !settings.policy.harvest_proxy_channel_id ? "已设置；留空保留" : "输入代理 URL"} onChange={e => setProxy(e.currentTarget.value)} /></div>}
      <div className="flex flex-wrap gap-2"><Button disabled={!settings || saving} loading={saving} onClick={() => void save()}>保存票据设置</Button><Button variant="outline" disabled={saving} onClick={() => void api.codexTickets().then(value => { if (settings) setSettings(value); else load(value); setError(""); }).catch(caught => setError(errorMessage(caught)))}>刷新状态</Button></div>
    </PageSectionPanel>
  </PageSection>;
}
