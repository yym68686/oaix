import { ArrowUpRightIcon, CableIcon, CheckCircle2Icon, PencilIcon, PlayIcon, PlusIcon, Trash2Icon } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { api, getAuthContext, type ProxyChannel, type ProxyPreview, type ProxyTestResult, type TokenAPIScope } from "@/lib/api";
import { navigateTo } from "@/app/router";
import { Button } from "@/registry/default/ui/button";
import { Input } from "@/registry/default/ui/input";
import { SettingsRow } from "@/shared/settings-row";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "@/registry/default/ui/select";
import { Label } from "@/registry/default/ui/label";
import { Badge } from "@/registry/default/ui/badge";
import { Dialog, DialogDescription, DialogFooter, DialogHeader, DialogPanel, DialogPopup, DialogTitle } from "@/registry/default/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/registry/default/ui/table";
import { PageSection, PageSectionAction, PageSectionDescription, PageSectionHeader, PageSectionPanel, PageSectionTitle } from "@/shared/page-section";
import { EmptyState, ErrorAlert, LoadingState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

type Notify = (title: string, variant?: ToastMessage["variant"]) => void;
const canWrite = () => String(getAuthContext()?.role || "").toLowerCase() !== "readonly_admin";
const endpoint = (item: ProxyPreview) => `${item.protocol.toUpperCase()} · ${item.host.includes(":") ? `[${item.host}]` : item.host}:${item.port}`;

export function ProxiesPage({ pushToast, refreshNonce }: { pushToast: Notify; refreshNonce: number }) {
  const [items, setItems] = useState<ProxyChannel[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<ProxyChannel | null>(null);
  const [name, setName] = useState("");
  const [raw, setRaw] = useState("");
  const [preview, setPreview] = useState<ProxyPreview | null>(null);
  const [parseError, setParseError] = useState("");
  const [parsing, setParsing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState<Record<number, boolean>>({});
  const [results, setResults] = useState<Record<number, ProxyTestResult>>({});
  const [deleteTarget, setDeleteTarget] = useState<ProxyChannel | null>(null);
  const [deleting, setDeleting] = useState(false);

  const load = useCallback(async () => {
    try {
      const data = await api.myProxies();
      setItems(data.items);
      setError("");
    } catch (caught) { setError(errorMessage(caught)); }
    finally { setLoading(false); }
  }, []);
  useEffect(() => { void load(); }, [load, refreshNonce]);

  useEffect(() => {
    let current = true;
    setPreview(null);
    setParseError("");
    setParsing(Boolean(raw.trim()));
    if (!raw.trim()) return;
    const timer = window.setTimeout(() => {
      void api.parseProxy(raw).then((value) => { if (current) setPreview(value); })
        .catch((caught) => { if (current) setParseError(errorMessage(caught)); })
        .finally(() => { if (current) setParsing(false); });
    }, 350);
    return () => { current = false; window.clearTimeout(timer); };
  }, [raw]);

  function clearForm() { setFormOpen(false); setEditing(null); setName(""); setRaw(""); setPreview(null); setParseError(""); }
  async function save() {
    if (saving || !canWrite() || parsing || parseError || (!editing && !preview)) return;
    setSaving(true);
    try {
      const saved = await api.saveProxy(editing?.id, { name: name.trim() || preview?.host || "", proxy: raw });
      setResults((current) => { const next = { ...current }; delete next[saved.id]; return next; });
      clearForm();
      pushToast(editing ? "代理渠道已更新" : "代理渠道已添加，可在账号详情中绑定");
      await load();
    } catch (caught) { pushToast(errorMessage(caught), "error"); }
    finally { setSaving(false); }
  }
  async function test(item: ProxyChannel) {
    setTesting((state) => ({ ...state, [item.id]: true }));
    setResults((state) => { const next = { ...state }; delete next[item.id]; return next; });
    try {
      const result = await api.testProxy(item.id);
      setResults((state) => ({ ...state, [item.id]: result }));
    } catch (caught) { pushToast(errorMessage(caught), "error"); }
    finally { setTesting((state) => ({ ...state, [item.id]: false })); }
  }
  async function remove() {
    if (!deleteTarget || deleting) return;
    setDeleting(true);
    try {
      await api.deleteProxy(deleteTarget.id);
      if (editing?.id === deleteTarget.id) clearForm();
      setDeleteTarget(null);
      pushToast("代理渠道已删除");
      await load();
    } catch (caught) { pushToast(errorMessage(caught), "error"); }
    finally { setDeleting(false); }
  }

  return <div className="grid gap-6">
    <PageSection>
      <PageSectionHeader>
        <PageSectionTitle className="flex items-center gap-2"><CableIcon className="size-5" />代理配置</PageSectionTitle>
        <PageSectionDescription>管理账号的出站代理。添加渠道后，在账号详情的「账号设置」中选择使用。</PageSectionDescription>
        <PageSectionAction><Button disabled={!canWrite()} onClick={() => { clearForm(); setFormOpen(true); }}><PlusIcon />添加代理</Button></PageSectionAction>
      </PageSectionHeader>
    </PageSection>
    <PageSection>
      <PageSectionHeader><PageSectionTitle>代理渠道 <span className="text-muted-foreground font-normal">{items.length}</span></PageSectionTitle>
        <PageSectionDescription>测试会通过代理访问 ChatGPT，显示出口 IP 和耗时。绑定中的渠道需先解绑才能删除。</PageSectionDescription>
      </PageSectionHeader>
      <PageSectionPanel>
        {error && <ErrorAlert title="代理配置加载失败" message={error} />}
        {loading ? <LoadingState /> : items.length === 0 ? <EmptyState title="还没有代理渠道" description="点击「添加代理」，粘贴代理字符串即可创建渠道。" /> :
          <Table><TableHeader><TableRow><TableHead>渠道</TableHead><TableHead>账号</TableHead><TableHead>可用性测试</TableHead><TableHead className="text-right">操作</TableHead></TableRow></TableHeader>
            <TableBody>{items.map((item) => <TableRow key={item.id}>
              <TableCell><div className="font-medium">{item.name}</div><div className="text-muted-foreground mt-1 break-all text-xs">{endpoint(item)}</div><div className="text-muted-foreground text-xs">{item.has_auth ? "已配置认证" : "无需认证"}</div></TableCell>
              <TableCell>{item.account_count} 个</TableCell>
              <TableCell className="max-w-80 whitespace-normal" aria-live="polite">{testing[item.id] ? <span className="text-muted-foreground text-xs">正在通过代理连接 ChatGPT…</span> : results[item.id] ? <div className="grid gap-1 text-xs"><span className={results[item.id].ok ? "text-emerald-600" : "text-destructive-foreground"}>{results[item.id].message}</span><span className="text-muted-foreground">{results[item.id].exit_ip ? `出口 ${results[item.id].exit_ip} · ` : ""}{results[item.id].duration_ms} ms · {new Date(results[item.id].checked_at).toLocaleTimeString()}</span></div> : <span className="text-muted-foreground text-xs">尚未测试</span>}</TableCell>
              <TableCell><div className="flex justify-end gap-1">
                <Button size="sm" variant="outline" loading={testing[item.id]} disabled={!canWrite() || (saving && editing?.id === item.id)} onClick={() => void test(item)} aria-label={`测试 ${item.name}`}><PlayIcon />测试</Button>
                <Button size="icon-sm" variant="ghost" disabled={saving || testing[item.id] || !canWrite()} aria-label={`编辑 ${item.name}`} onClick={() => { setEditing(item); setName(item.name); setRaw(""); setFormOpen(true); }}><PencilIcon /></Button>
                <Button size="icon-sm" variant="ghost" disabled={item.account_count > 0 || testing[item.id] || !canWrite()} aria-label={`删除 ${item.name}`} title={item.account_count ? "请先解除账号绑定" : "删除代理"} onClick={() => setDeleteTarget(item)}><Trash2Icon /></Button>
              </div></TableCell>
            </TableRow>)}</TableBody>
          </Table>}
      </PageSectionPanel>
    </PageSection>
    <Dialog open={formOpen} onOpenChange={(open) => { if (!saving) { if (!open) clearForm(); else setFormOpen(true); } }}><DialogPopup>
      <DialogHeader><DialogTitle>{editing ? "编辑代理渠道" : "添加代理渠道"}</DialogTitle><DialogDescription>粘贴代理字符串，自动识别地址、端口和认证信息。</DialogDescription></DialogHeader>
      <form className="grid gap-4" onSubmit={(event) => { event.preventDefault(); void save(); }}>
          <DialogPanel className="grid gap-4">
            <div className="grid content-start gap-2"><Label htmlFor="proxy-name">渠道名称</Label>
              <Input id="proxy-name" nativeInput maxLength={128} placeholder="例如：美国代理" value={name} disabled={saving || !canWrite()} onChange={(event) => setName(event.currentTarget.value)} />
            </div>
            <div className="grid min-w-0 gap-2"><Label htmlFor="proxy-string">{editing ? "更换代理（留空保留原配置）" : "粘贴代理字符串"}</Label>
              <Input id="proxy-string" nativeInput type="password" autoComplete="new-password" spellCheck={false} maxLength={4096} aria-describedby="proxy-format" placeholder="host:port:用户名:密码" value={raw} disabled={saving || !canWrite()} onChange={(event) => setRaw(event.currentTarget.value)} />
              <p id="proxy-format" className="text-muted-foreground text-xs leading-relaxed">支持 host:port:用户名:密码、host:port 和 HTTP / HTTPS / SOCKS5 URL。粘贴后自动解析；凭证加密保存。</p>
              <div aria-live="polite" className="text-xs">
                {parsing ? <span className="text-muted-foreground">正在解析…</span> : parseError ? <span className="text-destructive-foreground">{parseError}</span> : preview ? <span className="flex flex-wrap items-center gap-2"><CheckCircle2Icon className="size-4 text-emerald-600" />{endpoint(preview)}<Badge variant="outline">{preview.has_auth ? "已识别认证信息" : "无需认证"}</Badge></span> : editing ? <span className="text-muted-foreground">当前：{endpoint(editing)}</span> : null}
              </div>
            </div>
          </DialogPanel>
          <DialogFooter>
            <Button type="button" variant="ghost" disabled={saving} onClick={clearForm}>取消</Button>
            <Button type="submit" loading={saving} disabled={!canWrite() || parsing || Boolean(parseError) || (!editing && !preview) || (!name.trim() && !preview?.host)}><PlusIcon />{editing ? "保存修改" : "添加渠道"}</Button>
          </DialogFooter>
        </form>
    </DialogPopup></Dialog>
    <Dialog open={Boolean(deleteTarget)} onOpenChange={(open) => { if (!open && !deleting) setDeleteTarget(null); }}><DialogPopup>
      <DialogHeader><DialogTitle>删除代理渠道</DialogTitle><DialogDescription>确定删除「{deleteTarget?.name}」？</DialogDescription></DialogHeader>
      <DialogPanel><p className="text-muted-foreground text-sm">删除后需要重新添加该代理的连接和认证信息。</p></DialogPanel>
      <DialogFooter><Button variant="ghost" disabled={deleting} onClick={() => setDeleteTarget(null)}>取消</Button><Button variant="destructive" loading={deleting} onClick={() => void remove()}>删除</Button></DialogFooter>
    </DialogPopup></Dialog>
  </div>;
}

export function TokenProxyEditor({ tokenID, apiScope, pushToast }: { tokenID: number; apiScope: TokenAPIScope; pushToast: Notify }) {
  const [items, setItems] = useState<ProxyChannel[]>([]);
  const [selected, setSelected] = useState("0");
  const [saved, setSaved] = useState("0");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [reload, setReload] = useState(0);
  useEffect(() => {
    let current = true;
    setLoading(true);
    setError("");
    void api.tokenProxy(tokenID, apiScope).then((data) => {
      if (!current) return;
      setItems(data.items); setSelected(String(data.proxy_channel_id)); setSaved(String(data.proxy_channel_id));
    }).catch((caught) => { if (current) setError(errorMessage(caught)); }).finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [tokenID, apiScope, reload]);
  async function save() {
    setBusy(true);
    try {
      const data = await api.updateTokenProxy(tokenID, Number(selected), apiScope);
      setItems(data.items); setSaved(String(data.proxy_channel_id)); setSelected(String(data.proxy_channel_id));
      pushToast(data.proxy_channel_id ? "账号代理已保存，新请求将使用所选渠道" : "已取消账号代理");
    } catch (caught) { pushToast(errorMessage(caught), "error"); }
    finally { setBusy(false); }
  }
  const missing = saved !== "0" && !items.some((item) => String(item.id) === saved);
  const options = [
    { value: "0", label: "不使用代理" },
    ...items.map((item) => ({ value: String(item.id), label: `${item.name} · ${item.host}:${item.port}` })),
    ...(missing ? [{ value: saved, label: "原代理不可用，请重新选择" }] : []),
  ];
  return (
    <SettingsRow controlId="token-proxy-channel" title="账号代理" description="为这个账号选择网络出口，保存后对新请求生效。">
      {loading ? <p className="text-muted-foreground flex min-h-9 items-center text-xs">正在加载代理渠道…</p> : error ? (
        <div className="grid gap-2">
          <ErrorAlert title="代理配置加载失败" message={error} />
          <Button className="w-fit" size="sm" variant="outline" onClick={() => setReload((n) => n + 1)}>重试</Button>
        </div>
      ) : <>
        <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-2">
          <Select disabled={busy || !canWrite()} items={options} value={selected} onValueChange={(value) => setSelected(String(value))}>
            <SelectTrigger id="token-proxy-channel" aria-label="账号代理" size="lg" className="h-10 min-w-0 sm:h-9"><SelectValue /></SelectTrigger>
            <SelectPopup>{options.map((option) => <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>)}</SelectPopup>
          </Select>
          <Button size="lg" className="min-w-20" aria-label="保存账号代理" loading={busy} disabled={selected === saved || !canWrite()} onClick={() => void save()}>保存</Button>
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-1 text-xs leading-5">
          <p className="text-muted-foreground">连接失败时不会自动直连。</p>
          <a className="inline-flex items-center gap-0.5 underline-offset-4 hover:underline focus-visible:rounded-sm focus-visible:outline-2 focus-visible:outline-ring" href="/account/proxies" onClick={(event) => {
            if (!event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) { event.preventDefault(); navigateTo("/account/proxies"); }
          }}>管理代理渠道<ArrowUpRightIcon className="size-3.5" /></a>
        </div>
        {items.length === 0 && <p className="text-muted-foreground text-xs leading-5">此账号所属用户尚未添加代理渠道。</p>}
        {missing && <p className="text-destructive-foreground text-xs leading-5" role="alert">当前代理归属已变更，请重新选择渠道或取消代理。</p>}
      </>}
    </SettingsRow>
  );
}
