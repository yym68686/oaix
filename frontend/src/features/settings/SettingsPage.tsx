import { DatabaseIcon, RefreshCwIcon, SaveIcon, Settings2Icon } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Textarea } from "@/registry/default/ui/textarea";
import { cn } from "@/registry/default/lib/utils";
import { api, type SettingItem } from "@/lib/api";
import { clamp } from "@/lib/format";
import { EmptyState, ErrorAlert, LoadingState } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

export function SettingsPage({
  onStreamCapChange,
  pushToast,
  refreshNonce,
  streamCap,
}: {
  onStreamCapChange: (value: number) => void;
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
  streamCap: number;
}) {
  const [items, setItems] = useState<SettingItem[]>([]);
  const [selectionSummary, setSelectionSummary] = useState("等待载入");
  const [settingKey, setSettingKey] = useState("");
  const [settingValue, setSettingValue] = useState("{\n  \"enabled\": true\n}");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const loadTokenSelection = useCallback(async () => {
    const payload = await api.tokenSelection();
    const cap = Number(payload.active_stream_cap || 10);
    setSelectionSummary(String(payload.strategy || "snapshot_round_robin"));
    onStreamCapChange(Number.isFinite(cap) ? clamp(cap, 1, 10) : 10);
  }, [onStreamCapChange]);

  const loadSettings = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [settingsPayload] = await Promise.all([api.settings(), loadTokenSelection()]);
      setItems(settingsPayload.items || []);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [loadTokenSelection]);

  useEffect(() => {
    void loadSettings();
  }, [loadSettings, refreshNonce]);

  async function saveStreamCap() {
    await api.updateTokenSelection({ active_stream_cap: streamCap });
    pushToast("调度设置已保存");
    await loadTokenSelection();
  }

  async function saveSetting() {
    if (!settingKey.trim()) {
      pushToast("请填写设置 key", "warning");
      return;
    }
    let value: unknown;
    try {
      value = JSON.parse(settingValue);
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
      return;
    }
    await api.updateSetting(settingKey.trim(), value);
    pushToast("设置已保存");
    await loadSettings();
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(320px,.7fr)_minmax(0,1fr)]">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings2Icon className="size-5" />
            调度设置
          </CardTitle>
          <CardDescription>Key 分发、每 Key 并发和当前 selector 状态。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
          <Alert variant="info">
            <DatabaseIcon />
            <AlertTitle>{selectionSummary}</AlertTitle>
            <AlertDescription>Go 网关按 snapshot selector 调度，保存后由后端配置决定是否立即生效。</AlertDescription>
          </Alert>
          <div className="grid gap-2">
            <Label htmlFor="stream-cap">每 Key 并发</Label>
            <Input
              id="stream-cap"
              max={10}
              min={1}
              nativeInput
              onChange={(event) => onStreamCapChange(clamp(Number(event.currentTarget.value || 1), 1, 10))}
              type="number"
              value={streamCap}
            />
          </div>
          <Button onClick={() => void saveStreamCap()}>
            <SaveIcon />
            保存调度设置
          </Button>
        </CardPanel>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>运行设置</CardTitle>
          <CardDescription>查看和写入 JSON 设置项。</CardDescription>
          <CardAction>
            <Button onClick={() => void loadSettings()} size="sm" variant="outline">
              <RefreshCwIcon className={cn(loading && "animate-spin")} />
              刷新
            </Button>
          </CardAction>
        </CardHeader>
        <CardPanel className="grid gap-4">
          <div className="grid gap-2 md:grid-cols-[minmax(180px,.35fr)_minmax(0,1fr)]">
            <div className="grid gap-2">
              <Label htmlFor="settings-key">Key</Label>
              <Input id="settings-key" nativeInput onChange={(event) => setSettingKey(event.currentTarget.value)} placeholder="例如 token_selection" value={settingKey} />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="settings-value">Value JSON</Label>
              <Textarea id="settings-value" onChange={(event) => setSettingValue(event.currentTarget.value)} rows={4} spellCheck={false} value={settingValue} />
            </div>
          </div>
          <Button className="w-fit" onClick={() => void saveSetting()}>
            <SaveIcon />
            保存设置
          </Button>
          {error ? (
            <ErrorAlert title="设置载入失败" message={error} />
          ) : (
            <div className="grid gap-2">
              {loading && !items.length && <LoadingState compact label="正在载入设置项" />}
              {items.map((item) => (
                <div className="rounded-lg border bg-muted/40 p-3" key={item.key}>
                  <div className="font-medium text-sm">{item.key}</div>
                  <pre className="mt-2 max-h-56 overflow-auto rounded-lg bg-background p-3 text-xs oaix-scrollbar">
                    {JSON.stringify(item.value ?? null, null, 2)}
                  </pre>
                  <div className="mt-2 text-muted-foreground text-xs">{item.updated_at || "-"}</div>
                </div>
              ))}
              {!items.length && !loading && <EmptyState title="暂无设置项" description="保存后会显示在这里。" compact />}
            </div>
          )}
        </CardPanel>
      </Card>
    </div>
  );
}
