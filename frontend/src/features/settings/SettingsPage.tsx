import { DatabaseIcon, RefreshCwIcon, SaveIcon, Settings2Icon, ShieldCheckIcon } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Textarea } from "@/registry/default/ui/textarea";
import { cn } from "@/registry/default/lib/utils";
import { api, getServiceKey, setServiceKey, type SettingItem } from "@/lib/api";
import { clamp } from "@/lib/format";
import {
  ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY,
  DEFAULT_TEST_MODEL,
  TEST_MODEL_SELECT_OPTIONS,
  USER_TOKEN_PROBE_MODEL_SETTING_KEY,
  testModelFromSettings,
  testModelSettingPayload,
  type TestModel,
} from "@/lib/test-models";
import { EmptyState, ErrorAlert, LoadingState, SelectField } from "@/shared/components";
import { errorMessage } from "@/shared/domain";
import type { ToastMessage } from "@/shared/types";

export function UserSettingsPage({
  pushToast,
  refreshNonce,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [probeModel, setProbeModel] = useState<TestModel>(DEFAULT_TEST_MODEL);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const loadSettings = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const payload = await api.mySettings();
      setProbeModel(testModelFromSettings(payload.items || [], USER_TOKEN_PROBE_MODEL_SETTING_KEY));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadSettings();
  }, [loadSettings, refreshNonce]);

  async function saveProbeModel() {
    setSaving(true);
    try {
      await api.updateMySetting(USER_TOKEN_PROBE_MODEL_SETTING_KEY, testModelSettingPayload(probeModel));
      pushToast("默认测试模型已保存");
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(320px,.65fr)_minmax(0,1fr)]">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings2Icon className="size-5" />
            测试设置
          </CardTitle>
          <CardDescription>设置用户页面 Key 测试按钮默认使用的模型。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error && <ErrorAlert title="设置载入失败" message={error} />}
          {loading && !error ? (
            <LoadingState compact label="正在载入设置" />
          ) : (
            <>
              <SelectField
                label="默认测试模型"
                onChange={(value) => setProbeModel(value as TestModel)}
                options={TEST_MODEL_SELECT_OPTIONS}
                value={probeModel}
              />
              <Button className="w-fit" disabled={saving} loading={saving} onClick={() => void saveProbeModel()}>
                <SaveIcon />
                保存测试设置
              </Button>
            </>
          )}
        </CardPanel>
      </Card>
    </div>
  );
}

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
  const [adminProbeModel, setAdminProbeModel] = useState<TestModel>(DEFAULT_TEST_MODEL);
  const [serviceKeyDraft, setServiceKeyDraft] = useState(() => getServiceKey());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [adminProbeModelSaving, setAdminProbeModelSaving] = useState(false);

  const loadTokenSelection = useCallback(async () => {
    const payload = await api.tokenSelection();
    const cap = Number(payload.active_stream_cap || 10);
    setSelectionSummary(String(payload.strategy || "snapshot_round_robin"));
    onStreamCapChange(Number.isFinite(cap) ? clamp(cap, 1, 50) : 10);
  }, [onStreamCapChange]);

  const loadSettings = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [settingsPayload] = await Promise.all([api.settings(), loadTokenSelection()]);
      const nextItems = settingsPayload.items || [];
      setItems(nextItems);
      setAdminProbeModel(testModelFromSettings(nextItems, ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY));
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

  async function saveAdminProbeModel() {
    setAdminProbeModelSaving(true);
    try {
      await api.updateSetting(ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY, testModelSettingPayload(adminProbeModel));
      pushToast("管理员测试模型已保存");
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setAdminProbeModelSaving(false);
    }
  }

  function saveServiceKey() {
    setServiceKey(serviceKeyDraft);
    pushToast("Service API Key 已保存");
  }

  function clearServiceKey() {
    setServiceKey("");
    setServiceKeyDraft("");
    pushToast("Service API Key 已清空", "info");
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(320px,.7fr)_minmax(0,1fr)]">
      <div className="grid gap-4">
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
                max={50}
                min={1}
                nativeInput
                onChange={(event) => onStreamCapChange(clamp(Number(event.currentTarget.value || 1), 1, 50))}
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
            <CardTitle className="flex items-center gap-2">
              <Settings2Icon className="size-5" />
              测试设置
            </CardTitle>
            <CardDescription>管理员号池总览和 Key 详情测试按钮默认使用的模型。</CardDescription>
          </CardHeader>
          <CardPanel className="grid gap-4">
            <SelectField
              label="默认测试模型"
              onChange={(value) => setAdminProbeModel(value as TestModel)}
              options={TEST_MODEL_SELECT_OPTIONS}
              value={adminProbeModel}
            />
            <Button className="w-fit" disabled={adminProbeModelSaving} loading={adminProbeModelSaving} onClick={() => void saveAdminProbeModel()}>
              <SaveIcon />
              保存测试设置
            </Button>
          </CardPanel>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <ShieldCheckIcon className="size-5" />
              Service API Key
            </CardTitle>
            <CardDescription>保存本浏览器访问受保护接口使用的管理员凭证。</CardDescription>
          </CardHeader>
          <CardPanel className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="settings-service-key">Service API Key</Label>
              <Input
                id="settings-service-key"
                nativeInput
                onChange={(event) => setServiceKeyDraft(event.currentTarget.value)}
                placeholder="oaix_service_..."
                type="password"
                value={serviceKeyDraft}
              />
            </div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={saveServiceKey}>
                <SaveIcon />
                保存凭证
              </Button>
              <Button onClick={clearServiceKey} variant="outline">
                清空
              </Button>
            </div>
          </CardPanel>
        </Card>
      </div>

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
