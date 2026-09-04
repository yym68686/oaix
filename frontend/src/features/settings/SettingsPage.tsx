import { DatabaseIcon, RefreshCwIcon, RotateCcwIcon, SaveIcon, Settings2Icon, ShieldCheckIcon, TimerResetIcon } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/registry/default/ui/alert";
import { Button } from "@/registry/default/ui/button";
import { Card, CardAction, CardDescription, CardHeader, CardPanel, CardTitle } from "@/registry/default/ui/card";
import { Checkbox } from "@/registry/default/ui/checkbox";
import { Input } from "@/registry/default/ui/input";
import { Label } from "@/registry/default/ui/label";
import { Textarea } from "@/registry/default/ui/textarea";
import { cn } from "@/registry/default/lib/utils";
import {
  api,
  getServiceKey,
  setServiceKey,
  type Ordinary429CooldownSettings,
  type SettingItem,
  type TokenConcurrencyPlan,
  type TokenModelAccessPlan,
  type TokenModelAccessSettings,
} from "@/lib/api";
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

const DEFAULT_ORDINARY_429_COOLDOWN_SECONDS = 300;
const MIN_ORDINARY_429_COOLDOWN_SECONDS = 1;
const MAX_ORDINARY_429_COOLDOWN_SECONDS = 86_400;
const ORDINARY_429_COOLDOWN_SETTING_KEY = "ordinary_429_cooldown";
const numberFormatter = new Intl.NumberFormat("zh-CN");

function formatCooldownDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "5 分钟";
  if (seconds % 3600 === 0) return `${numberFormatter.format(seconds / 3600)} 小时`;
  if (seconds % 60 === 0) return `${numberFormatter.format(seconds / 60)} 分钟`;
  return `${numberFormatter.format(seconds)} 秒`;
}

export function UserSettingsPage({
  pushToast,
  refreshNonce,
}: {
  pushToast: (title: string, variant?: ToastMessage["variant"]) => void;
  refreshNonce: number;
}) {
  const [probeModel, setProbeModel] = useState<TestModel>(DEFAULT_TEST_MODEL);
  const [concurrencyPlans, setConcurrencyPlans] = useState<TokenConcurrencyPlan[]>([]);
  const [concurrencyOverrides, setConcurrencyOverrides] = useState<Record<string, number>>({});
  const [globalConcurrency, setGlobalConcurrency] = useState(10);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [savingConcurrency, setSavingConcurrency] = useState(false);
  const [error, setError] = useState("");
  const concurrencyDirtyRef = useRef(false);
  const [modelSettings, setModelSettings] = useState<TokenModelAccessSettings | null>(null);
  const [modelOverrides, setModelOverrides] = useState<Record<string, string[]>>({});
  const [savingModels, setSavingModels] = useState(false);
  const modelDirtyRef = useRef(false);

  const loadSettings = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [payload, concurrency, models] = await Promise.all([api.mySettings(), api.myTokenConcurrency(), api.myTokenModels()]);
      setProbeModel(testModelFromSettings(payload.items || [], USER_TOKEN_PROBE_MODEL_SETTING_KEY));
      setModelSettings(models);
      if (!modelDirtyRef.current) {
        setModelOverrides(Object.fromEntries((models.plans || []).filter((plan) => plan.overridden).map((plan) => [plan.plan, plan.models || []])));
      }
      if (!concurrencyDirtyRef.current) {
        setConcurrencyPlans(concurrency.plans || []);
        setGlobalConcurrency(clamp(Number(concurrency.global_active_stream_cap || 10), 1, 50));
        setConcurrencyOverrides(
          Object.fromEntries(
            (concurrency.plans || [])
              .filter((plan) => plan.overridden)
              .map((plan) => [plan.plan, clamp(Number(plan.active_stream_cap || 1), 1, 50)]),
          ),
        );
      }
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

  async function saveConcurrency() {
    setSavingConcurrency(true);
    try {
      await api.updateMyTokenConcurrency(concurrencyOverrides);
      pushToast("计划并发设置已保存");
      concurrencyDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setSavingConcurrency(false);
    }
  }

  async function resetConcurrency() {
    setSavingConcurrency(true);
    try {
      await api.resetMyTokenConcurrency();
      pushToast("已恢复管理员默认并发", "info");
      concurrencyDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setSavingConcurrency(false);
    }
  }

  async function saveModels() {
    setSavingModels(true);
    try {
      await api.updateMyTokenModels(modelOverrides);
      pushToast("计划模型设置已保存");
      modelDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setSavingModels(false);
    }
  }

  async function resetModels() {
    setSavingModels(true);
    try {
      await api.resetMyTokenModels();
      pushToast("已恢复管理员默认模型", "info");
      modelDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setSavingModels(false);
    }
  }

  function toggleModelOverride(plan: TokenModelAccessPlan, enabled: boolean) {
    modelDirtyRef.current = true;
    setModelOverrides((current) => {
      const next = { ...current };
      if (enabled) {
        next[plan.plan] = [...(plan.models || [])];
      } else {
        delete next[plan.plan];
      }
      return next;
    });
  }

  function toggleModel(plan: TokenModelAccessPlan, model: string, checked: boolean) {
    modelDirtyRef.current = true;
    setModelOverrides((current) => {
      const selected = new Set(current[plan.plan] || plan.models || []);
      if (checked) selected.add(model);
      else selected.delete(model);
      return { ...current, [plan.plan]: [...selected].sort() };
    });
  }

  function toggleConcurrencyOverride(plan: TokenConcurrencyPlan, enabled: boolean) {
    concurrencyDirtyRef.current = true;
    setConcurrencyOverrides((current) => {
      const next = { ...current };
      if (enabled) {
        next[plan.plan] = clamp(Number(plan.active_stream_cap || globalConcurrency), 1, 50);
      } else {
        delete next[plan.plan];
      }
      return next;
    });
  }

  return (
    <div className="grid min-w-0 gap-4">
      <Card className="min-w-0">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings2Icon className="size-5" />
            计划并发
          </CardTitle>
          <CardDescription>按账号计划设置每个 Key 的并发上限。你的设置优先于管理员默认值。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
          {error && <ErrorAlert title="设置载入失败" message={error} />}
          {loading && !error ? (
            <LoadingState compact label="正在载入并发设置" />
          ) : (
            <>
              <Alert variant="info">
                <DatabaseIcon />
                <AlertTitle>管理员默认：每 Key {globalConcurrency} 并发</AlertTitle>
                <AlertDescription>关闭某个计划的“自定义”后，该计划会自动继承管理员默认值。</AlertDescription>
              </Alert>
              <div className="grid min-w-0 gap-3 md:grid-cols-2">
                {concurrencyPlans.map((plan) => {
                  const overridden = Object.hasOwn(concurrencyOverrides, plan.plan);
                  const cap = overridden ? concurrencyOverrides[plan.plan] : globalConcurrency;
                  return (
                    <div className="grid min-w-0 gap-3 rounded-lg border bg-muted/30 p-3" key={plan.plan}>
                      <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-3">
                        <div className="min-w-0">
                          <div className="font-medium [overflow-wrap:anywhere]">{plan.label || plan.plan}</div>
                          <div className="text-muted-foreground text-xs">{plan.token_count} 个 Key</div>
                        </div>
                        <Label className="flex min-h-11 shrink-0 items-center gap-2 text-xs sm:min-h-7">
                          <Checkbox checked={overridden} onCheckedChange={(value) => toggleConcurrencyOverride(plan, Boolean(value))} />
                          自定义
                        </Label>
                      </div>
                      <div className="grid gap-2">
                        <Label htmlFor={`plan-concurrency-${plan.plan}`}>每 Key 并发</Label>
                        <Input
                          disabled={!overridden}
                          id={`plan-concurrency-${plan.plan}`}
                          max={50}
                          min={1}
                          nativeInput
                          onChange={(event) => {
                            concurrencyDirtyRef.current = true;
                            setConcurrencyOverrides((current) => ({
                              ...current,
                              [plan.plan]: clamp(Number(event.currentTarget.value || 1), 1, 50),
                            }));
                          }}
                          type="number"
                          value={cap}
                        />
                      </div>
                    </div>
                  );
                })}
                {!concurrencyPlans.length && <EmptyState compact title="暂无计划" description="当前账号还没有可配置并发的 Key 计划。" />}
              </div>
              <div className="grid gap-2 sm:flex sm:flex-wrap">
                <Button className="w-full sm:w-auto" disabled={savingConcurrency} loading={savingConcurrency} onClick={() => void saveConcurrency()}>
                  <SaveIcon />
                  保存并发设置
                </Button>
                <Button className="w-full sm:w-auto" disabled={savingConcurrency} onClick={() => void resetConcurrency()} variant="outline">
                  <RotateCcwIcon />
                  全部恢复默认
                </Button>
              </div>
            </>
          )}
        </CardPanel>
      </Card>
      <TokenModelAccessPanel
        modelOverrides={modelOverrides}
        onReset={() => void resetModels()}
        onSave={() => void saveModels()}
        onToggleModel={toggleModel}
        onToggleOverride={toggleModelOverride}
        error={error}
        loading={loading}
        saving={savingModels}
        settings={modelSettings}
        userMode
      />
      <Card className="min-w-0">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings2Icon className="size-5" />
            测试设置
          </CardTitle>
          <CardDescription>设置用户页面 Key 测试按钮默认使用的模型。</CardDescription>
        </CardHeader>
        <CardPanel className="grid gap-4">
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

function TokenModelAccessPanel({
  modelOverrides,
  error,
  loading,
  onReset,
  onSave,
  onToggleModel,
  onToggleOverride,
  saving,
  settings,
  userMode = false,
}: {
  modelOverrides: Record<string, string[]>;
  error: string;
  loading: boolean;
  onReset: () => void;
  onSave: () => void;
  onToggleModel: (plan: TokenModelAccessPlan, model: string, checked: boolean) => void;
  onToggleOverride: (plan: TokenModelAccessPlan, enabled: boolean) => void;
  saving: boolean;
  settings: TokenModelAccessSettings | null;
  userMode?: boolean;
}) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2Icon className="size-5" />
          计划模型
        </CardTitle>
        <CardDescription>{userMode ? "按计划选择用户可使用的模型。用户设置优先于管理员默认值。" : "按计划选择默认可用模型，用户可以在自己的设置中覆盖。"}</CardDescription>
      </CardHeader>
      <CardPanel className="grid min-w-0 gap-4">
        {!settings ? (
          loading ? <LoadingState compact label="正在载入模型设置" /> : <ErrorAlert title="模型设置载入失败" message={error || "未返回模型设置"} />
        ) : (
          <>
            <Alert variant="info">
              <DatabaseIcon />
              <AlertTitle>{userMode ? "用户模型覆盖" : "管理员模型默认"}</AlertTitle>
              <AlertDescription>{userMode ? "关闭自定义会继承管理员设置；灰色模型表示当前计划目录没有返回该模型。" : "未勾选的模型不会被该计划的 token 调度。"}</AlertDescription>
            </Alert>
            <div className="grid min-w-0 gap-3 md:grid-cols-2">
              {(settings.plans || []).map((plan) => {
                const overridden = Object.hasOwn(modelOverrides, plan.plan);
                const selected = new Set(overridden ? modelOverrides[plan.plan] : plan.models || []);
                const modelIDs = [
                  ...new Set([
                    ...(settings.models || []).map((model) => model.id),
                    ...(plan.available_models || []),
                    ...(plan.models || []),
                    ...(plan.inherited_models || []),
                  ]),
                ].sort();
                return (
                  <div className="grid min-w-0 gap-3 rounded-lg border bg-muted/30 p-3" key={plan.plan}>
                    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-3">
                      <div className="min-w-0">
                        <div className="font-medium [overflow-wrap:anywhere]">{plan.label || plan.plan}</div>
                        <div className="text-muted-foreground text-xs">{plan.token_count} 个 Key</div>
                      </div>
                      <Label className="flex min-h-11 shrink-0 items-center gap-2 text-xs sm:min-h-7">
                        <Checkbox checked={overridden} onCheckedChange={(value) => onToggleOverride(plan, Boolean(value))} />
                        自定义
                      </Label>
                    </div>
                    <div className="grid min-w-0 gap-1">
                      {modelIDs.map((model) => {
                        const available = (plan.available_models || []).includes(model);
                        return (
                          <Label className="flex min-h-11 min-w-0 items-start gap-2 rounded-md px-2 py-2 text-xs hover:bg-muted/60 sm:min-h-8 sm:py-1.5" key={model}>
                            <Checkbox checked={selected.has(model)} disabled={!overridden || (!available && !selected.has(model))} onCheckedChange={(value) => onToggleModel(plan, model, Boolean(value))} />
                            <span className="min-w-0 [overflow-wrap:anywhere]">{model}</span>
                          </Label>
                        );
                      })}
                      {!modelIDs.length && <div className="text-muted-foreground text-xs">暂无已知模型</div>}
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="grid gap-2 sm:flex sm:flex-wrap">
              <Button className="w-full sm:w-auto" disabled={saving} loading={saving} onClick={onSave}>
                <SaveIcon />
                保存计划模型
              </Button>
              <Button className="w-full sm:w-auto" disabled={saving} onClick={onReset} variant="outline">
                <RotateCcwIcon />
                全部恢复默认
              </Button>
            </div>
          </>
        )}
      </CardPanel>
    </Card>
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
  const [adminModelSettings, setAdminModelSettings] = useState<TokenModelAccessSettings | null>(null);
  const [adminModelOverrides, setAdminModelOverrides] = useState<Record<string, string[]>>({});
  const [adminModelsSaving, setAdminModelsSaving] = useState(false);
  const adminModelsDirtyRef = useRef(false);
  const [ordinary429Settings, setOrdinary429Settings] = useState<Ordinary429CooldownSettings | null>(null);
  const [ordinary429Draft, setOrdinary429Draft] = useState(String(DEFAULT_ORDINARY_429_COOLDOWN_SECONDS));
  const [ordinary429Saving, setOrdinary429Saving] = useState(false);
  const [ordinary429Error, setOrdinary429Error] = useState("");
  const [ordinary429Dirty, setOrdinary429Dirty] = useState(false);
  const ordinary429DirtyRef = useRef(false);
  const ordinary429InputRef = useRef<HTMLInputElement>(null);

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
      const [settingsPayload, , models, cooldown] = await Promise.all([
        api.settings(),
        loadTokenSelection(),
        api.adminTokenModels(),
        api.ordinary429Cooldown(),
      ]);
      const nextItems = settingsPayload.items || [];
      setItems(nextItems);
      setAdminProbeModel(testModelFromSettings(nextItems, ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY));
      setAdminModelSettings(models);
      if (!adminModelsDirtyRef.current) {
        setAdminModelOverrides(Object.fromEntries((models.plans || []).filter((plan) => plan.overridden).map((plan) => [plan.plan, plan.models || []])));
      }
      setOrdinary429Settings(cooldown);
      if (!ordinary429DirtyRef.current) {
        setOrdinary429Draft(String(cooldown.cooldown_seconds || DEFAULT_ORDINARY_429_COOLDOWN_SECONDS));
        setOrdinary429Error("");
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(false);
    }
  }, [loadTokenSelection]);

  useEffect(() => {
    void loadSettings();
  }, [loadSettings, refreshNonce]);

  useEffect(() => {
    if (!ordinary429Dirty) return;
    const warnAboutUnsavedCooldown = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warnAboutUnsavedCooldown);
    return () => window.removeEventListener("beforeunload", warnAboutUnsavedCooldown);
  }, [ordinary429Dirty]);

  async function saveStreamCap() {
    await api.updateTokenSelection({ active_stream_cap: streamCap });
    pushToast("调度设置已保存");
    await loadTokenSelection();
  }

  function parsedOrdinary429Cooldown(): number | null {
    const value = Number(ordinary429Draft);
    if (!Number.isInteger(value) || value < MIN_ORDINARY_429_COOLDOWN_SECONDS || value > MAX_ORDINARY_429_COOLDOWN_SECONDS) {
      return null;
    }
    return value;
  }

  async function saveOrdinary429Cooldown() {
    const cooldownSeconds = parsedOrdinary429Cooldown();
    if (cooldownSeconds == null) {
      setOrdinary429Error("请输入 1–86,400 之间的整数秒数。");
      ordinary429InputRef.current?.focus();
      return;
    }
    setOrdinary429Saving(true);
    setOrdinary429Error("");
    try {
      const saved = await api.updateOrdinary429Cooldown(cooldownSeconds);
      setOrdinary429Settings(saved);
      setOrdinary429Draft(String(saved.cooldown_seconds));
      ordinary429DirtyRef.current = false;
      setOrdinary429Dirty(false);
      setItems((current) => {
        const item = { key: ORDINARY_429_COOLDOWN_SETTING_KEY, value: { cooldown_seconds: saved.cooldown_seconds }, updated_at: saved.updated_at };
        return [...current.filter((candidate) => candidate.key !== ORDINARY_429_COOLDOWN_SETTING_KEY), item].sort((left, right) => left.key.localeCompare(right.key));
      });
      pushToast(`普通 429 冷却已设为 ${formatCooldownDuration(saved.cooldown_seconds)}`);
    } catch (caught) {
      const message = errorMessage(caught);
      setOrdinary429Error(message);
      pushToast(message, "error");
    } finally {
      setOrdinary429Saving(false);
    }
  }

  async function resetOrdinary429Cooldown() {
    setOrdinary429Saving(true);
    setOrdinary429Error("");
    try {
      const reset = await api.resetOrdinary429Cooldown();
      setOrdinary429Settings(reset);
      setOrdinary429Draft(String(reset.cooldown_seconds));
      ordinary429DirtyRef.current = false;
      setOrdinary429Dirty(false);
      setItems((current) => current.filter((item) => item.key !== ORDINARY_429_COOLDOWN_SETTING_KEY));
      pushToast(`已恢复默认 ${formatCooldownDuration(reset.cooldown_seconds)}`, "info");
    } catch (caught) {
      const message = errorMessage(caught);
      setOrdinary429Error(message);
      pushToast(message, "error");
    } finally {
      setOrdinary429Saving(false);
    }
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

  async function saveAdminModels() {
    setAdminModelsSaving(true);
    try {
      await api.updateAdminTokenModels(adminModelOverrides);
      pushToast("管理员计划模型已保存");
      adminModelsDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setAdminModelsSaving(false);
    }
  }

  async function resetAdminModels() {
    setAdminModelsSaving(true);
    try {
      await api.resetAdminTokenModels();
      pushToast("已恢复内置模型默认值", "info");
      adminModelsDirtyRef.current = false;
      await loadSettings();
    } catch (caught) {
      pushToast(errorMessage(caught), "error");
    } finally {
      setAdminModelsSaving(false);
    }
  }

  function toggleAdminModelOverride(plan: TokenModelAccessPlan, enabled: boolean) {
    adminModelsDirtyRef.current = true;
    setAdminModelOverrides((current) => {
      const next = { ...current };
      if (enabled) next[plan.plan] = [...(plan.models || [])];
      else delete next[plan.plan];
      return next;
    });
  }

  function toggleAdminModel(plan: TokenModelAccessPlan, model: string, checked: boolean) {
    adminModelsDirtyRef.current = true;
    setAdminModelOverrides((current) => {
      const selected = new Set(current[plan.plan] || plan.models || []);
      if (checked) selected.add(model);
      else selected.delete(model);
      return { ...current, [plan.plan]: [...selected].sort() };
    });
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
    <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(320px,.7fr)_minmax(0,1fr)]">
      <div className="min-w-0 xl:col-span-2">
        <TokenModelAccessPanel
          modelOverrides={adminModelOverrides}
          onReset={() => void resetAdminModels()}
          onSave={() => void saveAdminModels()}
          onToggleModel={toggleAdminModel}
          onToggleOverride={toggleAdminModelOverride}
          error={error}
          loading={loading}
          saving={adminModelsSaving}
          settings={adminModelSettings}
        />
      </div>
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
              <TimerResetIcon aria-hidden="true" className="size-5" />
              普通 429 冷却
            </CardTitle>
            <CardDescription>上游返回普通 429 且没有明确额度重置时间时，暂时停止调度该 Key。</CardDescription>
          </CardHeader>
          <CardPanel className="grid gap-4">
            {!ordinary429Settings ? (
              loading ? <LoadingState compact label="正在载入 429 冷却设置" /> : <ErrorAlert title="429 冷却设置载入失败" message={error || "未返回冷却设置"} />
            ) : (
              <>
                <Alert variant="info">
                  <DatabaseIcon aria-hidden="true" />
                  <AlertTitle>
                    当前生效：{formatCooldownDuration(ordinary429Settings?.cooldown_seconds || DEFAULT_ORDINARY_429_COOLDOWN_SECONDS)}
                  </AlertTitle>
                  <AlertDescription>
                    {ordinary429Settings?.overridden ? "当前使用管理员自定义值。" : "当前使用默认值。"} 保存后立即应用到新发生的普通 429。
                  </AlertDescription>
                </Alert>
                <div className="grid gap-2">
                  <Label htmlFor="ordinary-429-cooldown-seconds">冷却时间（秒）</Label>
                  <Input
                    aria-describedby={ordinary429Error ? "ordinary-429-cooldown-help ordinary-429-cooldown-error" : "ordinary-429-cooldown-help"}
                    aria-invalid={Boolean(ordinary429Error)}
                    autoComplete="off"
                    id="ordinary-429-cooldown-seconds"
                    inputMode="numeric"
                    max={MAX_ORDINARY_429_COOLDOWN_SECONDS}
                    min={MIN_ORDINARY_429_COOLDOWN_SECONDS}
                    name="ordinary_429_cooldown_seconds"
                    nativeInput
                    onChange={(event) => {
                      ordinary429DirtyRef.current = true;
                      setOrdinary429Dirty(true);
                      setOrdinary429Draft(event.currentTarget.value);
                      setOrdinary429Error("");
                    }}
                    ref={ordinary429InputRef}
                    step={1}
                    type="number"
                    value={ordinary429Draft}
                  />
                  <p className="text-muted-foreground text-xs" id="ordinary-429-cooldown-help">
                    可设置 1 秒到 24 小时。默认 300 秒（5 分钟）；已在冷却中的 Key 不会被追溯修改。
                  </p>
                  {ordinary429Error ? (
                    <p className="text-destructive text-sm" id="ordinary-429-cooldown-error" role="alert">
                      {ordinary429Error}
                    </p>
                  ) : null}
                </div>
                <div className="grid gap-2 sm:flex sm:flex-wrap">
                  <Button className="w-full sm:w-auto" disabled={ordinary429Saving} loading={ordinary429Saving} onClick={() => void saveOrdinary429Cooldown()}>
                    <SaveIcon aria-hidden="true" />
                    保存 429 冷却
                  </Button>
                  <Button className="w-full sm:w-auto" disabled={ordinary429Saving} onClick={() => void resetOrdinary429Cooldown()} variant="outline">
                    <RotateCcwIcon aria-hidden="true" />
                    恢复默认 {formatCooldownDuration(ordinary429Settings?.default_cooldown_seconds || DEFAULT_ORDINARY_429_COOLDOWN_SECONDS)}
                  </Button>
                </div>
              </>
            )}
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
