export const DEFAULT_TEST_MODEL = "gpt-5.4-mini";

export const TEST_MODEL_OPTIONS = [
  "gpt-5.4-mini",
  "gpt-5.4",
  "gpt-5.5",
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
] as const;

export type TestModel = (typeof TEST_MODEL_OPTIONS)[number];

export const USER_TOKEN_PROBE_MODEL_SETTING_KEY = "token_probe_model";
export const ADMIN_TOKEN_PROBE_MODEL_SETTING_KEY = "admin_token_probe_model";

export const TEST_MODEL_SELECT_OPTIONS = TEST_MODEL_OPTIONS.map((model) => ({
  label: model,
  value: model,
}));

export function normalizeTestModel(value: unknown): TestModel {
  const raw = String(value || "").trim();
  return TEST_MODEL_OPTIONS.includes(raw as TestModel) ? (raw as TestModel) : DEFAULT_TEST_MODEL;
}

export function testModelSettingPayload(model: TestModel): { model: TestModel } {
  return { model: normalizeTestModel(model) };
}

export function testModelFromSettingValue(value: unknown): TestModel {
  if (typeof value === "string") {
    return normalizeTestModel(value);
  }
  if (value && typeof value === "object" && "model" in value) {
    return normalizeTestModel((value as { model?: unknown }).model);
  }
  return DEFAULT_TEST_MODEL;
}

export function testModelFromSettings(items: Array<{ key: string; value: unknown }>, key: string): TestModel {
  const item = items.find((candidate) => candidate.key === key);
  return item ? testModelFromSettingValue(item.value) : DEFAULT_TEST_MODEL;
}
