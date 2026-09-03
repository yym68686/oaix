package httpapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFrontendAdminNavigationContract(t *testing.T) {
	appShell := readFrontendFile(t, "src", "app", "AppShell.tsx")
	if !strings.Contains(appShell, "const admin = isAdminPrincipal(me)") {
		t.Fatal("AppShell must derive admin visibility from authenticated principal")
	}
	if !strings.Contains(appShell, "group.items.filter((item) => !item.adminOnly || admin)") {
		t.Fatal("AppShell must hide adminOnly navigation for non-admin users")
	}
	for _, required := range []string{
		`{ adminOnly: true, key: "admin_users", href: "/admin/users"`,
		`label: "用户状态"`,
		`{ adminOnly: true, key: "admin_pools", href: "/admin/pools"`,
		`label: "号池总览"`,
		`{ adminOnly: true, key: "admin_imports", href: "/admin/imports"`,
		`label: "全局导入"`,
	} {
		if !strings.Contains(appShell, required) {
			t.Fatalf("AppShell admin navigation contract missing %q", required)
		}
	}
}

func TestFrontendUserAPIKeyPageContract(t *testing.T) {
	appShell := readFrontendFile(t, "src", "app", "AppShell.tsx")
	router := readFrontendFile(t, "src", "app", "router.ts")
	app := readFrontendFile(t, "src", "App.tsx")
	page := readFrontendFile(t, "src", "features", "account", "APIKeysPage.tsx")
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	for _, required := range []string{
		`{ key: "account_api_keys", href: "/account/api-keys"`,
		`label: "API Key"`,
	} {
		if !strings.Contains(appShell, required) {
			t.Fatalf("API Key navigation contract missing %q", required)
		}
	}
	if !strings.Contains(router, `return { key: "account_api_keys"`) {
		t.Fatal("router must preserve /account/api-keys instead of redirecting it")
	}
	if !strings.Contains(app, `route.key === "account_api_keys"`) || !strings.Contains(app, "<AccountAPIKeysPage") {
		t.Fatal("App must render the user API Key page")
	}
	for _, required := range []string{
		"api.createMyAPIKey",
		"api.revealMyAPIKey",
		"api.revokeMyAPIKey",
		"只能调用你自己添加的",
		"复制",
		"删除",
	} {
		if !strings.Contains(page, required) {
			t.Fatalf("API Key page contract missing %q", required)
		}
	}
	if !strings.Contains(apiFile, "/api/me/api-keys/${id}/value") {
		t.Fatal("frontend API client must expose recoverable API key value endpoint")
	}
}

func TestFrontendUserSettingsExposePlanConcurrencyOverrides(t *testing.T) {
	page := readFrontendFile(t, "src", "features", "settings", "SettingsPage.tsx")
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	for _, required := range []string{
		"计划并发",
		"你的设置优先于管理员默认值",
		"api.myTokenConcurrency()",
		"api.updateMyTokenConcurrency(concurrencyOverrides)",
		"api.resetMyTokenConcurrency()",
		"const concurrencyDirtyRef = useRef(false)",
		"if (!concurrencyDirtyRef.current)",
		"全部恢复默认",
	} {
		if !strings.Contains(page, required) {
			t.Fatalf("user settings concurrency contract missing %q", required)
		}
	}
	for _, required := range []string{
		`myTokenConcurrency: () => requestJSON<TokenConcurrencySettings>("/api/me/token-concurrency")`,
		`postJSON<TokenConcurrencySettings>("/api/me/token-concurrency"`,
		`deleteJSON<TokenConcurrencySettings>("/api/me/token-concurrency")`,
	} {
		if !strings.Contains(apiFile, required) {
			t.Fatalf("frontend concurrency API contract missing %q", required)
		}
	}
}

func TestFrontendSettingsExposePlanModelOverrides(t *testing.T) {
	page := readFrontendFile(t, "src", "features", "settings", "SettingsPage.tsx")
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	for _, required := range []string{
		"计划模型",
		"用户设置优先于管理员默认值",
		"api.myTokenModels()",
		"api.updateMyTokenModels(modelOverrides)",
		"api.resetMyTokenModels()",
		"api.adminTokenModels()",
		"api.updateAdminTokenModels(adminModelOverrides)",
		"关闭自定义会继承管理员设置",
	} {
		if !strings.Contains(page, required) {
			t.Fatalf("user settings model access contract missing %q", required)
		}
	}
	for _, required := range []string{
		`myTokenModels: () => requestJSON<TokenModelAccessSettings>("/api/me/token-models")`,
		`postJSON<TokenModelAccessSettings>("/api/me/token-models"`,
		`deleteJSON<TokenModelAccessSettings>("/api/me/token-models")`,
		`adminTokenModels: () => requestJSON<TokenModelAccessSettings>("/admin/token-models")`,
	} {
		if !strings.Contains(apiFile, required) {
			t.Fatalf("frontend model access API contract missing %q", required)
		}
	}
}

func TestFrontendProfileSupportsSavedAccountSwitching(t *testing.T) {
	appShell := readFrontendFile(t, "src", "app", "AppShell.tsx")
	router := readFrontendFile(t, "src", "app", "router.ts")
	app := readFrontendFile(t, "src", "App.tsx")
	profile := readFrontendFile(t, "src", "features", "account", "ProfilePage.tsx")
	accounts := readFrontendFile(t, "src", "lib", "accounts.ts")
	for _, required := range []string{
		`{ key: "account_profile", href: "/account/profile"`,
		`accounts.length > 1`,
		`<MenuGroupLabel>切换账号</MenuGroupLabel>`,
		`onSwitchAccount={switchAccount}`,
		`authBlocked && protectedMode && savedAccounts.length === 0`,
		`退出当前账号`,
	} {
		if !strings.Contains(appShell, required) {
			t.Fatalf("account menu switching contract missing %q", required)
		}
	}
	if !strings.Contains(router, `return { key: "account_profile"`) {
		t.Fatal("router must expose /account/profile")
	}
	if !strings.Contains(router, `navigateTo("/account/profile", { replace: true })`) {
		t.Fatal("legacy /account route must redirect to the personal profile page")
	}
	if !strings.Contains(app, `route.key === "account_profile"`) || !strings.Contains(app, `<ProfilePage`) {
		t.Fatal("App must render the personal profile page")
	}
	for _, required := range []string{
		`添加账号`,
		`api.login`,
		`rememberPasswordAuthAccount`,
		`activateSavedAccount`,
		`removeSavedAccount`,
		`不会保存密码`,
	} {
		if !strings.Contains(profile, required) {
			t.Fatalf("profile account management contract missing %q", required)
		}
	}
	for _, required := range []string{
		`oaix.savedAccounts.v1`,
		`export function rememberSavedAccount`,
		`export function activateSavedAccount`,
		`export function removeSavedAccount`,
		`export function restoreSavedAccount`,
	} {
		if !strings.Contains(accounts, required) {
			t.Fatalf("saved account storage contract missing %q", required)
		}
	}
}

func TestFrontendAccountSwitchInvalidatesStaleAuthorization(t *testing.T) {
	app := readFrontendFile(t, "src", "App.tsx")
	appShell := readFrontendFile(t, "src", "app", "AppShell.tsx")
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")

	for _, required := range []string{
		`const refreshIDRef = useRef(0)`,
		`const validatedCredentialRef = useRef("")`,
		`const credential = getServiceKey().trim()`,
		`refreshID === refreshIDRef.current && getServiceKey().trim() === credential`,
		`validatedCredentialRef.current !== credential`,
		`setAuthContext(null)`,
		`setMe(null)`,
		`setCounts({})`,
		`api.me(credential)`,
		`rememberSavedAccount(credential, mePayload, false)`,
		`api.myPoolSummary(credential)`,
		`api.tokenSelection(credential)`,
		`if (adminRoute && !admin)`,
	} {
		if !strings.Contains(app, required) {
			t.Fatalf("account switch authorization contract missing %q", required)
		}
	}
	if strings.Contains(app, `if (adminRoute && !admin && !loading)`) {
		t.Fatal("admin routes must not render while the current credential is still unverified")
	}
	if !strings.Contains(appShell, `const activeSavedAccount = getActiveSavedAccount()`) {
		t.Fatal("account menu must resolve the active account from the current credential on every parent render")
	}
	if !strings.Contains(apiFile, `authKey === undefined ? getServiceKey() : authKey.trim()`) {
		t.Fatal("identity refresh requests must be bound to the credential captured at refresh start")
	}
}

func TestFrontendAdminPagesContract(t *testing.T) {
	adminPages := readFrontendFile(t, "src", "features", "admin", "AdminPages.tsx")
	for _, required := range []string{
		"api.adminUsers",
		"api.adminPoolSummaryByUser",
		"UserDetailPage",
		"KeyListPage",
		`apiScope: "admin"`,
		"ownerFilterOptions",
		"AdminImportsPage",
		"api.importJobs(120, \"admin\")",
		"api.adminUserImportJobs",
		`detailBasePath: "/admin/pools"`,
	} {
		if !strings.Contains(adminPages, required) {
			t.Fatalf("AdminPages must expose user status and admin pool key list data path %q", required)
		}
	}
}

func TestFrontendSub2APIUserSelectorLoadsEveryActiveUserPage(t *testing.T) {
	sub2APIPage := readFrontendFile(t, "src", "features", "admin", "Sub2APIPage.tsx")
	for _, required := range []string{
		"loadActiveUserPlanCatalog()",
		"SUB2API_USER_PAGE_LIMIT = 500",
		`include_usage: "false"`,
		"if (!pagination?.has_next) break",
		"pagination.offset + pagination.returned",
		`Number(right.role === "admin") - Number(left.role === "admin")`,
		`{ label: "请选择 OAIX 用户（管理员优先）", value: "0" }`,
	} {
		if !strings.Contains(sub2APIPage, required) {
			t.Fatalf("Sub2API owner selector must load the complete active-user catalog: missing %q", required)
		}
	}
	if strings.Contains(sub2APIPage, `new URLSearchParams({ limit: "200", status: "active" })`) {
		t.Fatal("Sub2API owner selector must not truncate active users to its first 200 results")
	}
}

func TestFrontendKeysPageUsesSelfScope(t *testing.T) {
	keysPage := readFrontendFile(t, "src", "features", "keys", "KeysPage.tsx")
	for _, required := range []string{
		`config={{ apiScope: "self" }}`,
		`apiScope="self"`,
	} {
		if !strings.Contains(keysPage, required) {
			t.Fatalf("KeysPage must force normal key routes through self token APIs: missing %q", required)
		}
	}
}

func TestFrontendSelectAllTargetsCompleteFilteredTokenSet(t *testing.T) {
	keysPage := readFrontendFile(t, "src", "features", "keys", "KeysPage.tsx")
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	for _, required := range []string{
		`type TokenSelection =`,
		`{ mode: "filtered"; excludedIds: Set<number> }`,
		`setSelection({ mode: "filtered", excludedIds: new Set() })`,
		`payload.all_filtered = true`,
		`payload.excluded_token_ids = [...selection.excludedIds]`,
		`all_filtered: Boolean(deleteTarget.allFiltered)`,
		`}, [filterKey]);`,
		`全选`,
	} {
		if !strings.Contains(keysPage, required) {
			t.Fatalf("keys page complete filtered selection contract missing %q", required)
		}
	}
	if strings.Contains(keysPage, "全选本页") {
		t.Fatal("keys page still exposes page-only select-all copy")
	}
	for _, required := range []string{
		`tokenScopedPath("/api/tokens/batch", "/admin/tokens/batch", scope)`,
		"postJSON<Record<string, unknown>>(`${base}${query ? `?${query}` : \"\"}`, payload)",
	} {
		if !strings.Contains(apiFile, required) {
			t.Fatalf("frontend filtered batch API contract missing %q", required)
		}
	}
}

func TestFrontendQuotaStateDistinguishesPendingFromUnavailable(t *testing.T) {
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	components := readFrontendFile(t, "src", "shared", "components.tsx")
	keysPage := readFrontendFile(t, "src", "features", "keys", "KeysPage.tsx")
	importsPage := readFrontendFile(t, "src", "features", "imports", "ImportsPage.tsx")

	for _, required := range []string{
		`"pending"`,
		"quota_fetch_state?: TokenQuotaFetchState",
	} {
		if !strings.Contains(apiFile, required) {
			t.Fatalf("frontend quota API contract missing %q", required)
		}
	}
	for _, required := range []string{
		`state === "pending"`,
		"额度更新中",
		"额度不可用",
	} {
		if !strings.Contains(components, required) {
			t.Fatalf("frontend quota rendering contract missing %q", required)
		}
	}
	if !strings.Contains(keysPage, "state={item.quota_fetch_state}") || !strings.Contains(keysPage, "state={token.quota_fetch_state}") {
		t.Fatal("keys page must pass quota fetch state to every quota strip")
	}
	if !strings.Contains(importsPage, "state={token.quota_fetch_state}") {
		t.Fatal("imports page must pass quota fetch state to its quota strip")
	}
	for _, required := range []string{
		"quotaRefreshPending",
		"quotaRefreshPolls.current >= 12",
		"item.quota_fetch_state === \"pending\"",
		"10_000",
	} {
		if !strings.Contains(keysPage, required) {
			t.Fatalf("keys page quota polling contract missing %q", required)
		}
	}
}

func TestFrontendUserAreaUsesUserPrincipalScope(t *testing.T) {
	apiFile := readFrontendFile(t, "src", "lib", "api.ts")
	for _, required := range []string{
		"export function hasUserPrincipal",
		`scopedPath(userPath: string, adminPath: string): string`,
		`return hasUserPrincipal() ? userPath : adminPath`,
		"export function isAuthContextPending",
		`importJobs: (limit = 50, scope: ImportAPIScope = "self")`,
		`scope === "admin" || !hasUserPrincipal() ?`,
	} {
		if !strings.Contains(apiFile, required) {
			t.Fatalf("user area APIs must use user principal scope before admin fallback: missing %q", required)
		}
	}
}

func readFrontendFile(t *testing.T, parts ...string) string {
	t.Helper()
	pathParts := append([]string{"..", "..", "frontend"}, parts...)
	data, err := os.ReadFile(filepath.Join(pathParts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
