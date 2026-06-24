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
	} {
		if !strings.Contains(appShell, required) {
			t.Fatalf("AppShell admin navigation contract missing %q", required)
		}
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
		`detailBasePath: "/admin/pools"`,
	} {
		if !strings.Contains(adminPages, required) {
			t.Fatalf("AdminPages must expose user status and admin pool key list data path %q", required)
		}
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

func readFrontendFile(t *testing.T, parts ...string) string {
	t.Helper()
	pathParts := append([]string{"..", "..", "frontend"}, parts...)
	data, err := os.ReadFile(filepath.Join(pathParts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
