package httpapi

import (
	"testing"

	"github.com/yym68686/oaix/internal/store"
)

func TestParseUserDashboardRange(t *testing.T) {
	for _, test := range []struct {
		input string
		want  store.UserDashboardRange
	}{
		{input: "", want: store.UserDashboardMonth},
		{input: "TODAY", want: store.UserDashboardToday},
		{input: " week ", want: store.UserDashboardWeek},
		{input: "month", want: store.UserDashboardMonth},
		{input: "year", want: store.UserDashboardYear},
	} {
		got, err := parseUserDashboardRange(test.input)
		if err != nil || got != test.want {
			t.Fatalf("parseUserDashboardRange(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	if _, err := parseUserDashboardRange("all"); err == nil {
		t.Fatal("invalid dashboard range was accepted")
	}
}
