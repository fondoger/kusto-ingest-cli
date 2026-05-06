package namesafe

import (
	"reflect"
	"testing"
)

func TestSanitizeChars(t *testing.T) {
	cases := []struct{ in, want string }{
		{"123abc", "123abc"},
		{"foo-bar", "foo_bar"},
		{"", "_"},
	}
	for _, c := range cases {
		if got := SanitizeChars(c.in); got != c.want {
			t.Errorf("SanitizeChars(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestValidIdentifier(t *testing.T) {
	good := []string{"a", "_x", "raw_", "Foo123", "_"}
	bad := []string{"", "1abc", "foo-bar", "a b", "中"}
	for _, s := range good {
		if !ValidIdentifier(s) {
			t.Errorf("ValidIdentifier(%q) = false, want true", s)
		}
	}
	for _, s := range bad {
		if ValidIdentifier(s) {
			t.Errorf("ValidIdentifier(%q) = true, want false", s)
		}
	}
}

func TestSanitize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"foo", "foo"},
		{"foo bar", "foo_bar"},
		{"foo-bar.baz", "foo_bar_baz"},
		{"123abc", "_123abc"},
		{"", "_"},
		{"中文列", "___"},
		{"_already_ok", "_already_ok"},
		{"a!@#b", "a___b"},
	}
	for _, c := range cases {
		if got := Sanitize(c.in); got != c.want {
			t.Errorf("Sanitize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDedupColumns(t *testing.T) {
	in := []string{"a", "a", "a", "b-x", "b x", "1c"}
	got := DedupColumns(in)
	want := []string{"a", "a_2", "a_3", "b_x", "b_x_2", "_1c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
