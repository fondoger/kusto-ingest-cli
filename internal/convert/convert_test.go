package convert

import (
	"strings"
	"testing"

	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

func TestRowToJSONTypes(t *testing.T) {
	cols := []string{"i", "f", "b", "s", "ts"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeReal, schema.TypeBool, schema.TypeString, schema.TypeDateTime}
	rec := []string{"42", "3.14", "true", "hello", "2025-01-02T03:04:05Z"}
	got, err := RowToJSON(cols, types, rec)
	if err != nil {
		t.Fatal(err)
	}
	s := string(got)
	for _, want := range []string{`"i":42`, `"f":3.14`, `"b":true`, `"s":"hello"`, `"ts":"2025-01-02T03:04:05Z"`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in %s", want, s)
		}
	}
}

func TestRowToJSONNullVsEmptyString(t *testing.T) {
	cols := []string{"i", "s"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeString}
	rec := []string{"", ""}
	got, _ := RowToJSON(cols, types, rec)
	s := string(got)
	if strings.Contains(s, `"i"`) {
		t.Errorf("expected i omitted (null), got %s", s)
	}
	if !strings.Contains(s, `"s":""`) {
		t.Errorf("expected s as empty string, got %s", s)
	}
}

func TestRowToJSONShortRecord(t *testing.T) {
	cols := []string{"a", "b", "c"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeLong, schema.TypeString}
	rec := []string{"1"}
	got, _ := RowToJSON(cols, types, rec)
	s := string(got)
	if !strings.Contains(s, `"a":1`) {
		t.Errorf("a missing: %s", s)
	}
	if strings.Contains(s, `"b"`) {
		t.Errorf("b should be omitted (empty -> null): %s", s)
	}
	if !strings.Contains(s, `"c":""`) {
		t.Errorf("c should be empty string: %s", s)
	}
}
