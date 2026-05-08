package convert

import (
	"strings"
	"testing"

	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

func TestRowToCSVPassThrough(t *testing.T) {
	cols := []string{"i", "f", "b", "s", "ts"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeReal, schema.TypeBool, schema.TypeString, schema.TypeDateTime}
	rec := []string{"42", "3.14", "true", "hello", "2025-01-02T03:04:05Z"}
	got := string(RowToCSV(cols, types, rec, ','))
	want := "42,3.14,true,hello,2025-01-02T03:04:05Z"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRowToCSVEmpty(t *testing.T) {
	cols := []string{"a", "b", "c"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeString, schema.TypeReal}
	rec := []string{"", "", ""}
	got := string(RowToCSV(cols, types, rec, ','))
	if got != ",," {
		t.Errorf("got %q, want %q", got, ",,")
	}
}

func TestRowToCSVNonFiniteReal(t *testing.T) {
	cols := []string{"a", "b", "c", "d"}
	types := []schema.KustoType{schema.TypeReal, schema.TypeReal, schema.TypeReal, schema.TypeReal}
	rec := []string{"1.5", "NaN", "Inf", "-Inf"}
	got := string(RowToCSV(cols, types, rec, ','))
	want := "1.5,,," // NaN/Inf -> empty
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRowToCSVQuoting(t *testing.T) {
	cols := []string{"a", "b", "c", "d"}
	types := []schema.KustoType{schema.TypeString, schema.TypeString, schema.TypeString, schema.TypeString}
	rec := []string{"plain", "has,comma", `has"quote`, "has\nnewline"}
	got := string(RowToCSV(cols, types, rec, ','))
	want := `plain,"has,comma","has""quote","has` + "\n" + `newline"`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRowToCSVTabDelimiter(t *testing.T) {
	cols := []string{"a", "b", "c"}
	types := []schema.KustoType{schema.TypeString, schema.TypeString, schema.TypeString}
	rec := []string{"x", "has,comma", "has\ttab"}
	got := string(RowToCSV(cols, types, rec, '\t'))
	// commas don't need quoting under tab delimiter; tabs do
	want := "x\thas,comma\t\"has\ttab\""
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRowToCSVShortRecord(t *testing.T) {
	cols := []string{"a", "b", "c"}
	types := []schema.KustoType{schema.TypeLong, schema.TypeLong, schema.TypeString}
	rec := []string{"1"}
	got := string(RowToCSV(cols, types, rec, ','))
	if !strings.HasPrefix(got, "1,,") {
		t.Errorf("got %q, want prefix %q", got, "1,,")
	}
}
