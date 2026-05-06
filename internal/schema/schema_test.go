package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestInferLongAndReal(t *testing.T) {
	p := writeTemp(t, "t.csv", "a,b,c\n1,1.5,hi\n2,2,bye\n3,3.14,x\n")
	s, err := Infer(p, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Types[0], TypeLong; got != want {
		t.Errorf("col a = %v, want %v", got, want)
	}
	if got, want := s.Types[1], TypeReal; got != want {
		t.Errorf("col b = %v, want %v", got, want)
	}
	if got, want := s.Types[2], TypeString; got != want {
		t.Errorf("col c = %v, want %v", got, want)
	}
}

func TestInferLongWithBlanks(t *testing.T) {
	p := writeTemp(t, "t.csv", "n\n1\n\n2\n3\n")
	s, _ := Infer(p, 10000)
	if s.Types[0] != TypeLong {
		t.Errorf("got %v, want long (blanks should be ignored)", s.Types[0])
	}
}

func TestInferBoolRequiresLiteral(t *testing.T) {
	// 0/1 only without true/false should stay long, not bool
	p := writeTemp(t, "t.csv", "flag\n0\n1\n0\n1\n")
	s, _ := Infer(p, 10000)
	if s.Types[0] != TypeLong {
		t.Errorf("got %v, want long (no true/false literal)", s.Types[0])
	}

	p2 := writeTemp(t, "t.csv", "flag\ntrue\nfalse\n0\n1\n")
	s2, _ := Infer(p2, 10000)
	if s2.Types[0] != TypeBool {
		t.Errorf("got %v, want bool", s2.Types[0])
	}
}

func TestInferAllEmptyToString(t *testing.T) {
	p := writeTemp(t, "t.csv", "x,y\n1,\n2,\n3,\n")
	s, _ := Infer(p, 10000)
	if s.Types[1] != TypeString {
		t.Errorf("col y = %v, want string", s.Types[1])
	}
}

func TestInferDateTime(t *testing.T) {
	p := writeTemp(t, "t.csv", "ts\n2025-01-01\n2025-06-15\n2026-12-31\n")
	s, _ := Infer(p, 10000)
	if s.Types[0] != TypeDateTime {
		t.Errorf("got %v, want datetime", s.Types[0])
	}
}

func TestInferBOMAndCRLF(t *testing.T) {
	content := "\xEF\xBB\xBFa,b\r\n1,2\r\n3,4\r\n"
	p := writeTemp(t, "t.csv", content)
	s, err := Infer(p, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if s.Columns[0] != "a" {
		t.Errorf("BOM not stripped: cols[0]=%q", s.Columns[0])
	}
	if s.Types[0] != TypeLong || s.Types[1] != TypeLong {
		t.Errorf("got %v %v, want long long", s.Types[0], s.Types[1])
	}
}

func TestInferEmbeddedCommaQuoted(t *testing.T) {
	p := writeTemp(t, "t.csv", "name,age\n\"Doe, John\",30\n\"Roe, Jane\",25\n")
	s, _ := Infer(p, 10000)
	if s.Types[0] != TypeString || s.Types[1] != TypeLong {
		t.Errorf("got %v %v", s.Types[0], s.Types[1])
	}
}

func TestInferTSVDelimiter(t *testing.T) {
	p := writeTemp(t, "t.tsv", "a\tb\n1\thello\n2\tworld\n")
	s, _ := Infer(p, 10000)
	if len(s.Columns) != 2 {
		t.Fatalf("got %d cols", len(s.Columns))
	}
	if s.Types[0] != TypeLong || s.Types[1] != TypeString {
		t.Errorf("got %v %v", s.Types[0], s.Types[1])
	}
}
