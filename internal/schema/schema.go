package schema

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fondoger/kusto-ingest-cli/internal/namesafe"
)

type KustoType int

const (
	TypeBool KustoType = iota
	TypeLong
	TypeReal
	TypeDateTime
	TypeTimespan
	TypeString
)

func (t KustoType) String() string {
	switch t {
	case TypeBool:
		return "bool"
	case TypeLong:
		return "long"
	case TypeReal:
		return "real"
	case TypeDateTime:
		return "datetime"
	case TypeTimespan:
		return "timespan"
	default:
		return "string"
	}
}

type Schema struct {
	Columns  []string
	Types    []KustoType
	RowCount int64 // total data rows (excludes header)
}

func DelimiterFor(path string) rune {
	if strings.EqualFold(filepath.Ext(path), ".tsv") {
		return '\t'
	}
	return ','
}

// CountDataRows counts data rows (excludes header) by scanning newlines.
// This is approximate when fields contain embedded newlines, but is fine for sampling step calculation.
func CountDataRows(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(stripBOMReader(f), 1<<20)
	var n int64
	buf := make([]byte, 64*1024)
	for {
		c, err := r.Read(buf)
		for i := 0; i < c; i++ {
			if buf[i] == '\n' {
				n++
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	if n > 0 {
		n--
	}
	return n, nil
}

type bomReader struct {
	r       io.Reader
	checked bool
}

func (b *bomReader) Read(p []byte) (int, error) {
	if !b.checked {
		b.checked = true
		var pre [3]byte
		n, _ := io.ReadFull(b.r, pre[:])
		if n == 3 && pre[0] == 0xEF && pre[1] == 0xBB && pre[2] == 0xBF {
			// drop BOM
		} else if n > 0 {
			b.r = io.MultiReader(strings.NewReader(string(pre[:n])), b.r)
		}
	}
	return b.r.Read(p)
}

func stripBOMReader(r io.Reader) io.Reader {
	return &bomReader{r: r}
}

func newCSVReader(r io.Reader, delim rune) *csv.Reader {
	cr := csv.NewReader(stripBOMReader(r))
	cr.Comma = delim
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.ReuseRecord = false
	return cr
}

// Infer reads the file, returns sanitized header + inferred types.
func Infer(path string, inferRows int) (*Schema, error) {
	delim := DelimiterFor(path)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cr := newCSVReader(f, delim)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	cols := namesafe.DedupColumns(header)
	nCols := len(cols)

	totalRows, err := CountDataRows(path)
	if err != nil {
		return nil, err
	}

	step := int64(1)
	if inferRows > 0 && totalRows > int64(inferRows) {
		step = totalRows / int64(inferRows)
		if step < 1 {
			step = 1
		}
	}

	candidates := make([][]KustoType, nCols)
	hasBoolLiteral := make([]bool, nCols)
	allEmpty := make([]bool, nCols)
	for i := range candidates {
		candidates[i] = []KustoType{TypeBool, TypeLong, TypeReal, TypeDateTime, TypeTimespan, TypeString}
		allEmpty[i] = true
	}

	var rowIdx int64
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row %d: %w", rowIdx+1, err)
		}
		if rowIdx%step == 0 {
			for i := 0; i < nCols && i < len(rec); i++ {
				val := rec[i]
				if val == "" {
					continue
				}
				allEmpty[i] = false
				candidates[i] = filterCandidates(candidates[i], val, &hasBoolLiteral[i])
			}
		}
		rowIdx++
	}

	types := make([]KustoType, nCols)
	for i := 0; i < nCols; i++ {
		if allEmpty[i] {
			types[i] = TypeString
			continue
		}
		picked := TypeString
		for _, c := range candidates[i] {
			if c == TypeBool && !hasBoolLiteral[i] {
				continue
			}
			picked = c
			break
		}
		types[i] = picked
	}
	return &Schema{Columns: cols, Types: types, RowCount: totalRows}, nil
}

func filterCandidates(cands []KustoType, val string, sawBoolLit *bool) []KustoType {
	out := cands[:0]
	for _, c := range cands {
		ok, isBoolLit := matchType(c, val)
		if !ok {
			continue
		}
		if c == TypeBool && isBoolLit {
			*sawBoolLit = true
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return []KustoType{TypeString}
	}
	return out
}

func matchType(t KustoType, s string) (ok bool, boolLiteral bool) {
	switch t {
	case TypeBool:
		ls := strings.ToLower(s)
		switch ls {
		case "true", "false":
			return true, true
		case "0", "1":
			return true, false
		}
		return false, false
	case TypeLong:
		_, err := strconv.ParseInt(s, 10, 64)
		return err == nil, false
	case TypeReal:
		_, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return false, false
		}
		// require non-integer formatting to prefer long
		if strings.ContainsAny(s, ".eE") {
			return true, false
		}
		// integer-looking string would already be long; but still acceptable as real
		return true, false
	case TypeDateTime:
		return parseDateTime(s) != nil, false
	case TypeTimespan:
		return parseTimespan(s), false
	case TypeString:
		return true, false
	}
	return false, false
}

var dtLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func parseDateTime(s string) *time.Time {
	for _, l := range dtLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return &t
		}
	}
	return nil
}

func parseTimespan(s string) bool {
	// d.hh:mm:ss(.fff) or hh:mm:ss(.fff)
	rest := s
	if i := strings.Index(rest, "."); i > 0 && !strings.Contains(rest[:i], ":") {
		// possibly day prefix like "1.02:03:04"
		if _, err := strconv.Atoi(rest[:i]); err == nil {
			rest = rest[i+1:]
		}
	}
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 {
		return false
	}
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return false
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return false
	}
	sec := parts[2]
	if i := strings.Index(sec, "."); i >= 0 {
		if _, err := strconv.Atoi(sec[:i]); err != nil {
			return false
		}
		if _, err := strconv.Atoi(sec[i+1:]); err != nil {
			return false
		}
	} else {
		if _, err := strconv.Atoi(sec); err != nil {
			return false
		}
	}
	return true
}
