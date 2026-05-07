package convert

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

// RowToCSV encodes a single record as one CSV/TSV line (no trailing newline)
// using the given delimiter. Type-aware sanitization:
//   - Real columns: NaN/±Inf become empty (Kusto interprets empty as null)
//   - All other types pass through; Kusto parses them per the table schema.
//
// Empty cells stay empty (null in Kusto). String columns also map empty to
// null in CSV — this is a deliberate change from the JSON path, which kept
// "" distinct from null. CSV cannot represent that distinction.
func RowToCSV(cols []string, types []schema.KustoType, rec []string, delim rune) []byte {
	var b bytes.Buffer
	for i := range cols {
		if i > 0 {
			b.WriteRune(delim)
		}
		var v string
		if i < len(rec) {
			v = rec[i]
		}
		v = sanitize(types[i], v)
		writeCSVField(&b, v, delim)
	}
	return b.Bytes()
}

func sanitize(t schema.KustoType, v string) string {
	if v == "" {
		return ""
	}
	if t == schema.TypeReal {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return ""
			}
		}
	}
	return v
}

func writeCSVField(b *bytes.Buffer, s string, delim rune) {
	if !strings.ContainsAny(s, "\"\r\n") && !strings.ContainsRune(s, delim) {
		b.WriteString(s)
		return
	}
	b.WriteByte('"')
	b.WriteString(strings.ReplaceAll(s, `"`, `""`))
	b.WriteByte('"')
}
