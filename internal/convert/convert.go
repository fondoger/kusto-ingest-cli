package convert

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

// RowToJSON converts a single CSV record into a JSON object respecting types.
// Empty cells: string -> "", others -> field omitted (null).
func RowToJSON(cols []string, types []schema.KustoType, rec []string) ([]byte, error) {
	obj := make(map[string]any, len(cols))
	for i, c := range cols {
		var v string
		if i < len(rec) {
			v = rec[i]
		}
		t := types[i]
		if v == "" {
			if t == schema.TypeString {
				obj[c] = ""
			}
			continue
		}
		converted, err := convertValue(t, v)
		if err != nil {
			// fall back to raw string if conversion fails despite inference
			obj[c] = v
			continue
		}
		obj[c] = converted
	}
	return json.Marshal(obj)
}

func convertValue(t schema.KustoType, s string) (any, error) {
	switch t {
	case schema.TypeBool:
		ls := strings.ToLower(s)
		switch ls {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		}
		return nil, fmt.Errorf("bad bool")
	case schema.TypeLong:
		return strconv.ParseInt(s, 10, 64)
	case schema.TypeReal:
		return strconv.ParseFloat(s, 64)
	case schema.TypeDateTime:
		t := parseDateTime(s)
		if t == nil {
			return nil, fmt.Errorf("bad datetime")
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case schema.TypeTimespan:
		return s, nil
	default:
		return s, nil
	}
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
