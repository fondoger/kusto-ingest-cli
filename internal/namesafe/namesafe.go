package namesafe

import (
	"fmt"
	"strings"
	"unicode"
)

func Sanitize(name string) string {
	s := SanitizeChars(name)
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return s
}

// SanitizeChars replaces illegal characters with underscores but does NOT
// prefix an underscore for digit-leading names. Use this when a prefix
// will be prepended.
func SanitizeChars(name string) string {
	if name == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) && r < 128 || unicode.IsDigit(r) && r < 128 || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	s := b.String()
	if s == "" {
		return "_"
	}
	return s
}

// ValidIdentifier reports whether s is a valid Kusto-friendly identifier:
// non-empty, ASCII letters/digits/underscore only, not starting with a digit.
func ValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		isDigit := r >= '0' && r <= '9'
		if !(isLetter || isDigit || r == '_') {
			return false
		}
		if i == 0 && isDigit {
			return false
		}
	}
	return true
}

func DedupColumns(names []string) []string {
	seen := map[string]int{}
	out := make([]string, len(names))
	for i, n := range names {
		s := Sanitize(n)
		if _, ok := seen[s]; !ok {
			seen[s] = 1
			out[i] = s
			continue
		}
		seen[s]++
		candidate := fmt.Sprintf("%s_%d", s, seen[s])
		for {
			if _, ok := seen[candidate]; !ok {
				seen[candidate] = 1
				out[i] = candidate
				break
			}
			seen[s]++
			candidate = fmt.Sprintf("%s_%d", s, seen[s])
		}
	}
	return out
}
