package wc_helpers

import (
	"regexp"
	"strings"
)

// converts wild card to a regex pattern
func WildCardToRegex(p string) string {
	cards := strings.Split(p, "*")
	if len(cards) == 1 {
		return "^" + p + "$"
	}
	var result strings.Builder
	for i, literal := range cards {
		if i > 0 {
			result.WriteString(".*")
		}
		result.WriteString(regexp.QuoteMeta(literal))
	}
	return "^" + result.String() + "$"
}
