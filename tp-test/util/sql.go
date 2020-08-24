package util

import "strings"

func IsQuery(sql string) bool {
	sql = strings.ToLower(strings.TrimSpace(sql))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}
