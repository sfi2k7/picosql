package picosql

import "strings"

const (
	colonSeperator = ":"
	atSeperator    = "@"
)

func ExtractNamedParameters(query string) (string, []string) {
	var paramaters []string

	if len(strings.TrimSpace(query)) == 0 {
		return query, paramaters
	}

	hasColon := strings.Index(query, colonSeperator) > 0
	hasAt := strings.Index(query, atSeperator) > 0

	if !hasColon && !hasAt {
		return query, paramaters
	}
	sep := colonSeperator

	if hasAt {
		sep = atSeperator
	}

	isLast := false

	s := strings.TrimSpace(query)
	for {
		next := strings.Index(s, sep)

		if next < 0 {
			break
		}
		nextPlus := next + 1
		further := strings.Index(s[nextPlus:], ",")

		if further < 0 {
			further = strings.Index(s[nextPlus:], " ")
			if further < 0 {
				further = strings.Index(s[nextPlus:], ")")
				if further < 0 {
					further = len(s) - (nextPlus)
				}
			}
		}

		if further < 0 {
			isLast = true
		}

		until := len(s)
		if !isLast {
			until = further + next
		}

		arg := s[nextPlus : until+1]
		paramaters = append(paramaters, strings.TrimSpace(arg))
		if isLast {
			break
		}
		s = s[until:]
	}
	for _, p := range paramaters {
		query = strings.Replace(query, sep+p, "?", 1)
	}

	return query, paramaters
}
