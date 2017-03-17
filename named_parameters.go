package picosql

import "strings"

func ExtractNamedParameters(query string) (string, []string) {
	var paramaters []string

	if len(strings.TrimSpace(query)) == 0 {
		return "", paramaters
	}

	if strings.Index(query, ":") < 0 {
		return "", paramaters
	}

	isLast := false
	s := query
	for {
		next := strings.Index(s, ":")

		if next < 0 {
			break
		}

		further := strings.Index(s[next+1:], ",")
		if further < 0 {
			further = strings.Index(s[next+1:], " ")
			if further < 0 {
				further = len(s) - (next + 1)
			}
		}

		if further < 0 {
			isLast = true
		}

		until := len(s)
		if !isLast {
			until = further + next
		}

		arg := s[next+1 : until+1]
		paramaters = append(paramaters, arg)
		if isLast {
			break
		}
		s = s[until:]
	}
	for _, p := range paramaters {
		query = strings.Replace(query, ":"+p, "?", 1)
	}
	return query, paramaters
}
