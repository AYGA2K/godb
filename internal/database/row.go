package database

import (
	"fmt"
	"strings"
)

// Row represents a table row (record)
type Row map[string]any

func (r Row) String() string {
	var result strings.Builder
	result.WriteString("{")
	first := true
	for col, val := range r {
		if !first {
			result.WriteString(", ")
		}
		first = false
		result.WriteString(col)
		result.WriteString(": ")
		result.WriteString(fmt.Sprint(val))
	}
	result.WriteString("}")
	return result.String()
}
