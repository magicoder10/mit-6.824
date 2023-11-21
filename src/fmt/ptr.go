package fmt

import (
	"fmt"
)

func FromPtr[Value any](val *Value) string {
	if val == nil {
		return "nil"
	}

	return fmt.Sprintf("%v", *val)
}
