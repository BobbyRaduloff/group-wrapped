package pkg

import "fmt"

func Invariant(cond bool, messages ...any) {
	if !cond {
		panic(fmt.Sprintf("invariant failed: %v", messages))
	}
}
