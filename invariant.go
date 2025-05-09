package main

import "fmt"

func invariant(cond bool, messages ...any) {
	if !cond {
		panic(fmt.Sprintf("invariant failed: %v", messages))
	}
}
