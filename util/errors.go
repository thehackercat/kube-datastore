package util

import "fmt"

var (
	ErrKeyLeaseLocked = fmt.Errorf("key lease already locked")
)
