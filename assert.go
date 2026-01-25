package kvdb

func check(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

