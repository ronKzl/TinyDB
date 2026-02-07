package kvdb

func check(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

func BinarySearchFunc[S ~[]E, E any, T any](slice S, target T, cmp func(E, T) int) (pos int, ok bool){
	left := 0
	right := len(slice) - 1

	for left <= right {
		mid := (left + ((right-left) / 2))
		res := cmp(slice[mid],target)
		if res == 0 {
			return mid, true
		} else if res < 0{ // slice[mid] < target
			left = mid + 1
		} else { // slice[mid] > target
			right = mid - 1
		}
	}
	return left, false
}