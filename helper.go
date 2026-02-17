package kvdb

// check performs a runtime assertion. It panics if the provided condition 
// is false, typically used to catch "impossible" states in the system.
func check(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

// BinarySearchFunc performs a binary search on a sorted slice using a custom 
// comparison function. It returns the index where the target is found and a 
// boolean indicating success.
//
// If the target is not found, it returns the insertion point (the index of 
// the first element greater than the target) and false.
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