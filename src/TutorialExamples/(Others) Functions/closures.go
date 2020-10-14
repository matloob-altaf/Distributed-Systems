package main

import (
	"fmt"
)

func main() {

	// get a function which counts down from ten
	countDownFromTen := countDown(10)

	// test the countdown
	fmt.Println("Results for calling countDownFromTen:")
	for i := 0; i < 3; i++ {
		fmt.Println(countDownFromTen())
	}

	fmt.Println("\n-----------------------\n")

	// get a function which counts down from twenty
	countDownFromTwenty := countDown(20)

	// test the countdown
	fmt.Println("Results for calling countDownFromTwenty:")
	for i := 0; i < 3; i++ {
		fmt.Println(countDownFromTwenty())
	}
}

func countDown(num int) func() int {

	// define and return function
	return func() int {
		num--
		return num
	}
}
