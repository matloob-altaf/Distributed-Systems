package main

import (
	"fmt"
)

func main() {
	fmt.Println("Result of isEven(10):", isEven(10))
}

func isEven(num int) bool {
	// to be called immediately after the function returns
	defer print("I have exited already")

	// normal execution
	fmt.Println("I am running")
	return num%2 == 0
}

func print(s string) {
	fmt.Println(s)
}
