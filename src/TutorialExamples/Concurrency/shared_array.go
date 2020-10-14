package main

import (
	"fmt"
	"math/rand"
	"time"
)

type writeToArray struct {
	index int
	value int
}

// This functions contains an array that will be shared by many goroutines
func sliceHandler(sliceLen int, read <-chan int, write <-chan writeToArray, readResult chan<- int) {

	// Declaring the shared array
	mySlice := make([]int, sliceLen)

	// Initilizing the shared array with the same value as its index
	for i := 0; i < sliceLen; i++ {
		mySlice[i] = i
	}

	// Wait forever to read and write to shared array
	for {

		// Select clause to process both read and write channels
		select {

		// Read request from another goroutine
		case readIndex := <-read:
			readResult <- mySlice[readIndex]

		// Write request from another goroutine
		case writeOp := <-write:
			mySlice[writeOp.index] = writeOp.value
		}
	}
}

func main() {
	// Initilizing the read request channel
	read := make(chan int)

	// Initilizing the read result channel
	readResult := make(chan int)

	// Initilizing the write channel
	write := make(chan writeToArray)

	// Launch the shared array handler as a separare goroutine
	sliceLen := 20
	go sliceHandler(sliceLen, read, write, readResult)

	for i := 0; i < sliceLen; i++ {
		// Launch 20 reader routines
		go func(index int) {
			read <- index
		}(i)

		// Launch 20 writer routines
		go func() {
			index := rand.Intn(sliceLen)
			write <- writeToArray{index, index * 100}
		}()
	}

	for {
		select {
		// Print read results
		case readValue := <-readResult:
			fmt.Println(readValue)

		// Quit after 3 seconds of no activity
		case <-time.After(3 * time.Second):
			return
		}
	}
}
