package main

import (
	"fmt"
	"time"
)

// A worker receives tasks from jobs channel until its closed.
// These tasks are processed and results are sent on another
// channel. Note that the jobs channel is receive-only and
// results channel is send-only.
func worker(jobs <-chan bool, results chan<- int, name int) {

	// fetch tasks, process them and send results
	for range jobs {
		time.Sleep(1 * time.Second)
		results <- name
	}

	// print worker's exit
	fmt.Println("Worker", name, "done.")
}

func main() {

	// channel to send jobs through to workers
	// note that the channel is buffered
	jobs := make(chan bool, 10)

	// channel to receive results on
	results := make(chan int)

	// add jobs concurrently
	go func() {

		// send tasks on the jobs channel
		for i := 0; i < 10; i++ {
			jobs <- true
		}

		// close the channel to signal no more jobs
		close(jobs)
	}()

	// create workers concurrently
	go func() {
		for i := 0; i < 4; i++ {
			go worker(jobs, results, i+1)
		}
	}()

	// print results of the processing tasks sent to workers
	for {
		select {

		// fetch results
		case r := <-results:
			fmt.Println("Worker", r, "finished a task.")

		// close the main process if no results are received for upto 2 seconds
		case <-time.After(2 * time.Second):
			fmt.Println("Stopping collection of results.")
			return
		}
	}
}
