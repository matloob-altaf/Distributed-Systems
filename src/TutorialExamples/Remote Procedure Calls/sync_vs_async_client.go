// Observe the ordering of prints
//
// The "After ..." print is seen before the RPC reply when
// using asynchronus RPC whereas for synchronus it is seen
// after the reply
package main

import (
	"fmt"
	"net/rpc"
	"time"

	"./myrpc"
)

func main() {
	// Open a HTTP RPC connection to the RPC server
	cli, err := rpc.DialHTTP("tcp", "localhost:9999")

	// Print error and return if couldn't connect to RPC server
	if err != nil {
		fmt.Println("Error connecting to RPC server: ", err)
		return
	}
	// Allocate memory for a Add arguments struct
	args := new(myrpc.AddArgs)

	// Allocate memory for a Add reply struct
	reply := new(myrpc.AddReply)

	// Set the arguments
	args.OperandA = float32(1)
	args.OperandB = float32(2)

	// Making a synchronus RPC call
	fmt.Println("Performing synchronus RPC")
	err = cli.Call("Server.Add", args, reply)

	// Print and return on error
	if err != nil {
		fmt.Println("Error making RPC:", err)
		return
	}
	fmt.Println("Synchronus RPC Result:", reply.Result)
	fmt.Println("After Synchronus RPC")
	fmt.Println("-----------------------------------------")

	// Make a channel to receive RPC replies
	done := make(chan *rpc.Call, 1)

	// Making an synchronus RPC call
	fmt.Println("Performing asynchronus RPC")
	cli.Go("Server.Add", args, reply, done)

	go func() {
		// Waiting for RPC call to complete
		call := <-done

		// Print and return on error
		if call.Error != nil {
			fmt.Println("Error making RPC call", call.Error)
			return
		}

		// Type casting reply
		reply := call.Reply.(*myrpc.AddReply)
		fmt.Println("Asynchronus RPC Result:", reply.Result)
	}()
	fmt.Println("After Asynchronus RPC")

	// Wait before exit
	time.Sleep(time.Duration(7) * time.Second)
}
