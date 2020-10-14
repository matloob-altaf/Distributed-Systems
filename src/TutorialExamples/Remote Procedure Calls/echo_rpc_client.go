package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"

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

	// Get a reader from Stdin
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		// Read from Stdin
		input := scanner.Text()

		// Allocate memory for a Echo arguments struct
		args := new(myrpc.EchoArgs)

		// Allocate memory for a Echo reply struct
		reply := new(myrpc.EchoReply)

		// Set the arguments
		args.Input = input

		// Make a synchronus RPC call
		err = cli.Call("Server.Echo", args, reply)

		// Print and return on error
		if err != nil {
			fmt.Println("Error making RPC call", err)
			return
		}

		// Print RPC result
		fmt.Println(reply.Output)

	}
}
