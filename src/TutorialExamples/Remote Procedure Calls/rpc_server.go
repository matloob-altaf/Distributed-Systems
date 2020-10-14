package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"./myrpc"
)

func main() {
	// Allocate memory for a new object of Server class
	es := new(myrpc.Server)
	// The methods of this class will be made available for RPC access

	// Attempt to listen on TCP port 9999
	ln, err := net.Listen("tcp", "localhost:9999")

	// Print error and return if couldn't listen on port 9999
	if err != nil {
		fmt.Println("Couldn't listen on port 9999: ", err)
		return
	}

	// Instantiate a new RPC server object
	rpcServer := rpc.NewServer()

	// Register Server class methods for RPC access
	rpcServer.Register(es)

	// Register an HTTP handler for the RPC server
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	// Start a go-routine that listens for RPC calls on the TCP listener ln forever
	go http.Serve(ln, nil)

	// Block main forever
	fmt.Println("RPC server started...")
	for {
		select {}
	}
}
