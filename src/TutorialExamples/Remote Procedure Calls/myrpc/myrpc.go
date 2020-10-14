package myrpc

import (
	"fmt"
	"time"
)

// Server - a class with a single member function
type Server struct {
}

// Echo - a function we want to make available for RPC calls
// echoes the clients message back to the client
func (sv *Server) Echo(args *EchoArgs, reply *EchoReply) error {
	fmt.Println("Received Echo RPC from client with input:", args.Input)

	// Set the value to respond with, in the reply struct
	reply.Output = "ECHO: " + args.Input
	return nil
}

// Add - a function we want to make available for RPC calls
// replies with the sum of the two input numbers
func (sv *Server) Add(args *AddArgs, reply *AddReply) error {

	// Sleep to simulate a heavy-time consuming workload
	time.Sleep(time.Second * time.Duration(5))

	reply.Result = args.OperandA + args.OperandB
	return nil
}
