package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {

	// attempt to listen on port 9999
	ln, err := net.Listen("tcp", ":9999")

	// print error and return if couldn't listen on port 999
	if err != nil {
		fmt.Printf("Couldn't listen on port 9999: %s\n", err)
		return
	}

	// listen forever
	for {
		conn, err := ln.Accept()

		// handle successful connections concurrently
		if err != nil {
			fmt.Printf("Couldn't accept a client connection: %s\n", err)
		} else {
			go handleConnection(conn)
		}
	}
}

// handleConnection handles client connections
func handleConnection(conn net.Conn) {

	// clean up once the connection closes
	defer Clean(conn)

	// obtain a buffered reader / writer on the connection
	rw := ConnectionToRW(conn)

	for {
		// get client message
		msg, err := rw.ReadString('\n')
		if err != nil {
			fmt.Printf("There was an error reading from a client connection: %s\n", err)
			return
		}

		// print client message
		fmt.Printf("Recieved: '%s' of len %d from client: %v\n", msg[:len(msg)-1], len(msg), conn)

		// echo back the same message to the client
		_, err = rw.WriteString(msg)
		if err != nil {
			fmt.Printf("There was an error writing to a client connection: %s\n", err)
			return
		}
		err = rw.Flush()
		if err != nil {
			fmt.Printf("There was an error writing to a client connection: %s\n", err)
			return
		}
	}
}

// Clean closes a connection
func Clean(conn net.Conn) {
	// clean up connection related data structures and goroutines here
	conn.Close()
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}
