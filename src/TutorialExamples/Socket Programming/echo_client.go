package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {

	// open a connection to the echo server
	rw, err := Open("localhost:9999")
	if err != nil {
		fmt.Printf("There was an error opening a connection to localhost on port 9999: %s\n", err)
		return
	}

	// get a reader from Stdin
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		// read from Stdin
		input := scanner.Text()

		// write it to the server
		_, err := rw.WriteString(input + "\n")
		if err != nil {
			fmt.Printf("There was an error writing to the server: %s\n", err)
			return
		}
		err = rw.Flush()
		if err != nil {
			fmt.Printf("There was an error writing to the server: %s\n", err)
			return
		}

		// read server's reply
		msg, err := rw.ReadString('\n')
		if err != nil {
			fmt.Printf("There was an error reading from the server: %s\n", err)
			return
		}

		// print server's reply
		fmt.Println(msg)
	}
}

// Open opens a connection to addr and returns back any errors
func Open(addr string) (*bufio.ReadWriter, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}
