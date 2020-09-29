// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

// maximum size of buffer to maintain for slow connections
const msgBufferSize = 500

type keyValueServer struct {
	// TODO: implement this!
	quit               bool
	listener           net.Listener
	clientDropChan     chan net.Conn
	clientActivateChan chan net.Conn
	clients            map[net.Conn]chan []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	server := keyValueServer{
		quit:               false,
		listener:           nil,
		clientDropChan:     make(chan net.Conn, 1),
		clientActivateChan: make(chan net.Conn, 1),
		clients:            make(map[net.Conn]chan []byte)}
	return &server
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	listener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	kvs.listener = listener
	initDB()
	go acceptClients(kvs)
	go handleConnections(kvs)
	return err
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.quit = true
	for conn := range kvs.clients {
		removeClient(kvs, conn)
	}
	kvs.listener.Close()
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return len(kvs.clients)
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	return nil
}

// TODO: add additional methods/functions below!

func acceptClients(kvs *keyValueServer) {
	for {
		conn, err := kvs.listener.Accept()
		if kvs.quit == true {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		kvs.clientActivateChan <- conn
	}
}

func handleConnections(kvs *keyValueServer) {
	for {
		select {
		case conn := <-kvs.clientActivateChan:
			kvs.clients[conn] = make(chan []byte, msgBufferSize)
			go readFromClient(kvs, conn)
			go writeToClient(kvs, conn)
		case conn := <-kvs.clientDropChan:
			removeClient(kvs, conn)
		default:
			if kvs.quit == true {
				return
			}
		}
	}
}

func readFromClient(kvs *keyValueServer, conn net.Conn) {
	clientReader := bufio.NewReader(conn)
	for {
		str, err := clientReader.ReadString('\n')
		buffer := []byte(str)
		if err != nil {
			kvs.clientDropChan <- conn
			return
		}
		// Parsing: requestMsg[0] = operation, requestMsg[1] = key, (if) requestMsg[2] = value
		requestMsg := bytes.Split(buffer, []byte(","))
		processRequest(kvs, conn, requestMsg)
	}
}

// all clients share one put channel, but can read simultaneously
func processRequest(kvs *keyValueServer, conn net.Conn, requestMsg [][]byte) {
	key := string(bytes.TrimSuffix(requestMsg[1][:], []byte("\n")))
	switch string(requestMsg[0][:]) {
	case "put":
		put(key, requestMsg[2][:]) //this lines causes error, need to fix it
	case "get":
		if len(kvs.clients[conn]) < msgBufferSize {
			responseMsg := append([]byte(key+","), get(key)...)
			kvs.clients[conn] <- responseMsg
		}
	}
}

func writeToClient(kvs *keyValueServer, conn net.Conn) {
	for {
		responseMsg := <-kvs.clients[conn]
		for client := range kvs.clients {
			// time.Sleep(75 * time.Millisecond)
			_, err := client.Write(responseMsg)
			if err != nil {
				if kvs.quit == false {
					kvs.clientDropChan <- client
				}
				return
			}
		}
	}
}

func removeClient(kvs *keyValueServer, conn net.Conn) {
	conn.Close()              //closes connection
	delete(kvs.clients, conn) //deletes it from map
}
