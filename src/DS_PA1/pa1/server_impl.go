// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
)

// maximum size of buffer to maintain for slow connections
const msgBufferSize = 500

type keyValueServer struct {
	// TODO: implement this!
	listener        *net.TCPListener
	currentClientID int
	operationChan   chan int
	responseMsgChan chan []byte
	clients         chan map[int]*client
	rpcOp           chan int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	server := &keyValueServer{
		currentClientID: 0,
		operationChan:   make(chan int, 1),
		responseMsgChan: make(chan []byte, 1),
		clients:         make(chan map[int]*client, 1),
		rpcOp:           make(chan int, 1)}

	server.operationChan <- 1
	server.clients <- make(map[int]*client, 1)
	return server
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	kvs.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	go kvs.AcceptClients()
	go kvs.WriteToClientBuffers()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.listener.Close()
	clients := <-kvs.clients
	for _, cl := range clients {
		cl.conn.Close()
		cl.writeChan <- 1
	}
	kvs.clients <- clients
	close(kvs.responseMsgChan)
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	clients := <-kvs.clients
	count := len(clients)
	kvs.clients <- clients
	return count
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!

	ln, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return err
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.Wrap(kvs))
	http.DefaultServeMux = http.NewServeMux() //workaround mentioned in assignment handout
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	kvs.rpcOp <- 1
	go http.Serve(ln, nil)

	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	_, ok := <-kvs.rpcOp
	if !ok {
		return nil
	}
	reply.Value = get(args.Key)
	kvs.rpcOp <- 1
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	_, ok := <-kvs.rpcOp
	if !ok {
		return nil
	}
	put(args.Key, args.Value)
	kvs.rpcOp <- 1
	return nil
}

// TODO: add additional methods/functions below!

type client struct {
	id                int
	conn              *net.TCPConn
	readChan          chan int
	writeChan         chan int
	responseMsgBuffer chan []byte
	server            *keyValueServer
}

func (kvs *keyValueServer) AcceptClients() {
	for {
		conn, err := kvs.listener.AcceptTCP()
		if err != nil {
			return
		}
		cl := &client{
			id:                kvs.currentClientID,
			conn:              conn,
			writeChan:         make(chan int, 1),
			readChan:          make(chan int, 1),
			responseMsgBuffer: make(chan []byte, msgBufferSize),
			server:            kvs}
		kvs.currentClientID++
		clients := <-kvs.clients
		clients[cl.id] = cl
		kvs.clients <- clients
		go cl.ReadFromClient()
		go cl.WriteToClient()
	}
}

func (cl *client) ReadFromClient() {
	reader := bufio.NewReader(cl.conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			cl.readChan <- 1
			return
		}
		responseFlag, responseMsg := cl.server.Parse(string(msg))
		switch responseFlag {
		case true:
			select {
			case cl.server.responseMsgChan <- []byte(responseMsg):
				break
			case <-cl.writeChan:
				cl.readChan <- 1
				return
			}
		case false:
			select {
			case <-cl.writeChan:
				cl.readChan <- 1
				return
			default:
				break
			}
		}
	}
}

func (cl *client) WriteToClient() {
	for {
		select {
		case data, ok := <-cl.responseMsgBuffer:
			if !ok {
				return
			}
			_, err := cl.conn.Write(data)
			if err != nil {
				return
			}
		case <-cl.readChan:
			clients := <-cl.server.clients
			delete(clients, cl.id)
			cl.server.clients <- clients
			return
		}
	}
}

func (kvs *keyValueServer) WriteToClientBuffers() {
	for {
		select {
		case data, ok := <-kvs.responseMsgChan:
			if !ok {
				return
			}
			clients := <-kvs.clients
			for _, cl := range clients {
				select {
				case cl.responseMsgBuffer <- data:
					break
				default:
					break
				}
			}
			kvs.clients <- clients
		}
	}
}

func (kvs *keyValueServer) Parse(msg string) (bool, string) {
	requestMsg := strings.Split(msg, ",")
	switch strings.EqualFold(requestMsg[0], "put") {
	case true:
		requestMsg[2] = strings.TrimSpace(requestMsg[2])
		<-kvs.operationChan
		put(requestMsg[1], []byte(requestMsg[2]))
		kvs.operationChan <- 1
		return false, ""
	case false:
		requestMsg[1] = strings.TrimSpace(requestMsg[1])
		<-kvs.operationChan
		value := get(requestMsg[1])
		responseMsg := fmt.Sprintf("%v,%v\n", requestMsg[1], string(value))
		kvs.operationChan <- 1
		return true, responseMsg
	default:
		return false, ""
	}
}
