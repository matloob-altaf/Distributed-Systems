package myrpc

// EchoArgs contains the Echo RPC arguments
type EchoArgs struct {
	Input string
}

// EchoReply contains the Echo RPC reply
type EchoReply struct {
	Output string
}

// AddArgs contains the Add RPC args
type AddArgs struct {
	OperandA float32
	OperandB float32
}

// AddReply contains the Add RPC replies
type AddReply struct {
	Result float32
}
