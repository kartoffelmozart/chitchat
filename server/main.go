package main

import (
	pb "chitchat/proto"
	"chitchat/util"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"google.golang.org/grpc"
)

// chatServer implements the Chat service defined in chat.proto.
type chitChatServer struct {
	pb.UnimplementedChitChatServer

	mu             sync.Mutex
	client_chans   map[string]chan *pb.ChatMessage
	done_chans     map[string]chan bool
	broadcast_chan chan *pb.ChatMessage
	clock          *util.LamportClock
}

func NewServer() *chitChatServer {
	return &chitChatServer{}
}

func (this *chitChatServer) OpenChatStream(stream pb.ChitChat_OpenChatStreamServer) error {
	// TODO: figure out what to lock everywhere

	name := this.connectUser(stream)

	this.clock.Incr()
	this.broadcast_chan <- &pb.ChatMessage{
		Sender:    "SERVER",
		Body:      "Participant " + name + " joined Chit Chat at logical time " + strconv.Itoa(int(this.clock.GetTimestamp())),
		Timestamp: this.clock.GetTimestamp(),
		Type:      pb.MessageType_NormalChatMessage,
	}

	error_ch := make(chan error)

	go this.receiveLoop(stream, error_ch)
	go this.sendLoop(name, stream, error_ch)

	return <-error_ch
}

func (this *chitChatServer) receiveLoop(stream pb.ChitChat_OpenChatStreamServer, error_ch chan error) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatalf("failed to receive: %v", err)
			error_ch <- err
			return
		}

		// log.Printf()

		this.clock.Update(msg.GetTimestamp())

		if msg.Type == pb.MessageType_NormalChatMessage {

			this.broadcast_chan <- &pb.ChatMessage{
				Sender:    msg.GetSender(),
				Body:      msg.GetBody(),
				Timestamp: this.clock.GetTimestamp(),
			}
		} else if msg.Type == pb.MessageType_Connect {

		} else if msg.Type == pb.MessageType_Disconnect {

			this.disconnect(msg.GetSender())

			this.broadcast_chan <- &pb.ChatMessage{
				Sender:    "SERVER",
				Body:      "Participant " + msg.GetSender() + " left Chit Chat at logical time " + strconv.Itoa(int(this.clock.GetTimestamp())),
				Timestamp: this.clock.GetTimestamp(),
				Type:      pb.MessageType_NormalChatMessage,
			}
			return
		}
	}

}

func (this *chitChatServer) sendLoop(name string, stream pb.ChitChat_OpenChatStreamServer, error_ch chan error) {

	for {
		select {

		case msg := <-this.client_chans[name]:

			err := stream.Send(msg)
			if err != nil {
				log.Printf("server failed to send: %v", err)
				error_ch <- err
				return
			}

		case <-this.done_chans[name]:
			error_ch <- nil
			return
		}
	}
}

func (this *chitChatServer) connectUser(stream pb.ChitChat_OpenChatStreamServer) string {
	this.clock.Incr()

	var connect_msg *pb.ChatMessage
	var connect_msg_err error
	var name string

	stream.Send(&pb.ChatMessage{
		Sender:    "SERVER",
		Body:      "Hello and welcome to ChitChat! Please enter your name: ",
		Timestamp: this.clock.GetTimestamp(),
		Type:      pb.MessageType_Connect,
	})

	for {

		connect_msg, connect_msg_err = stream.Recv()
		if connect_msg_err != nil || connect_msg.Type != pb.MessageType_Connect {
			log.Printf("SERVER: Failed to receive connect message")
		}

		name = connect_msg.Sender

		this.mu.Lock()

		_, name_taken := this.client_chans[name]

		if !name_taken {

			this.client_chans[name] = make(chan *pb.ChatMessage)
			this.done_chans[name] = make(chan bool)

			this.mu.Unlock()

			stream.Send(&pb.ChatMessage{ // send token msg to let the user know they connected
				Sender:    "",
				Body:      "",
				Timestamp: this.clock.GetTimestamp(),
				Type:      pb.MessageType_Connect,
			})
			log.Println("SERVER: Connected to " + name)
			return name

		} else {

			stream.Send(&pb.ChatMessage{
				Sender:    "",
				Body:      "",
				Timestamp: this.clock.GetTimestamp(),
				Type:      pb.MessageType_NameTakenError,
			})

			this.mu.Unlock()
		}
	}

}

func (this *chitChatServer) broadcastLoop() {
	for {

		msg := <-this.broadcast_chan
		log.Println("SERVER: Broadcasting message: '" + msg.Body + "' from " + msg.Sender + " at " + strconv.Itoa(int(msg.Timestamp)) + ".")
		for _, ch := range this.client_chans {
			ch <- msg
		}
	}
}

func NewChitChatServer() *chitChatServer {
	server := &chitChatServer{

		mu:             sync.Mutex{},
		client_chans:   map[string]chan *pb.ChatMessage{},
		done_chans:     map[string]chan bool{},
		broadcast_chan: make(chan *pb.ChatMessage),
		clock:          util.NewLamportClock(),
	}

	go server.broadcastLoop()
	return server
}

func (this *chitChatServer) disconnect(name string) {
	// TODO: figure out where to lock
	// TODO: properly disconnect
	this.done_chans[name] <- true
	this.mu.Lock()
	delete(this.client_chans, name)
	delete(this.done_chans, name)
	this.mu.Unlock()
	log.Println("SERVER: disconnected " + name)
}

func main() {
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()

	// File only:
	// log.SetOutput(logFile)

	// File and terminal:
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	// Create a TCP listener on port 50051.
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server.
	grpcServer := grpc.NewServer()

	chitChatServer := NewChitChatServer()

	// Register our ChitChatServer implementation with the gRPC server.
	pb.RegisterChitChatServer(grpcServer, chitChatServer)

	log.Println("SERVER: Started and listening on :50051")

	// Start accepting gRPC connections.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
