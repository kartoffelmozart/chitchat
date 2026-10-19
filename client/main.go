package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	pb "chitchat/proto"

	"chitchat/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	pb.ChitChatClient
	name                   string
	clock                  *util.LamportClock
	stream                 grpc.BidiStreamingClient[pb.ChatMessage, pb.ChatMessage]
	contextCancelFunc      context.CancelFunc
	grpc_client_connection *grpc.ClientConn
	quit                   chan struct{}
}

func NewClient(quit chan struct{}) *Client {

	// Connect to the gRPC server.
	grpc_client_connection, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	// Create a client for the Chat service.
	grpc_client := pb.NewChitChatClient(grpc_client_connection)

	// Give the RPC a maximum amount of time to complete.
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := grpc_client.OpenChatStream(ctx)

	if err != nil {
		cancel()
		log.Fatalf("RPC failed: %v", err)
	}

	welcome_msg, err := stream.Recv()
	if err != nil {
		log.Fatalf("failed to receive welcome msg: %v", err)
	}
	displayMsg(welcome_msg)

	var name string

	fmt.Scanln(&name)

	for {

		stream.Send(&pb.ChatMessage{
			Sender: name,
			Body:   "",
			Type:   pb.MessageType_Connect,
		})

		name_dialogue_msg, err := stream.Recv()

		if err != nil {
			cancel()
			log.Fatalf("failed during name diagloue: %v", err)
		}

		if name_dialogue_msg.Type == pb.MessageType_NameTakenError {

			fmt.Println("This name is already taken! Try another one: ")
			fmt.Scanln(&name)

		} else {
			return &Client{
				ChitChatClient:         grpc_client,
				name:                   name,
				clock:                  util.NewLamportClock(),
				stream:                 stream,
				contextCancelFunc:      cancel,
				grpc_client_connection: grpc_client_connection,
				quit:                   quit,
			}
		}
	}
}

func (this *Client) sendMessageLoop() {

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {

		msg := scanner.Text()

		if msg == "quit" {
			close(this.quit)
			return
		}

		this.sendMessage(msg)
	}
}

// protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/chitchat.proto

func (this *Client) sendMessage(msg_body string) {

	this.clock.Incr()

	this.stream.Send(&pb.ChatMessage{
		Sender: this.name,
		Body:   msg_body,
		Type:   pb.MessageType_NormalChatMessage,
	})
}

func (this *Client) receiveMessage() {

	msg, err := this.stream.Recv()

	if err != nil {
		this.contextCancelFunc()
		log.Fatalf("RPC failed: %v", err)
	}

	this.clock.Update(msg.GetTimestamp())

	displayMsg(msg)
}

func (this *Client) receiveMessageLoop() {
	for {
		this.receiveMessage()
	}
}

func (this *Client) disconnect() {
	this.stream.Send(&pb.ChatMessage{
		Sender: this.name,
		Body:   "",
		Type:   pb.MessageType_Disconnect,
	})
	// TODO: Receive message from server before closing? To make sure the server received the disconnect
	this.contextCancelFunc()
	this.grpc_client_connection.Close()
}

func displayMsg(msg *pb.ChatMessage) {
	log.Println("CLIENT: " + msg.Sender + " at " + strconv.Itoa(int(msg.GetTimestamp())) + ": " + msg.GetBody())
}

func main() {
	quit := make(chan struct{})

	fmt.Println("(You can quit any time by entering the word 'quit')")

	client := NewClient(quit)

	go client.sendMessageLoop()
	go client.receiveMessageLoop()

	<-quit

	client.disconnect()

	log.Println("Client finished")

}
