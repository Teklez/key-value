package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Connect to the server
	serverAddress := "localhost:8080"
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to server %s\n", serverAddress)

	// Create a channel to listen for interrupt signals 
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// Set a timeout for reading responses from the server
	timeoutDuration := 1 * time.Minute
	conn.SetReadDeadline(time.Now().Add(timeoutDuration))

	// Start reading user commands and sending them to the server
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-stopChan:
			fmt.Println("\nReceived interrupt signal, exiting client...")
			return
		default:
			fmt.Print("Client: ")
			scanner.Scan()
			command := scanner.Text()

			// Exit if the user types "exit"
			if command == "exit" {
				fmt.Println("Exiting client...")
				return
			}

			// Send the command to the server
			_, err := fmt.Fprintf(conn, command+"\n")
			if err != nil {
				fmt.Printf("Error sending command to server: %v\n", err)
				continue
			}

			// Receive the response from the server
			response, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				// Check if the error is a timeout or a connection issue
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					fmt.Printf("Error: Server response timeout\n")
				} else {
					fmt.Printf("Error reading response from server: %v\n", err)
				}
				continue
			}

			// Print the server's response
			fmt.Printf("Server: %s", response)
		}
	}
}
