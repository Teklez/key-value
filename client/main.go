package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// Connect to the server
	serverAddress := "localhost:8080" // Change this if necessary
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to server %s\n", serverAddress)

	// Start reading user commands and sending to server
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter command (PUT key value, GET key, DELETE key, LIST): ")
		scanner.Scan()
		command := scanner.Text()

		// Exit if the user types "exit"
		if command == "exit" {
			fmt.Println("Exiting client...")
			break
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
			fmt.Printf("Error reading response from server: %v\n", err)
			continue
		}

		// Print the server's response
		fmt.Printf("Server response: %s", response)
	}
}
