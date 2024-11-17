package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

// Server struct
type Server struct {
	listener      net.Listener
	activeClients map[*Client]bool
	dbMutex       sync.Mutex
	db            *sql.DB
}

// Client struct
type Client struct {
	conn net.Conn
}

// KeyValuePair struct
type KeyValuePair struct {
	Key   string
	Value string
}

// Database connection string
const connStr = "postgres://zemen:justme@localhost:5432/key_value_store?sslmode=disable"

// main function

func main() {
	server := &Server{}

	// Set a timeout duration for the server
	timeout := 1 * time.Minute

	// Create a channel to listen for termination signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// Attempt to connect to the database
	err := server.connectDB()
	if err != nil {
		fmt.Printf("Error connecting to the database: %v\n", err)
		return
	}

	// Ensure database connection is closed when the main function exits
	defer server.db.Close()

	// Set a timeout for the server startup
	serverStartTimer := time.NewTimer(timeout)

	// Start the server on a specified port
	port := "8080"
	go func() {
		err = server.Start(port)
		if err != nil {
			fmt.Printf("Error starting server: %v\n", err)
		}
	}()

	// Wait for a signal to stop the server or timeout
	select {
	case <-stopChan:
		//  shut down the server on receiving an interrupt or termination signal
		fmt.Println("\nReceived shutdown signal, shutting down server...")
		if err := server.listener.Close(); err != nil {
			fmt.Printf("Error closing listener: %v\n", err)
		}
		fmt.Println("Server shut down .")
	case <-serverStartTimer.C:
		// Timeout occurred while starting the server
		fmt.Println("\nServer startup timed out. Shutting down...")
		if err := server.listener.Close(); err != nil {
			fmt.Printf("Error closing listener: %v\n", err)
		}
	}
}

// Start server
func (s *Server) Start(port string) error {
	// Start TCP server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}

	s.listener = listener
	s.activeClients = make(map[*Client]bool)

	fmt.Printf("Server started on port %s\n", port)

	// Accept incoming connections

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %v", err)
		}

		client := &Client{conn: conn}
		s.activeClients[client] = true

		// handle client request concurrently
		go s.handleRequest(client)

	}
}

// Handle client request
func (s *Server) handleRequest(client *Client) {
	defer client.conn.Close()

	// Read client request
	scanner := bufio.NewScanner((client.conn))

	for scanner.Scan() {
		request := scanner.Text()
		err := s.ProcessCommand(client, request)
		if err != nil {
			fmt.Fprintf(client.conn, "Error: %v\n", err)
			continue
		}

		if scanner.Err() != nil {
			fmt.Printf("Client %v disconnected with error: %v\n", client.conn.RemoteAddr(), scanner.Err())
		} else {
			fmt.Printf("Client %v disconnected \n", client.conn.RemoteAddr())
		}
	}
}

// Process command
func (s *Server) ProcessCommand(client *Client, command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("Error: empty command is not allowed")
	}

	switch strings.ToUpper(parts[0]) {
	case "PUT":
		fmt.Println("PUT request ............")
		if len(parts) < 3 {
			return fmt.Errorf("Error: PUT command requires key and value")
		}
		return s.PUT(client, parts[1], parts[2])

	case "GET":
		fmt.Println("GET request ............")
		if len(parts) < 2 {
			return fmt.Errorf("Error: GET command requires key")
		}
		value, err := s.GET(client, parts[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(client.conn, "%s\n", value)
		return nil

	case "DELETE":
		fmt.Println("DELETE request ............")
		if len(parts) < 2 {
			return fmt.Errorf("Error: DELETE command requires key")
		}

		return s.DELETE(client, parts[1])

	case "LIST":
		fmt.Println("LIST request ............")
		err := s.LIST(client)
		if err != nil {
			return err
		}

	default:
		fmt.Println("Unknown command ............")
		return fmt.Errorf("Error: unknown command: %s", parts[0])
	}
	return nil
}

// DATABASE OPERATIONS

// connect to database

func (s *Server) connectDB() error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	s.db = db
	return nil
}

// Put key value
func (s *Server) PUT(client *Client, key, value string) error {
	// Lock database access to prevent concurrent modification
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	// Check if key already exists
	var keyExist string
	err := s.db.QueryRow("SELECT value FROM key_value_pairs WHERE key = $1", key).Scan(&keyExist)
	if err == nil {
		// Key exists, update the value
		_, err = s.db.Exec("UPDATE key_value_pairs SET value = $1 WHERE key = $2", value, key)
		if err != nil {
			return fmt.Errorf("failed to update key-value pair: %v", err)
		}
		fmt.Fprintf(client.conn, "Updated key %s with value %s\n", key, value)
		return nil
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("failed to query database: %v", err)
	}

	// Key does not exist, insert the new key-value pair
	_, err = s.db.Exec("INSERT INTO key_value_pairs (key, value) VALUES ($1, $2)", key, value)
	if err != nil {
		return fmt.Errorf("failed to insert key-value pair: %v", err)
	}
	fmt.Fprintf(client.conn, "OK\n")
	return nil
}

// Get key value
func (s *Server) GET(client *Client, key string) (string, error) {
	// Lock database access to prevent race conditions
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	var value string
	err := s.db.QueryRow("SELECT value FROM key_value_pairs WHERE key = $1", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("key %s not found", key)
		}
		return "", fmt.Errorf("failed to retrieve value for key %s: %v", key, err)
	}

	return value, nil
}

// Delete key value
func (s *Server) DELETE(client *Client, key string) error {
	// Lock database access to prevent race conditions
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	// Delete the key-value pair
	_, err := s.db.Exec("DELETE FROM key_value_pairs WHERE key = $1", key)
	if err != nil {
		return fmt.Errorf("failed to delete key-value pair: %v", err)
	}

	fmt.Fprintf(client.conn, "OK\n")
	return nil
}

// List key values
func (s *Server) LIST(client *Client) error {
	// Lock database access to prevent race conditions
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	// Fetch all key-value pairs
	rows, err := s.db.Query("SELECT * FROM key_value_pairs")
	if err != nil {
		return fmt.Errorf("failed to list key-value pairs: %v", err)
	}
	defer rows.Close()
	var KeyValues string
	for rows.Next() {
		var kv KeyValuePair
		if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
			return fmt.Errorf("failed to scan key-value pair: %v", err)
		}
		KeyValues += fmt.Sprintf("%s: %s, ", kv.Key, kv.Value)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error while reading rows: %v", err)
	}
	KeyValues = strings.TrimSuffix(KeyValues, ",")
	KeyValues += "\n"
	fmt.Fprintf(client.conn, "%s", KeyValues)
	return nil
}
