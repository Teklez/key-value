package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"
)

// Server struct
type Server struct {
	listener      net.Listener
	activeClients map[*Client]bool
	clientsMutex  sync.Mutex
	db            *sql.DB
}

// Client struct
type Client struct {
	conn   net.Conn
	name   string
	server *Server
}

// KeyValuePair struct
type KeyValuePair struct {
	Key   string
	Value string
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

		client := &Client{conn:conn}
		s.activeClients[client] = true

		// handle client request concurrently
		go s.handleRequest(client)

	}
}


// Handle client request

func (s *Server) handleRequest(client *Client){
	defer client.conn.Close()
	
	// Read client request
	scanner := bufio.NewScanner((client.conn))

	for scanner.Scan(){
		request := scanner.Text()\
		err := s.ProcessCommand(client, command)
		if err != nil {
			fmt.Fprintf(client.conn, "Error: %v\n", err)
			continue
		}

		if scanner.Err() != nil {
			fmt.Printf("Client %v disconnected with error: %v\n", client.conn.RemoteAddr(), scanner.Err())
		}else{
			fmt.Printf("Client %v disconnected gracefully\n", client.conn.RemoteAddr())
		}
	}
}

// Process command

func (s *Server) ProcessCommand(client *Client, command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command is not allowed")
	}

	switch strings.ToUpper(parts[0]) {
	case "PUT":
		if len(parts) < 3 {
			return fmt.Errorf("PUT command requires key and value")
		}
		return s.PUT(client, parts[1], parts[2])
	
	case "GET":
		if len(parts) < 2 {
			return fmt.Errorf("GET command requires key")
		}
		value, err := s.GET(client, parts[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(client.conn, "%s\n", value)
	case "DELETE":
		if len(parts) < 2 {
			return fmt.Errorf("DELETE command requires key")
		}

		return s.DELETE(client, parts[1])
	
	case "LIST":
		keyValues, err := s.ListKeyValues(client)
		if err != nil {
			err != nil {
				return err
			}
		for _, kv := range keyValues {
			fmt.Fprintf(client.conn, "%s: %s\n", kv.Key, kv.Value)
		}
	default:
		return fmt.Errorf("unknown command: %s", parts[0])
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
	err := s.db.QueryRow("SELECT value FROM key_value_store WHERE key = $1", key).Scan(&keyExist)
	if err == nil {
		// Key exists, update the value
		_, err = s.db.Exec("UPDATE key_value_store SET value = $1 WHERE key = $2", value, key)
		if err != nil {
			return fmt.Errorf("failed to update key-value pair: %v", err)
		}
		fmt.Fprintf(client.conn, "Updated key %s with value %s\n", key, value)
		return nil
	} else if err != sql.ErrNoRows {
		// Database error
		return fmt.Errorf("failed to query database: %v", err)
	}

	// Key does not exist, insert the new key-value pair
	_, err = s.db.Exec("INSERT INTO key_value_store (key, value) VALUES ($1, $2)", key, value)
	if err != nil {
		return fmt.Errorf("failed to insert key-value pair: %v", err)
	}
	fmt.Fprintf(client.conn, "Inserted key %s with value %s\n", key, value)
	return nil
}

// Get key value
func (s *Server) GET(client *Client, key string) (string, error) {
	// Lock database access to prevent race conditions
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	var value string
	err := s.db.QueryRow("SELECT value FROM key_value_store WHERE key = $1", key).Scan(&value)
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
	_, err := s.db.Exec("DELETE FROM key_value_store WHERE key = $1", key)
	if err != nil {
		return fmt.Errorf("failed to delete key-value pair: %v", err)
	}

	fmt.Fprintf(client.conn, "Deleted key %s\n", key)
	return nil
}


// List key values
func (s *Server) LIST(client *Client) ([]KeyValuePair, error) {
	// Lock database access to prevent race conditions
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	// Fetch all key-value pairs
	rows, err := s.db.Query("SELECT key, value FROM key_value_store")
	if err != nil {
		return nil, fmt.Errorf("failed to list key-value pairs: %v", err)
	}
	defer rows.Close()

	var keyValues []KeyValuePair
	for rows.Next() {
		var kv KeyValuePair
		if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
			return nil, fmt.Errorf("failed to scan key-value pair: %v", err)
		}
		keyValues = append(keyValues, kv)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error while reading rows: %v", err)
	}

	return keyValues, nil
}

