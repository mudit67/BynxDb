package core

import (
	"fmt"
	"io"
	"net"
	"strings"
)

func InitServer() {
	const PORT = 3030
	l, err := net.Listen("tcp", ":"+fmt.Sprint(PORT))
	fmt.Println("Lisening at: ", PORT)
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		// ! Not Using Go routines because Deadlocking is not implemented yet.
		handleConnection(c)
	}
}

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	packet := make([]byte, 1024)
	defer c.Close()
	// for {
	_, err := c.Read(packet)
	if err != nil {
		if err != io.EOF {
			fmt.Println("read error:", err)
		}
	}
	query := string(packet)

	fmt.Println(string(packet))
	_, err = c.Write(append([]byte("Received: "), packet...))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}

// Function to evaluate query for database commands.
func evaluateQuery(query string) {
	query = strings.TrimSpace(query)
	switch {
	case strings.HasPrefix(query, "PING"):
		fmt.Println("PONG")
	case strings.HasPrefix(query, "ECHO"):
		fmt.Println(strings.TrimPrefix(query, "ECHO "))
	default:
		fmt.Println("Unknown command")
	}
}
