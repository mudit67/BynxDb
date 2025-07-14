package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Initiating Client")

	if len(os.Args) < 2 {
		fmt.Println("Fatal Error: PORT not provided!")
		os.Exit(1)
	}
	PORT := os.Args[1]
	fmt.Println(PORT)

	if _, err := fmt.Sscanf(PORT, "%d", new(int)); err != nil {
		fmt.Println("Fatal Error: Invalid PORT number!")
		os.Exit(1)
	}

	fmt.Printf("Client is running on port %s\n", PORT)
	conn, err := initTcpClient(PORT)
	if err != nil {
		panic(err)
	}

	for {

		line, err := readLineFromStdin(PORT)
		if err != nil {
			fmt.Println("Error reading from stdin:", err)
			os.Exit(1)
		}

		if len(line) == 0 {
			fmt.Println("Error: Input cannot be empty.")
			continue
		}
		if line[len(line)-1] != ';' {
			fmt.Println("Error: Input must end with a semicolon.")
			continue
		}

		if line == "exit;" {
			fmt.Println("Exiting client...")
			os.Exit(0)
		}
		// Send the line to the server
		_, err = conn.Write([]byte(line))
		if err != nil {
			fmt.Println("Error sending data to server:", err)
			os.Exit(1)
		}

		// Read the response from the server
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from server:", err)
			os.Exit(1)
		}

		response := string(buffer[:n])
		fmt.Println("Server response:", response)

	}
}

func readLineFromStdin(PORT string) (string, error) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print(PORT + "> ")

	// Scan for the next line of input
	scanner.Scan()

	// Get the text from the scanned line
	line := scanner.Text()

	// Check for any errors during scanning
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return line, nil
}

func initTcpClient(PORT string) (net.Conn, error) {
	// Placeholder for TCP client initialization logic
	conn, err := net.Dial("tcp", "localhost:"+PORT)
	if err != nil {
		return nil, err
	}
	fmt.Println("TCP Client initialized")
	return conn, nil
}
