package config

import (
	"bufio"
	"fmt"
	"os"
)

func StartCLI() *bufio.Scanner {
	fmt.Println("Connected to RPC server. Type 'SET' or 'GET' to interact.")
	fmt.Println("Usage examples:")
	fmt.Println("  SET <bucket> <key> <value>")
	fmt.Println("  GET <bucket> <key>")

	return bufio.NewScanner(os.Stdin)
}
