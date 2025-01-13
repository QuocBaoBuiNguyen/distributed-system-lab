package config

import (
	"bufio"
	"fmt"
	"os"
)

func StartCLI() *bufio.Scanner {
	fmt.Println("Connected to RPC server. Type 'SET', 'GET', 'GETALL' or 'DELETE' to interact.")
	fmt.Println("Usage examples:")
	fmt.Println("  SET <bucket> <key> <value>")
	fmt.Println("  GET <bucket> <key>")
	fmt.Println("  GETALL <bucket>")
	fmt.Println("  DELETE <bucket> <key>")
	return bufio.NewScanner(os.Stdin)
}
