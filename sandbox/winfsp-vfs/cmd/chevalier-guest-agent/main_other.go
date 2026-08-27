//go:build !windows

package main

import "fmt"

func main() {
	fmt.Println("chevalier-guest-agent runs only on Windows")
}
