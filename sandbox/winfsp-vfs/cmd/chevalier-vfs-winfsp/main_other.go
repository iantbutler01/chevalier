//go:build !windows

package main

import "fmt"

func main() {
	fmt.Println("chevalier-vfs-winfsp runs only on Windows")
}
