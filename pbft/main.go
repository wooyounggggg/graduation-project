package main

import (
	"graduation-project/pbft/network"
	"os"
)

func main() {
	nodeID := os.Args[1]
	server := network.NewServer(nodeID)

	server.Start()
}
