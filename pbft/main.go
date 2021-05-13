package main

import (
	"os"
	"graduation-project/pbft/network"
)

func main() {
	nodeID := os.Args[1]
	server := network.NewServer(nodeID)

	server.Start()
}
