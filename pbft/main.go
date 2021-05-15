package main

import (
	"graduation-project/pbft/consensus"
	"graduation-project/pbft/network"
	"os"
	/* 문자열 -> 정수형 변환을 위한 module  */)

func main() {
	nodeID := os.Args[1]
	N := consensus.CustomAtoi(os.Args[2]) /* 2번째 실행 인자로 전체 node의 개수 N을 받습니다. */
	K := consensus.CustomAtoi(os.Args[3]) /* 3번째 실행 인자로 클러스터의 개수 K를 받습니다. */
	server := network.NewServer(nodeID, N, K)

	server.Start()
}
