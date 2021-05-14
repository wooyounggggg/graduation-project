package main

import (
	"fmt"
	"graduation-project/pbft/network"
	"os"
	"strconv" /* 문자열 -> 정수형 변환을 위한 module  */
)

func main() {
	nodeID := os.Args[1]
	N, err := strconv.Atoi(os.Args[2]) /* 2번째 실행 인자로 전체 node의 개수 N을 받습니다. */
	if err != nil {
		fmt.Println("err: strconv.Atoi err")
		os.Exit(1);
	}
	K, err := strconv.Atoi(os.Args[3]) /* 3번째 실행 인자로 클러스터의 개수 K를 받습니다. */
	if err != nil {
		fmt.Println("err: strconv.Atoi err")
		os.Exit(1);
	}
	server := network.NewServer(nodeID, N, K)

	server.Start()
}
