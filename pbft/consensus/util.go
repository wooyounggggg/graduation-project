package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
)

func HandleError(err error, message string) {
	if err != nil {
		fmt.Println(message)
		os.Exit(1)
	}
}
func CustomAtoi(str string) int {
	strToNum, err := strconv.Atoi(str)
	HandleError(err, "err: strconv.Atoi err")
	return strToNum
}

func Hash(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

/**
	func LeaderMapping
	
	@brief
		nodeId를 전달받아 해당 nodeId의 cluster leader id를
		찾아 반환합니다.

	@params
		string nodeId: Leader와 mapping하려는 node의 Id
  */
func LeaderMapping(nodeId string, N int, K int) string {
	// N = 20, K = 5 quot = 4
	// 1,2,3,4 / 5,6,7,8 / .... / 17,18,19,20
	// 15 -> 20 / 5 = 4 (1, 2, 3, 4)
	// 
	quotient := N / K
	if (CustomAtoi(nodeId) <= quotient ){
		fmt.Println("nodeId: "+nodeId + " LeaderId: -1")
		return "-1"
	}
	idInt := CustomAtoi(nodeId)-1
	clusterNo := idInt / quotient
	result := strconv.Itoa(clusterNo * quotient + 1)
	fmt.Println("nodeId: "+nodeId + " LeaderId: " + result)
	return result
}

/** 
	func MakeNodeTable
	
	@brief
		1~N번까지 노드의 번호를 기반으로
		{ nodeId: 'localhost:11XX'}의 형태로 만들어줍니다.

	@return
		{ 1: 'localhost:1111', 2: 'localhost:1112', ...}
	@params
		int max: 노드의 총 개수
*/
func MakeNodeTable(max int) map[string]string {
    min := 1
	nodeArray := make([]int, max-min+1)
    for i := range nodeArray {
        nodeArray[i] = min + i
    }
	nodeTable := map[string]string {}
	for i := range nodeArray {
		nodeTable[strconv.Itoa(i+1)] = "localhost:" + strconv.Itoa(1110 + i+1)
	}
    return nodeTable
}