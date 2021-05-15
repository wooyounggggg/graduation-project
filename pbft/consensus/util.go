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

func LeaderMapping(nodeId string) bool {
	return false
}

/** 
	func MakeNodeTable
	@brief
		1~N번까지 노드의 번호를 기반으로
		{ nodeId : 'localhost:11XX'}의 형태로 만들어줍니다.
	@params
		max int : 노드의 총 개수
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