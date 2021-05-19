package network

import (
	"fmt"
	"graduation-project/pbft/consensus"
	// "time"
)

func LogMsg(msg interface{}) {
	// t := time.Now().UnixNano()

	switch m := msg.(type) {
	case *consensus.RequestMsg:
		fmt.Printf("[REQUEST] ClientID: %s, Timestamp: %d, Operation: %s\n", m.ClientID, m.Timestamp, m.Operation)
	case *consensus.PrePrepareMsg:
		fmt.Printf("[PREPREPARE] SequenceID: %d\n", m.SequenceID)
	case *consensus.VoteMsg:
		if m.MsgType == consensus.PrepareMsg {
			fmt.Printf("[PREPARE] NodeID: %s\n", m.NodeID)
		} else if m.MsgType == consensus.CommitMsg {
			fmt.Printf("[COMMIT] NodeID: %s\n", m.NodeID)
		}
	case *consensus.ReplyMsg:
		fmt.Printf("[REPLY] Result: %s by %s\n", m.Result, m.NodeID)
	case *consensus.ViewChangeMsg:
		fmt.Printf("[ViewChangeMsg] NodeID: %s\n", m.NodeID)
	}
}

func LogStage(stage string, isDone bool) {
	if isDone {
		fmt.Printf("[STAGE-DONE] %s\n", stage)
	} else {
		fmt.Printf("[STAGE-BEGIN] %s\n", stage)
	}
}
