package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"graduation-project/pbft/consensus"
	"net/http"
)

type Server struct {
	url  string
	node *Node
}

func NewServer(nodeID string, N int, K int) *Server {
	node := NewNode(nodeID, N, K)
	server := &Server{node.NodeTable[nodeID], node}

	server.setRoute()

	return server
}

func (server *Server) Start() {
	fmt.Printf("Server will be started at %s...\n", server.url)
	if err := http.ListenAndServe(server.url, nil); err != nil {
		fmt.Println(err)
		return
	}
}

func (server *Server) setRoute() {
	http.HandleFunc("/req", server.getReq)
	http.HandleFunc("/preprepare", server.getPrePrepare)
	http.HandleFunc("/prepare", server.getPrepare)
	http.HandleFunc("/commit", server.getCommit)
	http.HandleFunc("/reply", server.getReply)

	// View change
	http.HandleFunc("/viewchange", server.getViewChange)
	http.HandleFunc("/newview", server.getNewView)
	http.HandleFunc("/authorization", server.getauthorize) /*primary node가 reply받은 후 node table의 전체 노드에게 보내지면 getauthorize함수를 실행합니다*/
}

func (server *Server) getReq(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.RequestMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getPrePrepare(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.PrePrepareMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getPrepare(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getCommit(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getReply(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.ReplyMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.GetReply(&msg)
}

func (server *Server) getViewChange(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.ViewChangeMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}


func (server *Server) getNewView(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.NewViewMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func send(url string, msg []byte) error {
	buff := bytes.NewBuffer(msg)
	c := &http.Client{}

	resp, err := c.Post("http://"+url, "application/json", buff)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
/*
	node.GetAuthorize()를 실행합니다
*/
func (server *Server) getauthorize(writer http.ResponseWriter, request *http.Request) {
	server.node.GetAuthorize()
}

func send(url string, msg []byte) {
	buff := bytes.NewBuffer(msg)
	http.Post("http://"+url, "application/json", buff)
}
