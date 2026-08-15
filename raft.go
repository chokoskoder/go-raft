package goraft

import (
	"net/http"
	"net/rpc"
	"sync"
)

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Command []byte
	Term    uint64
	// set by the primary so it can learn about the result of applying this command to the state machine

	result chan ApplyResult
}

type ClusterMember struct {
	Id      uint64
	Address string

	// index of the next log entry to send
	nextIndex uint64

	// highest log entry to replicated
	matchIndex uint64

	// who ws voted for in the most recent term
	votedFor uint64

	// TCP connection
	rpcdlient *rpc.Client
}


type ServerState string

const (
    leaderState    ServerState = "leader"
    followerState              = "follower"
    candidateState             = "candidate"
)

type Server struct{
	done bool 
	server *http.Server
	Debug bool 

	mu sync.Mutex

	// ---------------- PERSISTEN state ------------------- 
	// the curren term 
	currentTerm uint64

	log []Entry

	//votedFor is stored in 'cluster []ClusterMember' below, mapped by 'clusterIndex'
	/ ----------- READONLY STATE -----------

    // Unique identifier for this Server
    id uint64

    // The TCP address for RPC
    address string

    // When to start elections after no append entry messages
    electionTimeout time.Time

    // How often to send empty messages
    heartbeatMs int

    // When to next send empty message
    heartbeatTimeout time.Time

    // User-provided state machine
    statemachine StateMachine

    // Metadata directory
    metadataDir string

    // Metadata store
    fd *os.File
}
