package goraft

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
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

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    uint64
	Log         []Entry
}

type Server struct {
	done   bool
	server *http.Server
	Debug  bool

	mu sync.Mutex

	// ---------------- PERSISTENT state -------------------
	// the curren term
	currentTerm uint64

	log []Entry

	// votedFor is stored in 'cluster []ClusterMember' below, mapped by 'clusterIndex'
	// ----------- READONLY STATE -----------

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

	// ----------- VOLATILE STATE -----------

	// Index of highest log entry known to be committed
	commitIndex uint64

	// Index of highest log entry applied to state machine
	lastApplied uint64

	// Candidate, follower, or leader
	state ServerState

	// Servers in the cluster, including this one
	cluster []ClusterMember

	// Index of this server
	clusterIndex int
}

func NewServer(
	clusterConfig []ClusterMember,
	stateMachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("id must not be set to 0")
		}
		cluster = append(cluster, c)
	}

	return &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		statemachine: stateMachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  300,
		mu:           sync.Mutex{},
	}
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}

	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func (s *Server) warnf(msg string, args ...any) {
	fmt.Println(fmt.Sprintf(msg, args...))
}

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}

func (s *Server) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fd.Truncate(0)
	s.fd.Seek(0, 0)
	enc := gob.NewEncoder(s.fd)
	err := enc.Encode(PersistentState{
		CurrentTerm: s.currentTerm,
		Log:         s.log,
		VotedFor:    s.cluster[s.clusterIndex].votedFor,
	})
	if err != nil {
		panic(err)
	}
	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debug(fmt.Sprintf("Persisted. Term: %d. Log Len: %d. Voted For: %s.", s.currentTerm, len(s.log), s.cluster[s.clusterIndex].votedFor))
}

func (s *Server) restore() {
	var ps PersistentState
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0o755,
		)
		if err != nil {
			panic(err)
		}
	}

	s.fd.Seek(0, 0)
	err := gob.NewDecoder(s.fd).Decode(&ps)
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}

	s.currentTerm = ps.CurrentTerm
	s.setVotedFor(ps.VotedFor)
	s.log = ps.Log
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		// always has at least one log entry
		s.log = append(s.log, Entry{})
	}
}

func (s *Server) resetElectionTimeout() {
	// TODO
}

func (s *Server) heartbeat() {
	// TODO
}

func (s *Server) advanceCommitIndex() {
	// TODO
}

func (s *Server) timeout() {
	// TODO
}

func (s *Server) becomeLeader() {
	// TODO
}

func (s *Server) setVotedFor(votedFor uint64) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = votedFor
			return
		}
	}
	Server_assert(s, "Invalid cluster", true, false)
}

func (s *Server) Start() {
	s.mu.Lock()
	s.state = followerState
	s.done = false
	s.mu.Unlock()

	s.restore()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	s.server = &http.Server{Handler: mux}
	go s.server.Serve(l)

	go func() {
		s.mu.Lock()
		s.resetElectionTimeout()
		s.mu.Unlock()

		for {
			s.mu.Lock()
			if s.done {
				s.mu.Unlock()
				return
			}
			state := s.state
			s.mu.Unlock()
			switch state {
			case leaderState:
				s.heartbeat()
				s.advanceCommitIndex()
			case followerState:
				s.timeout()
				s.advanceCommitIndex()
			case candidateState:
				s.timeout()
				s.becomeLeader()
			}
		}
	}()
}
