package goraft

import (
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
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
	rpcClient *rpc.Client
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

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage

	// candidate requesting vote
	CandidateId uint64

	// index of candidates last log entry
	LastLogIndex uint64

	// term of last log
	LastLogTerm uint64
}

type RequestVoteResponse struct {
	RPCMessage

	VoteGranted bool
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

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	hasTimedOut := time.Now().After(s.electionTimeout)
	if hasTimedOut {
		s.debug("Timed out, starting new election.")
		s.state = candidateState
		s.currentTerm++
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
			} else {
				s.cluster[i].votedFor = 0
			}
		}

		s.resetElectionTimeout()
		s.persist()
		s.requestVote()
	}
}

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.debugf("New interval: %s.", interval*time.Millisecond)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			s.debugf("requesting vote from %d", s.cluster[i].Id)

			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[len(s.log)-1].Term

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp)

			if !ok {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term
			if dropStaleResponse {
				return
			}

			if rsp.VoteGranted {
				s.debugf("Vote granted by %d", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	transition := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		transition = true
		s.debug("transitioned to follow")
		s.resetElectionTimeout()
		s.persist()
	}
	return transition
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	s.debugf("Recieved vote request from %d", req.CandidateId)

	rsp.VoteGranted = false
	rsp.Term
}

func (s *Server) heartbeat() {
	// TODO
}

func (s *Server) advanceCommitIndex() {
	// TODO
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	if quorum == 0 {
		for i := range s.cluster {
			s.cluster[i].nextIndex = uint64(len(s.log) + 1)
			s.cluster[i].matchIndex = 0
		}
	}

	s.debug("New leader has been appointed and its you")
	s.state = leaderState

	s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
	// what is command here
	s.persist()

	s.heartbeatTimeout = time.Now()
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

func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()

	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}

	return err == nil
}
