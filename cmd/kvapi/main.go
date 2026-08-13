package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type statemachine struct {
	db     *sync.Map
	server int
}

type commandKind uint8

type command struct {
	kind  commandKind
	key   string
	value string
}

const (
	setCommand commandKind = iota
	getCommand
)

func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)

	switch c.kind {
	case setCommand:
		s.db.Store(c.key, c.value)
	case getCommand:
		value, ok := s.db.Load(c.key)
		if !ok {
			return nil, fmt.Errorf("Key not found")
		}
		return []byte(value.(string)), nil
	default:
		return nil, fmt.Errorf("Unknown command: %x", cmd)
	}

	return nil, nil
}

func encodeCommand(c command) []byte {
	msg := bytes.NewBuffer(nil)
	err := msg.WriteByte(uint8(c.kind))
	if err != nil {
		panic(err)
	}

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.key)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.key)

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.value)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.value)

	return msg.Bytes()
}

func decodeCommand(msg []byte) command {
	var c command
	c.kind = commandKind(msg[0])

	keylen := binary.LittleEndian.Uint64(msg[1:9])
	c.key = string(msg[9 : 9+keylen])

	if c.kind == setCommand {
		valLen := binary.LittleEndian.Uint64(msg[9+keylen : 9+keylen+8])
		c.value = string(msg[9+keylen+8 : 9+keylen+8+valLen])
	}

	return c
}

type httpServer struct {
	raft *goraft.Server
	db   *sync.Map
}

func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = setCommand
	c.key = r.URL.Query().Get("key")
	c.value = r.URL.Query().Get("value")

	_, err := hs.raft.Apply([][]byte{encodeCommand(c)})
	if err != nil {
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
}

// getting values from the raft server is where the interesting stuff happens
// we already embed a local in memory map with which we are working, we could just read from that
// map in the current process BUT it might not be up to date (when working on a
// distributed cluster setup) thus we will pass it through the replication logs

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = getCommand
	c.key = r.URL.Query().Get("key")

	var value []byte
	var err error
	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.db.Load(c.key)
		if !ok {
			err = fmt.Errorf("key not found")
		} else {
			value = []byte(v.(string))
		}
	} else {
		var results []goraft.ApplyResult
		results, err := hs.raft.Apply([][]byte{encodeCommand(c)})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("Expected single response from Raft, got: %d.", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}
		}
	}

	if err != nil {
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	written := 0

	for written < len(value) {
		n, err := w.Write(value[written:])
		if err != nil {
			log.Printf("Could not encode key-value in http response: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		written += n
	}
}
