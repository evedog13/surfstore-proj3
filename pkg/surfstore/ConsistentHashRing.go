package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// blockHash := c.Hash(blockId) //?

	// sorted by hash
	hashes := []string{}
	for hash := range c.ServerMap {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)

	responsibleSever := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleSever = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleSever == "" { // if no responsible server, which means it's in the tail, return the first one
		responsibleSever = c.ServerMap[hashes[0]]
	}

	return responsibleSever
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string), // hash : servername
	}

	for _, addr := range serverAddrs {
		c.ServerMap[c.Hash("blockstore"+addr)] = addr
	}

	return c
}
