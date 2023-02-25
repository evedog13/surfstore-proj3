// package surfstore

// import (
// 	context "context"
// 	"fmt"
// )

// type BlockStore struct {
// 	BlockMap map[string]*Block
// 	UnimplementedBlockStoreServer
// }

// func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
// 	block, ok := bs.BlockMap[blockHash.Hash] // retrieve the block from the map
// 	if !ok {
// 		return nil, fmt.Errorf("no block found")
// 	}
// 	return block, nil
// }

// func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
// 	hash := GetBlockHashString(block.BlockData)
// 	bs.BlockMap[hash] = block // store block in the map according to hash
// 	return &Success{Flag: true}, nil
// }

// // Given a list of hashes “in”, returns a list containing the
// // subset of in that are stored in the key-value store
// func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
// 	var hash []string
// 	for _, hashIn := range blockHashesIn.Hashes { // blockHashesIn is the list we want to check: e.g 1 2 3
// 		if _, ok := bs.BlockMap[hashIn]; ok { // bs.BlockMap is the map we now have: e.g 1 2
// 			hash = append(hash, hashIn) // if bs.BlockMap does have the hash in blockHashesIn, append
// 		}
// 	}
// 	return &BlockHashes{Hashes: hash}, nil // return the hash we have in blockHashesIn
// }

// // This line guarantees all method for BlockStore are implemented
// var _ BlockStoreInterface = new(BlockStore)

//	func NewBlockStore() *BlockStore {
//		return &BlockStore{
//			BlockMap: map[string]*Block{},
//		}
//	}
package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, fmt.Errorf("GetBlock wrong")
	} else {
		return &Block{BlockData: block.GetBlockData(), BlockSize: block.GetBlockSize()}, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := blockHashesIn.Hashes
	subHashes := []string{}
	for _, hash := range hashes {
		block, _ := bs.GetBlock(ctx, &BlockHash{Hash: hash})
		if block != nil {
			subHashes = append(subHashes, hash)
		}
	}
	return &BlockHashes{Hashes: subHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
