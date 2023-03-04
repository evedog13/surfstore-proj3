package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

/*
server side:
blockstore stores content in a key-value format, the key is the block hash, and the value is the block itself.
This is the API provied by the server side, for example, this can be provided by Amazon, and we don't know the implemenation yet,
we can just use this API and get result.
*/

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

// put block to the server, which will be used in the download process when the client wants to download files from the server side
// Stores block b in the key-value store, indexed by hash value h
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) { // * means the pointer and creat new object, return the reference
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, fmt.Errorf("GetBlock wrong")
	} else {
		return &Block{BlockData: block.GetBlockData(), BlockSize: block.GetBlockSize()}, nil // & means we get the reference, and write something on the reference
	}
}

// get block from the server, which will be used in the upload process when the client wants to upload files to the server side
// Retrieves a block indexed by hash value h
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given an input hashlist,
// returns an output hashlist containing the subset of hashlist_in that are stored in the key-value store
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

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := []string{}
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
