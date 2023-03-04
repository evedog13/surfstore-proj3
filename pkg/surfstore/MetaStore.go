package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

/*
server side:
metastore stores the info of each file in a key-value format, the key is the file name, and the value is the file version, file hashlist and so on.
This is the API provied by the server side, for example, this can be provided by Amazon, and we don't know the implemenation yet,
we can just use this API and get result.
*/

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

// Returns a mapping of the files stored in the SurfStore cloud service,
// including the version, filename, and hashlist.
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

// Updates the FileInfo values associated with a file stored in the cloud.
// 1）This method replaces the hash list for the file with the provided hash list
// only if the new version number is exactly one greater than the current version number.
// 2）Otherwise, you can send version=-1 to the client telling them that the version
// they are trying to store is not right (likely too old).
func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fmt.Println("start updatefile")
	// MetaStore is in the server side, we need to update it according to fileMetaData in the client side
	filename := fileMetaData.Filename         // need to check
	version := fileMetaData.Version           // need to check
	if _, ok := m.FileMetaMap[filename]; ok { // can find the file in the map
		if version-1 == m.FileMetaMap[filename].Version { // replace the hash list
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else { // cannot find the file in the map ==> create a new one
		m.FileMetaMap[filename] = fileMetaData
	}
	return &Version{Version: version}, nil
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)

	hashes := blockHashesIn.Hashes
	for _, hash := range hashes {
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(hash)
		if blockStoreMap[responsibleServer] == nil {
			blockStoreMap[responsibleServer] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap[responsibleServer].Hashes = append(blockStoreMap[responsibleServer].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

// func NewMetaStore(blockStoreAddr string) *MetaStore {
// 	return &MetaStore{
// 		FileMetaMap:    map[string]*FileMetaData{},
// 		BlockStoreAddr: blockStoreAddr,
// 	}
// }

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
