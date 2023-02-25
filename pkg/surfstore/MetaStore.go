// package surfstore

// import (
// 	context "context"
// 	"fmt"

// 	emptypb "google.golang.org/protobuf/types/known/emptypb"
// )

// type MetaStore struct {
// 	FileMetaMap    map[string]*FileMetaData
// 	BlockStoreAddr string
// 	UnimplementedMetaStoreServer
// }

// // Returns a mapping of the files stored in the SurfStore cloud service,
// // including the version, filename, and hashlist.
// func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
// 	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
// }

// // Updates the FileInfo values associated with a file stored in the cloud.
// // 1）This method replaces the hash list for the file with the provided hash list
// // only if the new version number is exactly one greater than the current version number.
// // 2）Otherwise, you can send version=-1 to the client telling them that the version
// // they are trying to store is not right (likely too old).
// //
// //	func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
// //		fmt.Println("start updatefile")
// //		// MetaStore is in the server side, we need to update it according to fileMetaData in the client side
// //		filename := fileMetaData.Filename         // need to check
// //		version := fileMetaData.Version           // need to check
// //		if _, ok := m.FileMetaMap[filename]; ok { // can find the file in the map
// //			fmt.Println(version)
// //			fmt.Println(m.FileMetaMap[filename].Version)
// //			if version-1 == m.FileMetaMap[filename].Version { // replace the hash list
// //				m.FileMetaMap[filename].Version = version
// //				fmt.Println(fileMetaData.BlockHashList)
// //				m.FileMetaMap[filename].BlockHashList = fileMetaData.BlockHashList
// //				fmt.Println(m.FileMetaMap[filename].BlockHashList)
// //			} else {
// //				version = -1
// //			}
// //		} else { // cannot find the file in the map ==> create a new one
// //			m.FileMetaMap[filename] = &FileMetaData{
// //				Filename:      filename,
// //				Version:       version,
// //				BlockHashList: fileMetaData.BlockHashList,
// //			}
// //		}
// //		return &Version{Version: version}, nil
// //	}
// func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
// 	fmt.Println("enter updatefile")
// 	filename := fileMetaData.Filename
// 	version := fileMetaData.Version

// 	if _, ok := m.FileMetaMap[filename]; ok {
// 		if version == m.FileMetaMap[filename].Version+1 {
// 			m.FileMetaMap[filename] = fileMetaData
// 		} else {
// 			version = -1
// 		}
// 	} else {
// 		m.FileMetaMap[filename] = fileMetaData
// 	}
// 	fmt.Println("leave updatefile")
// 	return &Version{Version: version}, nil
// }

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

// // This line guarantees all method for MetaStore are implemented
// var _ MetaStoreInterface = new(MetaStore)

//	func NewMetaStore(blockStoreAddr string) *MetaStore {
//		return &MetaStore{
//			FileMetaMap:    map[string]*FileMetaData{},
//			BlockStoreAddr: blockStoreAddr,
//		}
//	}
package surfstore

import (
	context "context"
	"fmt"

	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mtx            sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fmt.Println("enter updatefile")
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	if _, ok := m.FileMetaMap[filename]; ok {
		if version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}
	fmt.Println("leave updatefile")
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
