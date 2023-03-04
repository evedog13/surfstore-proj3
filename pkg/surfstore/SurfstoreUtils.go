package surfstore

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	reflect "reflect"
	"strings"
)

// Implement the logic for a client syncing with the server here.

// 要check本地有没有修改 有可能本地修改了 但是还是localindex还是上次和server端sync的index
// 如果本地修改了 先更新本地的localindex 再sync更新server端的index
func ClientSync(client RPCClient) {
	// 1.The client should first scan the base directory, and for each file, compute that file’s hash list.
	files, err := ioutil.ReadDir(client.BaseDir) // read file error
	if err != nil {
		fmt.Println("Error when reading basedir: %v", err)
	}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir) // the local index we need to update according to files
	if err != nil {
		fmt.Println("Could not load meta from meta file: %v", err)
	}

	// 2.then consult the local index file and compare the results, to see whether (1) there are now new files in the base directory that aren’t in the index file,
	// or (2) files that are in the index file, but have changed since the last time the client was executed (i.e., the hash list is different).
	hashMap := make(map[string][]string)
	for _, file := range files {
		// check filename
		if file.Name() == "index.db" || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}

		// get blocks numbers
		var numsBlocks int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			fmt.Println("Error reading file in basedir: ", err)
		}
		defer fileToRead.Close()
		var hashlist []string
		byteSlice := make([]byte, client.BlockSize)
		for i := 0; i < numsBlocks; i++ {
			// byteSlice := make([]byte, client.BlockSize) // build a byteSlice to contain []bytes in the block
			len, err := fileToRead.Read(byteSlice) // the lenth of each block, the last block may be less than client.BlockSize
			if err != nil {
				fmt.Println("Error reading bytes from file in basedir: ", err)
			}
			// byteSlice = byteSlice[:len]           // read []byte from each block
			hash := GetBlockHashString(byteSlice[:len]) // compute each block's hash, and make intergration, which is the file's hash
			hashlist = append(hashlist, hash)           // arrcoding to the same file name to append the hash
		}
		hashMap[file.Name()] = hashlist
	}

	// check in the base directory，local side file VS local database ==> sync
	for fileName, hashList := range hashMap {
		if localIndex[fileName] == nil { // check new file, then update it
			localIndex[fileName] = &FileMetaData{Filename: fileName, Version: int32(1), BlockHashList: hashList}
		} else if !reflect.DeepEqual(localIndex[fileName].BlockHashList, hashList) { // check changed file
			localIndex[fileName].BlockHashList = hashList
			localIndex[fileName].Version = localIndex[fileName].Version + 1
			// fmt.Println("file is changed")
		}
		// if reflect.DeepEqual(localIndex[fileName].BlockHashList, []string{"0"}) { // check deleted file
		// 	continue
		// 	// fmt.Println("file is empty")
		// }
	}

	// check in the deleted files: file name in the localIndex and but not in the base directory
	for fileName, fileMetaData := range localIndex {
		if _, ok := hashMap[fileName]; !ok { // update the feature of the deleted file
			if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" {
				fileMetaData.Version++
				fileMetaData.BlockHashList = []string{"0"}
			}
		}
	}

	// 3.client should connect to the server and download an updated FileInfoMap. let’s call this the “remote index.”
	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		fmt.Println("Could not get blockStoreAddr: ", err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		fmt.Println("Could not get remote index: ", err)
	}
	//fmt.Println(remoteIndex)

	// 4. compare the local index with the remote index
	// 4.1. remote index refers to a file not present in the local index,
	// the client should download the blocks associated with that file, reconstitute that file in the base directory,
	// and then add the updated FileInfo information to the local index.
	// 4.2 there are new files in the local base directory that aren’t in the local index or in the remote index.
	// The client should upload the blocks corresponding to this file to the server,
	// then update the server with the new FileInfo.
	for fileName, localMetaData := range localIndex { // check local side, 有几个block就会有几个filename
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(hashMap[fileName], &m) //return了一个blockStoreAdrr的map
		if err != nil {
			fmt.Println("Could not get blockStoreAddr: ", err)
		}
		if remoteMetaData, ok := remoteIndex[fileName]; ok {
			if localMetaData.Version > remoteMetaData.Version {
				uploadFile(client, localMetaData, blockStoreAddrs, m)
			}
		} else {
			uploadFile(client, localMetaData, blockStoreAddrs, m)
		}
	}
	for fileName, remoteMetaData := range remoteIndex { // check server side
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(hashMap[fileName], &m) //return了一个blockStoreAdrr的map
		if err != nil {
			fmt.Println("Could not get blockStoreAddr: ", err)
		}
		if localMetaData, ok := localIndex[fileName]; !ok { // remote index refers to a file not present in the local index
			localIndex[fileName] = &FileMetaData{}
			downloadFile(client, localIndex[fileName], remoteMetaData, m) // download the remotefile and update
		} else {
			if remoteMetaData.Version >= localIndex[fileName].Version { // only if in this situation we need to update remote side to local side
				downloadFile(client, localMetaData, remoteMetaData, m)
			}
		}
	}

	// for fileName, localMetaData := range localIndex { // check local side
	// 	if remoteMetaData, ok := remoteIndex[fileName]; ok {
	// 		if localMetaData.Version > remoteMetaData.Version {
	// 			uploadFile(client, localMetaData, blockStoreAddr)
	// 		}
	// 	} else {
	// 		uploadFile(client, localMetaData, blockStoreAddr)
	// 	}
	// }

	WriteMetaFile(localIndex, client.BaseDir)
}

func uploadFile(client RPCClient, metaData *FileMetaData, blockStoreAddrs []string, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + metaData.Filename // local file path

	// special cheeck: for deleted file
	var latestVersion int32
	if _, err := os.Stat(path); os.IsNotExist(err) {
		e := client.UpdateFile(metaData, &latestVersion)
		if e != nil {
			fmt.Println("Could not update file: %v", err)
		} else {
			metaData.Version = latestVersion
		}
		return err
	}

	file, err := os.Open(path)

	if err != nil {
		fmt.Println("Error opening file: ", err)
	}
	defer file.Close()

	fileStat, err := os.Stat(path)
	if err != nil {
		fmt.Println("Error geting fileInfo: ", err)
	}

	var numsBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numsBlocks; i++ {
		byteSlice := make([]byte, client.BlockSize) // build a byteSlice to contain []bytes in the block
		len, err := file.Read(byteSlice)            // the lenth of each block, the last block may be less than client.BlockSize
		if err != nil {
			fmt.Println("Error reading bytes from file in basedir: ", err)
		}
		byteSlice = byteSlice[:len] // read []byte from each block

		block := Block{BlockData: byteSlice, BlockSize: int32(len)}
		hash := GetBlockHashString(block.BlockData)

		var responsibleSever string
		for blockStoreAddr, blockHashes := range blockStoreMap {
			for _, blockHash := range blockHashes {
				if hash == blockHash {
					responsibleSever = blockStoreAddr
				}
			}
		}

		var succ bool
		if err := client.PutBlock(&block, strings.ReplaceAll(responsibleSever, "blockstore", ""), &succ); err != nil {
			fmt.Println("Could not put block: %v", err)
		}
	}

	if err := client.UpdateFile(metaData, &latestVersion); err != nil {
		fmt.Println("Could not update file: %v", err)
	}

	metaData.Version = latestVersion
	return nil
}

func downloadFile(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + remoteMetaData.Filename // local file path

	file, err := os.Create(path) // create new file regardless of whether it exists
	if err != nil {
		fmt.Println("Error creating file: ", err)
	}

	defer file.Close()

	_, err = os.Stat(path)
	if err != nil {
		fmt.Println("Error geting fileInfo: ", err)
	}

	// check deleted file
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		fmt.Println("deleted file")
		if err := os.Remove(path); err != nil {
			fmt.Println("Could not remove file: %v", err)
		}
		*localMetaData = *remoteMetaData
		return nil
	}

	m := make(map[string][]string)
	err = client.GetBlockStoreMap(remoteMetaData.BlockHashList, &m) //return了一个blockStoreAdrr的map
	if err != nil {
		fmt.Println("Could not get blockStoreAddr: ", err)
	}

	blockData := ""
	for _, hash := range remoteMetaData.BlockHashList { // remote端的file的hash
		for blockStoreAddr, blockHashes := range m {
			for _, blockHash := range blockHashes {

				if hash == blockHash {
					fmt.Println("hash==blockhash")
					var block Block
					if err := client.GetBlock(hash, strings.ReplaceAll(blockStoreAddr, "blockstore", ""), &block); err != nil {
						fmt.Println("Could not get block: %v", err)
					}
					blockData += string(block.BlockData)
				}
			}
		}
	}
	//fmt.Println(blockData)
	if _, err := file.WriteString(blockData); err != nil {
		fmt.Println("Could not write to file: %v", err)
	}

	*localMetaData = *remoteMetaData
	return nil
}
