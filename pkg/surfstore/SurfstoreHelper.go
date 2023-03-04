package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `CREATE table IF NOT EXISTS indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// 我们没有办法获取remote端的database
// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			fmt.Println("1.Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		fmt.Println("2.Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println(err)
	}
	statement.Exec()

	for _, fileMeta := range fileMetas { // for every single metadta file
		statement, err := db.Prepare(insertTuple) // placeholders
		if err != nil {
			fmt.Println("4.Error During Meta Write Back")
		}

		for idx, hash := range fileMeta.BlockHashList { // for every block hash
			statement.Exec(fileMeta.Filename, fileMeta.Version, idx, hash)
		}
	}
	db.Close()
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes;`

const getTuplesByFileName string = `SELECT version, hashValue FROM indexes WHERE fileName = ? order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	// metaFile is the database, we can use this function to get the meta map
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil {
		_, e := os.Create(metaFilePath)
		if e != nil {
			fmt.Println("Error During Meta Read Back")
		}
		return fileMetaMap, e
	}
	if metaFileStats.IsDir() {
		return fileMetaMap, fmt.Errorf("%s is a directory", metaFilePath)
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		fmt.Println("Error When Opening Meta")
	}
	defer db.Close()

	fileNames, err := db.Query(getDistinctFileName)
	if err != nil {
		fmt.Println("Error When Getting Distinct File Names")
		return fileMetaMap, err
	}

	for fileNames.Next() { // if hasNext
		var filename string
		fileNames.Scan(&filename) // scan出来确切的数据 具体这个filename叫什么
		tuples, err := db.Query(getTuplesByFileName, filename)
		if err != nil {
			return nil, err
		}

		var version int32
		var BlockHashList []string
		for tuples.Next() { // 很多个相同的filename对应的一个tuples是一个matrix，很多条数据，但是filename都是一样的
			var hashValue string // 放在func外面可能就一直是相同的hashValue
			err = tuples.Scan(&version, &hashValue)
			if err != nil {
				return nil, err
			}
			BlockHashList = append(BlockHashList, hashValue) // 同一个filename可能有好几个block，要把所有block的hashValue拼一块
		}
		fileMetaMap[filename] = &FileMetaData{Filename: filename, Version: version, BlockHashList: BlockHashList}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
