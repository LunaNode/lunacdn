package lunacdn

/*
cache.go: Cache interface.
*/

type CacheFile interface {
	GetPathHash() string
	GetLength() int64
	GetNumBlocks() uint16
	GetBlockSize() uint32
}

type prepareAnnounceCallback func([]AnnounceFile)

type Cache interface {
	NotifyFile(hash string, length int64, numBlocks uint16, blockSize uint32)
	DownloadInit(path string) (CacheFile, error)
	DownloadInitHash(pathHash string) (CacheFile, error)
	DownloadRead(file CacheFile, index int, tryPeers bool) []byte
	PrepareAnnounce(callback prepareAnnounceCallback)
}
