package lunacdn

import "math/rand"
import "crypto/md5"
import "encoding/hex"
import "strconv"
import "time"

// peer protocol constants
const HEADER_CONSTANT = 229
const PROTO_NULL = 0
const PROTO_HELLO = 1
const PROTO_ANNOUNCE = 2
const PROTO_ANNOUNCE_CONTINUE = 3
const PROTO_DOWNLOAD = 4
const PROTO_DOWNLOAD_CANCEL = 5
const PROTO_UPLOAD = 6
const PROTO_UPLOAD_PART = 7
const PROTO_UPLOAD_FAIL = 8

// how frequently to attempt to connect to disconnected peers
const CONNECT_INTERVAL = 10 * time.Second

// how frequently to announce blocks
const ANNOUNCE_INTERVAL = 10 * time.Second

// how frequently to print peer stats
const PEER_STATS_INTERVAL = 60 * time.Second

// if using FileCache, how often to scan filesystem
const FILECACHE_SCAN_INTERVAL = 120 * time.Second

// default speed to assume from untested peer
const DEFAULT_PEER_SPEED = 10 * time.Second

// length of a block
const BLOCK_SIZE = 128 * 1024

// file transfer constants
const TRANSFER_PACKET_SIZE = 32 * 1024

// how many blocks to buffer for clients
const SERVE_BUFFER_BLOCKS = 3

// number of bytes in file hashes
const HASH_BYTES = 16

// how long to cache failed file downloads
const DOWNLOAD_ERROR_CACHE_TIME = 30 * time.Second

// maximum number of attempts to download block from peers
const DOWNLOAD_MAX_ATTEMPTS = 3

// minimum timeout for block download from peer
const DOWNLOAD_MIN_TIMEOUT = time.Second

// connection timeout
const CONNECT_TIMEOUT = 5 * time.Second

const HEXADECIMAL_CHARS = "0123456789abcdef"

func randSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func hash(b []byte) []byte {
	hashArray := md5.Sum(b)
	return hashArray[:]
}

func strToInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 0, 64)
	return n
}

func strToInt(s string) int {
	n, _ := strconv.ParseInt(s, 0, 32)
	return int(n)
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func extractStrings(b []byte) []string {
	str := make([]string, 0)
	lastStart := 0

	for pos, x := range b {
		if x == 0 {
			lastStart = pos + 1
			str = append(str, string(b[lastStart : pos]))
		}
	}

	// any trailing bytes discarded, no null terminator!
	return str
}

func hexHash(str string) string {
	hashArray := md5.Sum([]byte(str))
	return hex.EncodeToString(hashArray[:])
}
