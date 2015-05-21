package lunacdn

import "net"
import "sync"
import "time"
import "strings"
import "encoding/binary"
import "math/rand"
import "bytes"

type PeerDownload struct {
	Id int64
	NotifyChannel chan bool
	Bytes []byte
	Length int64
	StartTime time.Time
}

type PeerBlock struct {
	FileHash string
	Index int
}

type Peer struct {
	mu sync.Mutex // used for updating fields and sending data; receiving data is done outside the lock
	conn *net.TCPConn
	nextConnectTime time.Time
	cache *Cache

	// set of blocks this peer advertises
	availableBlocks map[PeerBlock]bool

	// indicator for how quickly we download a block from this peer
	// recomputed as rollingSpeed = 0.3 * speed + 0.7 * rollingSpeed
	rollingSpeed int64

	// pending downloads from random download id => Download struct
	pendingDownloads map[int64]*PeerDownload
}

type PeerList struct {
	mu sync.Mutex
	peers map[string]*Peer
	listener *net.TCPListener
	cache *Cache
}

func MakePeerList(cfg *Config) *PeerList {
	this := new(PeerList)

	// construct map of peers from config
	this.peers = make(map[string]*Peer)
	for _, peerAddr := range strings.Split(cfg.BackendList, ",") {
		peerAddr = strings.TrimSpace(peerAddr)
		if len(peerAddr) > 0 {
			this.peers[peerAddr] = &Peer{
				availableBlocks: make(map[PeerBlock]bool),
				rollingSpeed: -1,
			}
		}
	}

	Log.Info.Printf("Loaded %d peers", len(this.peers))

	// initialize server socket
	Log.Info.Printf("Listening for backend connections on [%s]", cfg.BackendBind)
	addr, err := net.ResolveTCPAddr("tcp", cfg.BackendBind)
	if err != nil {
		panic(err)
	}

	this.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	// listen for connections
	go func() {
		for {
			conn, err := this.listener.AcceptTCP()
			if err != nil {
				Log.Warn.Printf("Error while accepting connection: %s", err.Error())
				continue
			}
			Log.Info.Printf("New connection from %s", conn.RemoteAddr().String())
			this.handleConnection(conn)

		}
	}()

	return this
}

func (this *PeerList) SetCache(cache *Cache) {
	this.cache = cache
}

func (this *PeerList) handleConnection(conn *net.TCPConn) {
	defer conn.Close()
	peer := this.authorizeConnection(conn)
	if peer == nil {
		return
	}

	addr := conn.RemoteAddr().String()
	buf := make([]byte, 65536)
	bufPos := 0

	for {
		count, err := conn.Read(buf[bufPos:])
		if err != nil {
			Log.Info.Printf("Disconnected from %s: %s", addr, err.Error())
			break
		}

		bufPos += count

		// try to process a packet
		if bufPos < 4 {
			continue
		}

		if buf[0] != HEADER_CONSTANT {
			Log.Warn.Printf("Invalid header constant from %s, terminating connection", addr)
			break
		}

		header := buf[1]
		packetLen := binary.BigEndian.Uint16(buf[2:4])
		if bufPos < int(packetLen) {
			continue
		}
		packet := bytes.NewBuffer(buf[4:packetLen])

		// TODO: we can push processing to another thread probably?
		if header == PROTO_ANNOUNCE {
			this.handleAnnounce(peer, protocolReadAnnounce(packet), true)
		} else if header == PROTO_ANNOUNCE_CONTINUE {
			this.handleAnnounce(peer, protocolReadAnnounce(packet), false)
		} else if header == PROTO_UPLOAD {
			downloadId, downloadLen := protocolReadUpload(packet)
			this.handleUpload(peer, downloadId, downloadLen)
		} else if header == PROTO_UPLOAD_PART {
			downloadId, part := protocolReadUploadPart(packet)
			this.handleUploadPart(peer, downloadId, part)
		} else if header == PROTO_DOWNLOAD {
			downloadId, fileHash, blockIndex := protocolReadDownload(packet)
			this.handleDownload(peer, downloadId, fileHash, blockIndex)
		}
	}
}

func (this *PeerList) handleAnnounce(peer *Peer, files []AnnounceFile, restart bool) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if restart {
		peer.availableBlocks = make(map[PeerBlock]bool)
	}

	for _, file := range files {
		go this.cache.NotifyFile(file.Hash, file.Length)

		for _, idx := range file.Indexes {
			peerBlock := PeerBlock{FileHash: file.Hash, Index: idx}
			peer.availableBlocks[peerBlock] = true
		}
	}
}

func (this *PeerList) handleUpload(peer *Peer, downloadId int64, downloadLen int64) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.conn == nil {
		return
	}

	download, ok := peer.pendingDownloads[downloadId]
	if !ok {
		peer.conn.Write(protocolSendDownloadCancel(downloadId).Bytes())
		return
	}

	if download.Length == -1 {
		download.Length = downloadLen
		download.Bytes = make([]byte, 0, download.Length)
	}
}

func (this *PeerList) handleUploadPart(peer *Peer, downloadId int64, part []byte) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.conn == nil {
		return
	}

	download, ok := peer.pendingDownloads[downloadId]
	if !ok {
		peer.conn.Write(protocolSendDownloadCancel(downloadId).Bytes())
		return
	}

	download.Bytes = append(download.Bytes, part...)

	// check if download has completed
	if len(download.Bytes) >= int(download.Length) {
		download.Bytes = download.Bytes[:download.Length]
		download.NotifyChannel <- true
		delete(peer.pendingDownloads, downloadId)
	}
}

func (this *PeerList) handleDownload(peer *Peer, downloadId int64, fileHash string, blockIndex int) {
	cacheFile := this.cache.DownloadInit(fileHash)
	if cacheFile == nil {
		return
	}

	bytes := this.cache.DownloadRead(cacheFile, blockIndex, false)
	if bytes == nil {
		return
	}

	go func() {
		peer.mu.Lock()
		peer.conn.Write(protocolSendUpload(downloadId, int64(len(bytes))).Bytes())
		peer.mu.Unlock()

		for i := 0; i < len(bytes); i += TRANSFER_PACKET_SIZE {
			peer.mu.Lock()
			peer.conn.Write(protocolSendUploadPart(downloadId, bytes[i:i+4096]).Bytes())
			peer.mu.Unlock()
		}
	}()
}

func (this *PeerList) startDownload(peer *Peer, peerBlock PeerBlock) *PeerDownload {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.conn != nil {
		downloadId := rand.Int63()

		// send download packet
		_, err := peer.conn.Write(protocolSendDownload(downloadId, peerBlock.FileHash, peerBlock.Index).Bytes())
		if err != nil {
			peer.rollingSpeed *= 2
			return nil
		}

		// create peer download structure
		download := &PeerDownload{
			Id: downloadId,
			NotifyChannel: make(chan bool, 1), // buffer necessary since caller may timeout
			Bytes: nil,
			Length: -1,
			StartTime: time.Now(),
		}
		peer.pendingDownloads[download.Id] = download
		return download
	} else {
		peer.rollingSpeed *= 2
		return nil
	}
}

func (this *PeerList) cancelDownload(peer *Peer, downloadId int64) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	delete(peer.pendingDownloads, downloadId)
	peer.conn.Write(protocolSendDownloadCancel(downloadId).Bytes())
}

func (this *PeerList) DownloadBlock(fileHash string, blockIndex int, timeout time.Duration) []byte {
	peerBlock := PeerBlock{FileHash: fileHash, Index: blockIndex}
	startTime := time.Now()

	for time.Now().Before(startTime.Add(timeout)) {
		peer := this.findPeerWithBlock(peerBlock)
		download := this.startDownload(peer, peerBlock)
		if download == nil {
			continue
		}

		select {
			case <- download.NotifyChannel:
				return download.Bytes
			case <- time.After(timeout - time.Now().Sub(startTime)):
				this.cancelDownload(peer, download.Id)
				return nil
		}
	}

	return nil
}

func (this *PeerList) findPeerWithBlock(peerBlock PeerBlock) *Peer {
	// we first identify the set of peers that have the block available
	//  (both connected and announced block recently)
	// out of those, we select each one with probability proportional to 1/rollingSpeed
	this.mu.Lock()
	defer this.mu.Unlock()

	peerWeights := make(map[*Peer]int64)
	var totalWeight int64
	for _, peer := range this.peers {
		peer.mu.Lock()
		_, ok := peer.availableBlocks[peerBlock]
		if ok && peer.conn != nil {
			if peer.rollingSpeed == 0 {
				peerWeights[peer] = DEFAULT_PEER_SPEED
			} else {
				peerWeights[peer] = DEFAULT_PEER_SPEED * 100000 / peer.rollingSpeed
			}
			totalWeight += peerWeights[peer]
		}
		peer.mu.Unlock()
	}

	if totalWeight == 0 {
		return nil
	}

	// generate random number in total weight, and iterate through peers subtracting the weight
	r := rand.Int63n(totalWeight)
	for peer, weight := range peerWeights {
		r -= weight
		if r <= 0 {
			return peer
		}
	}

	Log.Error.Printf("Failed to select random peer, random number out of range?!")
	return nil
}

func (this *PeerList) authorizeConnection(conn *net.TCPConn) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()

	addr := conn.RemoteAddr().String()
	peer, ok := this.peers[addr]
	if !ok {
		Log.Info.Printf("Rejecting connection from unauthorized peer (%s)", addr)
		return nil
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.conn != nil {
		Log.Info.Printf("Rejecting duplicate connection with %s", addr)
		return nil
	}

	peer.conn = conn
	return peer
}
