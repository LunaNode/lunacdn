package lunacdn

/*
peer.go: handles backend communication

Initially, we only have the set of backends listed in configuration.
Based on that, we create two maps:
 - set of authorized IP addresses
 - set of peers that we should discover

We want to maintain a single connection with each peer. This is complicated by the fact that
 peers are also trying to make connections with us. To handle this, we have an additional map
 of peer instances identified by a session-unique peer ID; these instances include a connection
 pointer which is updated atomically.

Every now and then, we connect to the peers that we should discover; meanwhile we also accept
 remote connections. Upon a successful incoming or outgoing connection, we perform a HELLO
 exchange, where we send our peer ID and look for the remote peer ID. If these match, then we
 detect loop connection and terminate. Otherwise, if we haven't seen the peer ID before, we
 register the new peer, or if we have seen it before, we make sure we're the only connection.

If the connection disconnects, then we keep the peer instance registered but set the connection
 pointer to nil. We will try to reconnect every now and then.

A problem arises if we receive an incoming connection from a new peer ID: we do not know the
 address/port that the remote peer is listening on. In this case, we terminate the connection
 under the assumption that we will eventually make an outgoing connection to the peer. If that
 assumption holds, then this is fine since eventually both ends will have initiated connections
 to each other and thus associated the other's peer ID with the address/port from backend list.
*/

import "net"
import "sync"
import "time"
import "strings"
import "encoding/binary"
import "math"
import "math/rand"
import "bytes"
import "io"
import "github.com/oschwald/geoip2-golang"

/*
Synchronization strategy
- Lock this.mu before peer.mu; peer.mu locked to edit peer.conn and other fields
- Never call cache when we have the lock (since cache will call us synchronously)
- We maintain at most one handleConnection call per peer by synchronizing on peer.conn
*/

/*
 * A pending download, we are trying to retrieve a block from the remote end
 * Id: identifier for this download in exchanged packets (multiple downloads may be happening concurrently)
 * NotifyChannel: where to send bool once the download completes
 * Bytes: retrieved bytes so far
 * Length: total expected length
 * StartTime: time.Now() when download initialized
 */
type PeerDownload struct {
	Id int64
	NotifyChannel chan bool
	Bytes []byte
	Length int64
	StartTime time.Time
}

/*
 * Identifies a block by file and block index.
 */
type PeerBlock struct {
	FileHash string
	Index int
}

/*
 * A connection with remote peer.
 * Peer ID is a unique session identifier for the peer. We exchange peer ID's during
 *  HELLO protocol. Peers are registered whenever a new peer ID is seen.
 */
type Peer struct {
	addr string // "address:port" string
	mu sync.Mutex // used for updating fields and sending data; receiving data is done outside the lock
	conn *net.TCPConn
	lastConnectTime time.Time
	lastAnnounceTime time.Time
	peerId int64
	location string
	connecting bool // whether currently connecting

	// set of blocks this peer advertises
	availableBlocks map[PeerBlock]bool

	// indicator for how quickly we download a block from this peer
	// recomputed as rollingSpeed = 0.3 * speed + 0.7 * rollingSpeed
	rollingSpeed time.Duration

	// pending downloads from random download id => Download struct
	pendingDownloads map[int64]*PeerDownload
}

func MakePeer(addr string, peerId int64) *Peer {
	this := new(Peer)
	this.addr = addr
	this.peerId = peerId
	this.availableBlocks = make(map[PeerBlock]bool)
	this.pendingDownloads = make(map[int64]*PeerDownload)

	this.lastConnectTime = time.Now()
	this.rollingSpeed = DEFAULT_PEER_SPEED
	return this
}

type PeerList struct {
	mu sync.Mutex
	peers map[int64]*Peer
	authorizedIPs map[string]bool
	discoverable map[string]time.Time
	listener *net.TCPListener
	cache Cache

	// temporary random identifier for this peer
	peerId int64

	// redirect-related fields
	myLocation string
	myIP net.IP
	geoip *geoip2.Reader
}

func MakePeerList(cfg *Config, exitChannel chan bool) *PeerList {
	this := new(PeerList)
	this.peerId = rand.Int63()

	// construct map of peers from config
	this.peers = make(map[int64]*Peer)
	this.authorizedIPs = make(map[string]bool)
	this.discoverable = make(map[string]time.Time)
	defaultPortStr := strings.Split(cfg.BackendBind, ":")[1]

	for _, peerAddr := range strings.Split(cfg.BackendList, ",") {
		peerAddr = strings.TrimSpace(peerAddr)
		if len(peerAddr) > 0 {
			peerAddrParts := strings.Split(peerAddr, ":")
			this.authorizedIPs[peerAddrParts[0]] = true

			// add port if missing
			if len(peerAddrParts) == 1 {
				peerAddr += ":" + defaultPortStr
			}
			this.discoverable[peerAddr] = time.Now().Add(-1 * CONNECT_INTERVAL)
		}
	}

	Log.Info.Printf("Loaded %d authorized IPs", len(this.authorizedIPs))

	// set up redirect parameters
	this.myLocation = cfg.RedirectLocation
	if cfg.RedirectEnable {
		Log.Info.Printf("Loading geoip database")
		db, err := geoip2.Open(cfg.RedirectGeoipPath)
		if err != nil {
			Log.Error.Printf("Failed to load geoip data: %s; redirects are disabled", err.Error())
		} else {
			this.geoip = db
			this.myIP = net.ParseIP(cfg.RedirectIP)
		}
	}

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
			go this.handleConnection(conn, nil, "")
		}

		exitChannel <- true
	}()

	// connect to peers
	go func() {
		for {
			time.Sleep(time.Second)
			this.refreshPeers()
		}
	}()

	// print out peer speed statistics
	go func() {
		for {
			time.Sleep(PEER_STATS_INTERVAL)
			this.mu.Lock()
			for _, peer := range this.peers {
				peer.mu.Lock()
				Log.Info.Printf("Stats with %s: rollingSpeed=%.2f ms", peer.addr, float32(peer.rollingSpeed.Nanoseconds()) / 1000 / 1000)
				peer.mu.Unlock()
			}
			this.mu.Unlock()
		}
	}()

	return this
}

func (this *PeerList) SetCache(cache Cache) {
	this.cache = cache
}

/*
 * Connection handler.
 * conn: the connected connection, either received or finished connecting to remote end
 * peer: set if we are connecting since we disconnected from an existing peer
 * discoveredVia: set if we are connecting to a new discoverable
 */
func (this *PeerList) handleConnection(conn *net.TCPConn, peer *Peer, discoveredVia string) {
	defer conn.Close()
	addr := strings.Split(conn.RemoteAddr().String(), ":")[0] // get IP only for whitelisting
	_, ok := this.authorizedIPs[addr] // no synchronization issue since read-only table
	if !ok {
		Log.Warn.Printf("Rejecting unauthorized connection from %s", addr)
		return
	}

	conn.Write(protocolSendHello(this.peerId, this.myLocation).Bytes())
	conn.SetReadDeadline(time.Now().Add(CONNECT_TIMEOUT))
	helloSuccess, helloPeerId, helloPeerLocation := protocolReadHello(conn)
	if !helloSuccess {
		return
	}

	// we have peer ID now, so we will either create peer or the peer will be someone we don't want to connect to
	// this means we can remove from discoverable map
	if discoveredVia != "" {
		this.mu.Lock()
		delete(this.discoverable, discoveredVia)
		this.mu.Unlock()
	}

	// if peer ID matches our own, we connected to ourself...
	if helloPeerId == this.peerId {
		Log.Warn.Printf("Detected connection with self (addr=%s)", conn.RemoteAddr().String())
		return
	}

	// set up the peer struct
	if peer != nil {
		// this is successful outgoing connection to a peer that we already have entry for
		// this is easy if the peerId is same, but there is possibility peerId changed
		//  in that case we create a new peer entry
		if helloPeerId != peer.peerId {
			oldPeer := peer
			this.mu.Lock()
			delete(this.peers, oldPeer.peerId)

			// verify that new peer ID is not in the table (another connection might have already registered it)
			_, already := this.peers[helloPeerId]

			if !already {
				this.peers[helloPeerId] = MakePeer(oldPeer.addr, helloPeerId)
				peer = this.peers[helloPeerId]
			} else {
				this.mu.Unlock()
				return
			}
			this.mu.Unlock()
		}
	} else {
		// this is either incoming connection, or outgoing connection to new discovery
		// we can handle this UNLESS this is an incoming connection from a new peer
		//  (in which case we wouldn't have the peer address, so need to do hello exchange with outgoing connection first)
		this.mu.Lock()
		peer, ok = this.peers[helloPeerId]
		if !ok {
			if discoveredVia != "" {
				this.peers[helloPeerId] = MakePeer(discoveredVia, helloPeerId)
				peer = this.peers[helloPeerId]
			} else {
				Log.Warn.Printf("Connection with %s delayed to obtain full addr/port information", addr)
				this.mu.Unlock()
				return
			}
		}
		this.mu.Unlock()
	}

	peer.mu.Lock()
	if peer.conn != nil {
		Log.Info.Printf("Rejecting duplicate connection with %s", peer.addr)
		peer.mu.Unlock()
		return
	}
	peer.conn = conn
	peer.location = helloPeerLocation
	peer.mu.Unlock()
	Log.Info.Printf("Successful connection with %s", peer.addr)

	header := make([]byte, 4)
	buf := make([]byte, 65536)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * ANNOUNCE_INTERVAL)) // timeout to detect disconnects
		count, err := io.ReadFull(conn, header)
		if err != nil {
			Log.Info.Printf("Disconnected from %s: %s", peer.addr, err.Error())
			break
		} else if count != len(header) {
			Log.Info.Printf("Disconnected from %s: failed to read full packet header", peer.addr)
			break
		}

		if header[0] != HEADER_CONSTANT {
			Log.Warn.Printf("Invalid header constant from %s, terminating connection", peer.addr)
			break
		}

		packetType := header[1]
		packetLen := binary.BigEndian.Uint16(header[2:4])
		count, err = io.ReadFull(conn, buf[:packetLen-4])
		if err != nil {
			Log.Info.Printf("Disconnected from %s: %s", peer.addr, err.Error())
			break
		} else if uint16(count) != packetLen - 4 {
			Log.Info.Printf("Disconnected from %s: failed to read full packet body", peer.addr)
			break
		}

		packet := bytes.NewBuffer(buf[:packetLen-4])

		if packetType == PROTO_ANNOUNCE {
			this.handleAnnounce(peer, protocolReadAnnounce(packet), true)
		} else if packetType == PROTO_ANNOUNCE_CONTINUE {
			this.handleAnnounce(peer, protocolReadAnnounce(packet), false)
		} else if packetType == PROTO_UPLOAD {
			downloadId, downloadLen := protocolReadUpload(packet)
			this.handleUpload(peer, downloadId, downloadLen)
		} else if packetType == PROTO_UPLOAD_PART {
			downloadId, part := protocolReadUploadPart(packet)
			this.handleUploadPart(peer, downloadId, part)
		} else if packetType == PROTO_UPLOAD_FAIL {
			downloadId := protocolReadUploadFail(packet)
			this.handleUploadFail(peer, downloadId)
		} else if packetType == PROTO_DOWNLOAD {
			downloadId, fileHash, blockIndex := protocolReadDownload(packet)
			this.handleDownload(peer, downloadId, fileHash, blockIndex)
		} else if packetType == PROTO_DOWNLOAD_CANCEL {
			downloadId := protocolReadDownloadCancel(packet)
			this.handleDownloadCancel(peer, downloadId)
		} else {
			Log.Info.Printf("Disconnected from %s: unknown packet type %d", peer.addr, packetType)
			break
		}
	}

	peer.mu.Lock()
	if peer.conn == conn {
		peer.conn = nil
	}
	peer.mu.Unlock()
}

func (this *PeerList) handleAnnounce(peer *Peer, files []AnnounceFile, restart bool) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if restart {
		Log.Debug.Printf("[%s] Restarting peer's available blocks", peer.addr)
		peer.availableBlocks = make(map[PeerBlock]bool)
	}
	Log.Debug.Printf("[%s] Receiving %d available files in announcement", peer.addr, len(files))

	for _, file := range files {
		go this.cache.NotifyFile(file.Hash, file.Length, file.NumBlocks, file.BlockSize)

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
	Log.Debug.Printf("Begin download from %s for %d (%d bytes)", peer.addr, downloadId, downloadLen)

	download, ok := peer.pendingDownloads[downloadId]
	if !ok {
		Log.Debug.Printf("Upload from %s references unknown download %d, cancelling", peer.addr, downloadId)
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
		Log.Debug.Printf("Upload part from %s references unknown download %d, cancelling", peer.addr, downloadId)
		peer.conn.Write(protocolSendDownloadCancel(downloadId).Bytes())
		return
	}

	download.Bytes = append(download.Bytes, part...)

	// check if download has completed
	if len(download.Bytes) >= int(download.Length) {
		download.Bytes = download.Bytes[:download.Length]
		download.NotifyChannel <- true
		delete(peer.pendingDownloads, downloadId)

		// updating rolling speed, but only if this was a large enough block
		if download.Length >= BLOCK_SIZE / 4 {
			timePerBlock := float64(time.Now().Sub(download.StartTime)) * BLOCK_SIZE / float64(download.Length)
			peer.rollingSpeed = time.Duration(float64(peer.rollingSpeed) * 0.7 + timePerBlock * 0.3)

			if peer.rollingSpeed <= 0 {
				peer.rollingSpeed = 1
			}
		}
	}
}

func (this *PeerList) handleUploadFail(peer *Peer, downloadId int64) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	download, ok := peer.pendingDownloads[downloadId]
	if ok {
		download.Bytes = nil
		download.NotifyChannel <- false
		delete(peer.pendingDownloads, downloadId)
	}
}

func (this *PeerList) handleDownload(peer *Peer, downloadId int64, fileHash string, blockIndex int) {
	cacheFile, err := this.cache.DownloadInitHash(fileHash)
	if err != nil {
		Log.Warn.Printf("Failed to handle download from %s: cache doesn't contain file %s", peer.addr, fileHash)
		peer.conn.Write(protocolSendUploadFail(downloadId).Bytes())
		return
	}

	bytes := this.cache.DownloadRead(cacheFile, blockIndex, false)
	if bytes == nil {
		Log.Warn.Printf("Failed to handle download from %s: cache did not provide block %s/%d", peer.addr, fileHash, blockIndex)
		peer.conn.Write(protocolSendUploadFail(downloadId).Bytes())
		return
	}

	Log.Debug.Printf("Handling download %d from %s, providing block %s/%d", downloadId, peer.addr, fileHash, blockIndex)

	go func() {
		peer.mu.Lock()
		peer.conn.Write(protocolSendUpload(downloadId, int64(len(bytes))).Bytes())
		peer.mu.Unlock()

		for i := 0; i < len(bytes); i += TRANSFER_PACKET_SIZE {
			peer.mu.Lock()
			// write next TRANSFER_PACKET_SIZE bytes, or up to the block length
			limit := i + TRANSFER_PACKET_SIZE
			if limit > len(bytes) {
				limit = len(bytes)
			}

			peer.conn.Write(protocolSendUploadPart(downloadId, bytes[i:limit]).Bytes())
			peer.mu.Unlock()
		}
	}()
}

func (this *PeerList) handleDownloadCancel(peer *Peer, downloadId int64) {
	// TODO
}

/*
 * Request peerBlock from peer for download, and insert a pending download object for it.
 */
func (this *PeerList) startDownload(peer *Peer, peerBlock PeerBlock) *PeerDownload {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.conn != nil {
		downloadId := rand.Int63()

		// send download packet
		_, err := peer.conn.Write(protocolSendDownload(downloadId, peerBlock.FileHash, peerBlock.Index).Bytes())
		if err != nil {
			Log.Debug.Printf("Failed to initialize download from %s: error during download packet: %s", peer.addr, err.Error())
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
		Log.Debug.Printf("Initialized download from %s (id=%d, blk=%s/%d)", peer.addr, download.Id, peerBlock.FileHash, peerBlock.Index)
		return download
	} else {
		Log.Debug.Printf("Failed to initialize download from %s: peer is disconnected", peer.addr)
		peer.rollingSpeed *= 2
		return nil
	}
}

/*
 * Cancel a pending download with peer, and notify the remote end.
 */
func (this *PeerList) cancelDownload(peer *Peer, downloadId int64) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	delete(peer.pendingDownloads, downloadId)
	peer.conn.Write(protocolSendDownloadCancel(downloadId).Bytes())
}

/*
 * Local request (from cache) to retrieve a block from peers.
 */
func (this *PeerList) DownloadBlock(fileHash string, blockIndex int) []byte {
	peerBlock := PeerBlock{FileHash: fileHash, Index: blockIndex}
	ignorePeers := make(map[int64]bool) // set of peers that we shouldn't download from

	for attempt := 0; attempt < DOWNLOAD_MAX_ATTEMPTS; attempt++ {
		peer := this.findPeerWithBlock(peerBlock, ignorePeers)
		if peer == nil {
			Log.Warn.Printf("Failed to find peer with block %s/%d", peerBlock.FileHash, peerBlock.Index)
			return nil
		}

		download := this.startDownload(peer, peerBlock)
		if download == nil {
			ignorePeers[peer.peerId] = true
			continue
		}

		timeout := peer.rollingSpeed * 4
		if timeout < DOWNLOAD_MIN_TIMEOUT {
			timeout = DOWNLOAD_MIN_TIMEOUT
		}

		select {
			case <- download.NotifyChannel:
				if download.Bytes != nil {
					return download.Bytes
				} else {
					Log.Warn.Printf("Download from peer %s failed", peer.addr)
					ignorePeers[peer.peerId] = true
					continue
				}
			case <- time.After(timeout):
				Log.Warn.Printf("Download from peer %s timed out", peer.addr)
				this.cancelDownload(peer, download.Id)
				ignorePeers[peer.peerId] = true
				continue
		}
	}

	return nil
}

/*
 * Returns a peer who has announced the block, or nil if no such peer exists.
 * We prefer peers that have higher download speeds.
 */
func (this *PeerList) findPeerWithBlock(peerBlock PeerBlock, ignorePeers map[int64]bool) *Peer {
	// we first identify the set of peers that have the block available
	//  (both connected and announced block recently)
	// out of those, we select each one with probability proportional to 1/rollingSpeed
	this.mu.Lock()
	defer this.mu.Unlock()

	peerWeights := make(map[*Peer]int64)
	var totalWeight int64
	for peerId, peer := range this.peers {
		peer.mu.Lock()
		if peer.availableBlocks[peerBlock] && peer.conn != nil && !ignorePeers[peerId] {
			if peer.rollingSpeed == 0 {
				peerWeights[peer] = int64(DEFAULT_PEER_SPEED)
			} else {
				peerWeights[peer] = int64(DEFAULT_PEER_SPEED * 100000 / peer.rollingSpeed)
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

// extracted from golang-geo library (https://github.com/kellydunn/golang-geo/blob/master/point.go)
func (this *PeerList) ipDistance(record *geoip2.City, ip net.IP) float64 {
	ipRecord, err := this.geoip.City(ip)
	if err != nil {
		Log.Warn.Printf("Geoip query failed: %s", err.Error())
		return math.MaxFloat64
	}

	dLat := (record.Location.Latitude - ipRecord.Location.Latitude) * (math.Pi / 180.0)
	dLon := (record.Location.Longitude - ipRecord.Location.Longitude) * (math.Pi / 180.0)
	lat1 := record.Location.Latitude * (math.Pi / 180.0)
	lat2 := ipRecord.Location.Latitude * (math.Pi / 180.0)
	a1 := math.Sin(dLat/2) * math.Sin(dLat/2)
	a2 := math.Sin(dLon/2) * math.Sin(dLon/2) * math.Cos(lat1) * math.Cos(lat2)
	a := a1 + a2
	return 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

/*
 * Returns base URL of closest peer to provided IP, or empty string if we shouldn't redirect
 */
func (this *PeerList) findClosestPeer(clientIp net.IP) string {
	if this.geoip == nil || this.myIP == nil {
		return ""
	}

	record, err := this.geoip.City(clientIp)
	if err != nil {
		Log.Warn.Printf("Geoip query failed: %s", err.Error())
		return ""
	}

	peerIps := make(map[string]net.IP)
	this.mu.Lock()
	for _, peer := range this.peers {
		if peer.conn != nil {
			peerIps[peer.location] = net.ParseIP(strings.Split(peer.addr, ":")[0])
		}
	}
	this.mu.Unlock()

	minDistance := this.ipDistance(record, this.myIP)
	minLocation := ""

	for peerLocation, peerIp := range peerIps {
		dist := this.ipDistance(record, peerIp)
		if dist < minDistance {
			minDistance = dist
			minLocation = peerLocation
		}
	}

	return minLocation
}

func (this *PeerList) refreshPeers() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// try connecting to existing disconnected peers
	for _, peer := range this.peers {
		this.refreshPeer(peer)
	}

	// also try the initial connect map
	for addr, lastTime := range this.discoverable {
		if time.Now().After(lastTime.Add(CONNECT_INTERVAL)) {
			this.discoverable[addr] = time.Now()
			go func(addr string) {
				Log.Info.Printf("Attempting to connect to %s", addr)
				tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
				if err != nil {
					Log.Info.Printf("Failed to connect to %s: %s", addr, err.Error())
					return
				}

				nConn, err := net.DialTCP("tcp", nil, tcpAddr)
				if err != nil {
					Log.Info.Printf("Failed to connect to %s: %s", addr, err.Error())
					return
				}

				go this.handleConnection(nConn, nil, addr)
			}(addr)
		}
	}
}

func (this *PeerList) refreshPeer(peer *Peer) {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	// try to connect to the peer if CONNECT_INTERVAL seconds has elapsed
	// note that updating peer.conn is done in handleConnection, and only after the HELLO exchange succeeds
	if peer.conn == nil && !peer.connecting && time.Now().After(peer.lastConnectTime.Add(CONNECT_INTERVAL)) {
		peer.lastConnectTime = time.Now()
		peer.connecting = true

		go func() {
			defer func() {
				peer.mu.Lock()
				peer.connecting = false
				peer.mu.Unlock()
			}()

			Log.Info.Printf("Attempting to connect to %s", peer.addr)
			nConn, err := net.DialTimeout("tcp", peer.addr, CONNECT_TIMEOUT)
			if err != nil {
				Log.Info.Printf("Failed to connect to %s: %s", peer.addr, err.Error())
				return
			}

			this.handleConnection(nConn.(*net.TCPConn), peer, "")
		}()
	}

	// announce locally cached blocks every ANNOUNCE_INTERVAL seconds
	if peer.conn != nil && time.Now().After(peer.lastAnnounceTime.Add(ANNOUNCE_INTERVAL)) {
		peer.lastAnnounceTime = time.Now()

		// we can't call cache while we have the lock, so we do with an asynchronous callback
		go this.cache.PrepareAnnounce(func(announceFiles []AnnounceFile) {
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if peer.conn != nil {
				Log.Debug.Printf("Announcing %d files to %s", len(announceFiles), peer.addr)
				for _, buf := range protocolSendAnnounce(announceFiles) {
					peer.conn.Write(buf.Bytes())
				}
			}
		})
	}
}
