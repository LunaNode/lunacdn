package lunacdn

import "encoding/binary"
import "bytes"
import "net"

// extract a zero-byte-terminated string from the byte buffer
func protocolReadString(buf *bytes.Buffer) string {
	strBytes := make([]byte, 0)
	for {
		b, _ := buf.ReadByte()
		if b != 0 {
			strBytes = append(strBytes, b)
		} else {
			break
		}
	}

	return string(strBytes)
}

type AnnounceFile struct {
	Hash string
	Length int64
	Indexes []int
}

func protocolReadHello(conn *net.TCPConn) (bool, int64) {
	tbuf := make([]byte, 10)
	numRead, err := conn.Read(tbuf)
	buf := bytes.NewBuffer(tbuf)
	if numRead != 10 || err != nil {
		Log.Warn.Printf("Bad HELLO: didn't get ten bytes")
		return false, 0
	}

	header, _ := buf.ReadByte()
	if header != HEADER_CONSTANT {
		Log.Warn.Printf("Bad HELLO: incorrect header constant")
		return false, 0
	}

	pktType, _ := buf.ReadByte()
	if pktType != PROTO_HELLO {
		Log.Warn.Printf("Bad HELLO: incorrect packet HELLO header")
		return false, 0
	}

	var peerId int64
	binary.Read(buf, binary.BigEndian, &peerId)
	return true, peerId
}

func protocolReadAnnounce(buf *bytes.Buffer) []AnnounceFile {
	files := make([]AnnounceFile, 0)

	var numFiles int32
	binary.Read(buf, binary.BigEndian, &numFiles)
	for i := int32(0); i < numFiles; i++ {
		file := AnnounceFile{}
		file.Hash = protocolReadString(buf)
		binary.Read(buf, binary.BigEndian, &file.Length)

		var peerNumBlocks, idx int32
		binary.Read(buf, binary.BigEndian, &peerNumBlocks)
		for j := int32(0); j < peerNumBlocks; j++ {
			binary.Read(buf, binary.BigEndian, &idx)
			file.Indexes = append(file.Indexes, int(idx))
		}

		files = append(files, file)
	}

	return files
}

func protocolReadUpload(buf *bytes.Buffer) (int64, int64) {
	var downloadId, downloadLen int64
	binary.Read(buf, binary.BigEndian, &downloadId)
	binary.Read(buf, binary.BigEndian, &downloadLen)
	return downloadId, downloadLen
}

func protocolReadUploadPart(buf *bytes.Buffer) (int64, []byte) {
	var downloadId int64
	part := make([]byte, buf.Len() - 8)
	binary.Read(buf, binary.BigEndian, &downloadId)
	buf.Read(part)
	return downloadId, part
}

func protocolReadDownload(buf *bytes.Buffer) (int64, string, int) {
	var downloadId int64
	binary.Read(buf, binary.BigEndian, &downloadId)
	fileHash := protocolReadString(buf)
	var blockIndex int32
	binary.Read(buf, binary.BigEndian, &blockIndex)
	return downloadId, fileHash, int(blockIndex)
}

var fakeLength uint16 // placeholder for length

// sets the second uint16 in a packet to the length of the packet
func protocolAssignLength(buf *bytes.Buffer) {
	binary.BigEndian.PutUint16(buf.Bytes()[2:4], uint16(buf.Len()))
}

// HELLO is sent on connect and does not have length
func protocolSendHello(peerId int64) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_HELLO)
	binary.Write(buf, binary.BigEndian, peerId)
	return buf
}

func protocolSendAnnounce(files []AnnounceFile) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_ANNOUNCE)
	binary.Write(buf, binary.BigEndian, fakeLength)
	binary.Write(buf, binary.BigEndian, int32(len(files)))

	for _, file := range files {
		buf.Write([]byte(file.Hash))
		buf.WriteByte(0) // null terminator
		binary.Write(buf, binary.BigEndian, file.Length)
		binary.Write(buf, binary.BigEndian, int32(len(file.Indexes)))

		for _, idx := range file.Indexes {
			binary.Write(buf, binary.BigEndian, int32(idx))
		}
	}

	protocolAssignLength(buf)
	return buf
}

func protocolSendDownload(downloadId int64, fileHash string, blockIndex int) *bytes.Buffer {
	blockIndex32 := int32(blockIndex)

	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_DOWNLOAD)
	binary.Write(buf, binary.BigEndian, fakeLength)
	binary.Write(buf, binary.BigEndian, downloadId)
	buf.Write([]byte(fileHash))
	buf.WriteByte(0)
	binary.Write(buf, binary.BigEndian, blockIndex32)
	protocolAssignLength(buf)
	return buf
}

func protocolSendDownloadCancel(downloadId int64) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_DOWNLOAD_CANCEL)
	binary.Write(buf, binary.BigEndian, fakeLength)
	binary.Write(buf, binary.BigEndian, downloadId)
	protocolAssignLength(buf)
	return buf
}

func protocolSendUpload(downloadId int64, downloadLen int64) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_UPLOAD)
	binary.Write(buf, binary.BigEndian, fakeLength)
	binary.Write(buf, binary.BigEndian, downloadId)
	binary.Write(buf, binary.BigEndian, downloadLen)
	protocolAssignLength(buf)
	return buf
}

func protocolSendUploadPart(downloadId int64, data []byte) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.WriteByte(HEADER_CONSTANT)
	buf.WriteByte(PROTO_UPLOAD_PART)
	binary.Write(buf, binary.BigEndian, fakeLength)
	binary.Write(buf, binary.BigEndian, downloadId)
	buf.Write(data)
	protocolAssignLength(buf)
	return buf
}
