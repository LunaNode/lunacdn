package lunacdn

import "encoding/binary"
import "encoding/hex"
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

	var numFiles uint32
	binary.Read(buf, binary.BigEndian, &numFiles)
	for i := uint32(0); i < numFiles; i++ {
		file := AnnounceFile{}

		// the hex-encoded file.Hash is sent as binary in the stream
		hashBytes := make([]byte, 16)
		buf.Read(hashBytes)
		file.Hash = hex.EncodeToString(hashBytes)

		// int64 file length
		binary.Read(buf, binary.BigEndian, &file.Length)

		// find out what blocks the peer has
		// block indexes are range-encoded, we call each range a section
		var sectionStart uint32
		var numSections, sectionLength uint16
		binary.Read(buf, binary.BigEndian, &numSections)
		for j := uint16(0); j < numSections; j++ {
			binary.Read(buf, binary.BigEndian, &sectionStart)
			binary.Read(buf, binary.BigEndian, &sectionLength)

			for idx := int(sectionStart); idx < int(sectionStart) + int(sectionLength); idx++ {
				file.Indexes = append(file.Indexes, int(idx))
			}
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

type AnnounceSection struct {
	Start uint32
	Length uint16
}

func protocolSendAnnounce(files []AnnounceFile) []*bytes.Buffer {
	// announce is special since we need to split up the packet into chunks
	//  in case the overall announcement exceeds 2^16 bytes
	// we do this by maintaining a partial packet (without the header) and
	//  adding blocks and files until it fills, then adding the header
	//  and restarting on a new packet
	currentNumFiles := 0
	currentPacket := new(bytes.Buffer)
	packets := make([]*bytes.Buffer, 0)
	packetSoftLimit := 32 * 1024

	for _, file := range files {
		// first, deconstruct file block indexes into a set of sections
		// each section is six bytes and identifies range of block indexes
		// e.g. 0,5 10,5 means we have 0,1,2,3,4,10,11,12,13,14
		fileSections := make([]AnnounceSection, 0)
		currentSection := AnnounceSection{}
		lastIndex := -1
		for _, idx := range file.Indexes {
			if lastIndex == -1 {
				currentSection.Start = uint32(idx)
				currentSection.Length = 1
			} else if idx != lastIndex + 1 {
				// restart on new section
				fileSections = append(fileSections, currentSection)
				currentSection = AnnounceSection{Start: uint32(idx), Length: 1}
			} else {
				currentSection.Length++
			}

			lastIndex = idx
		}
		fileSections = append(fileSections, currentSection)

		// iteratively construct new packets until all sections are handled
		sectionStart := 0
		for sectionStart < len(fileSections) {
			// add file header
			currentNumFiles++
			hashBytes, err := hex.DecodeString(file.Hash)
			if err != nil {
				panic(err)
			}
			currentPacket.Write(hashBytes)
			binary.Write(currentPacket, binary.BigEndian, file.Length)

			// determine how many sections to include and add them
			numSections := len(fileSections) - sectionStart
			if currentPacket.Len() + numSections * 6 > packetSoftLimit {
				numSections = (packetSoftLimit - currentPacket.Len()) / 6
			}

			binary.Write(currentPacket, binary.BigEndian, uint16(numSections))
			for sectionIndex := sectionStart; sectionIndex < sectionStart + numSections; sectionIndex++ {
				binary.Write(currentPacket, binary.BigEndian, fileSections[sectionIndex].Start)
				binary.Write(currentPacket, binary.BigEndian, fileSections[sectionIndex].Length)
			}
			sectionStart = sectionStart + numSections

			// create new packet if needed
			if currentPacket.Len() > packetSoftLimit - 64 {
				buf := new(bytes.Buffer)
				buf.WriteByte(HEADER_CONSTANT)
				if len(packets) == 0 {
					buf.WriteByte(PROTO_ANNOUNCE)
				} else {
					buf.WriteByte(PROTO_ANNOUNCE_CONTINUE)
				}
				binary.Write(buf, binary.BigEndian, uint16(4 + 4 + currentPacket.Len()))
				binary.Write(buf, binary.BigEndian, uint32(currentNumFiles))
				buf.Write(currentPacket.Bytes())
				packets = append(packets, buf)
				currentPacket.Reset()
				currentNumFiles = 0
			}
		}
	}

	if currentNumFiles > 0 {
		buf := new(bytes.Buffer)
		buf.WriteByte(HEADER_CONSTANT)
		if len(packets) == 0 {
			buf.WriteByte(PROTO_ANNOUNCE)
		} else {
			buf.WriteByte(PROTO_ANNOUNCE_CONTINUE)
		}
		binary.Write(buf, binary.BigEndian, uint16(4 + 4 + currentPacket.Len()))
		binary.Write(buf, binary.BigEndian, uint32(currentNumFiles))
		buf.Write(currentPacket.Bytes())
		packets = append(packets, buf)
	}

	return packets
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
