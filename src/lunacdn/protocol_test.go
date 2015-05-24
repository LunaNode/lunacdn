package lunacdn

import "testing"
import "math/rand"

func TestAnnounce(t *testing.T) {
	for testIteration := 0; testIteration < 8; testIteration++ {
		original := make(map[string]AnnounceFile)
		transmitFiles := make([]AnnounceFile, rand.Intn(32) + 1)
		hexLetters := []rune("0123456789abcdef")

		for i := range transmitFiles {
			fileHashArray := make([]rune, 32)
			for j := range fileHashArray {
				fileHashArray[j] = hexLetters[rand.Intn(len(hexLetters))]
			}
			fileHash := string(fileHashArray)
			fileBlocks := rand.Intn(72 * 1024)
			file := AnnounceFile{
				Hash: fileHash,
				Length: int64(BLOCK_SIZE * fileBlocks),
				Indexes: make([]int, 0),
			}

			loadFactor := rand.Float32()
			for j := 0; j < fileBlocks; j++ {
				if rand.Float32() < loadFactor {
					file.Indexes = append(file.Indexes, j)
				}
			}

			original[fileHash] = file
			transmitFiles[i] = file
		}

		// read out to new map
		packets := protocolSendAnnounce(transmitFiles)
		newMap := make(map[string]AnnounceFile)
		for _, packet := range packets {
			// read header
			packet.ReadByte()
			packet.ReadByte()
			packet.ReadByte()
			packet.ReadByte()

			packetFiles := protocolReadAnnounce(packet)
			for _, file := range packetFiles {
				newFile, ok := newMap[file.Hash]
				if !ok {
					newMap[file.Hash] = file
				} else {
					for _, idx := range file.Indexes {
						newFile.Indexes = append(newFile.Indexes, idx)
					}
					newMap[file.Hash] = newFile
				}
			}
		}

		// compare original and new map
		if len(original) != len(newMap) {
			t.Errorf("len(original) != len(newMap) (%d != %d)", len(original), len(newMap))
			return
		}
		for hash, origFile := range original {
			newFile, ok := newMap[hash]
			if !ok {
				t.Errorf("newMap missing file %s", hash)
				return
			}
			for i := range origFile.Indexes {
				if newFile.Indexes[i] != origFile.Indexes[i] {
					t.Errorf("new file indexes do not match for %s", hash)
					return
				}
			}
		}
	}
}
