package tar

import (
	"fmt"
	"time"
)

func sumBytes(header []byte) uint32 {
	sum := uint32(0)
	for _, value := range header {
		sum += uint32(value)
	}
	return sum
}

const HeaderBytes = 512

func MakeHeader(seq uint64, length int) []byte {
	// See: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/pax.html - ustar interchange format
	header := make([]byte, HeaderBytes)
	// name:
	filename := fmt.Sprintf("%016d.entry", seq)
	copy(header[0:], filename)
	// mode - always 0400 (r--):
	copy(header[100:], "0000400\x00")
	// uid - always 1000:
	copy(header[108:], "0001750\x00")
	// gid - always 1000:
	copy(header[116:], "0001750\x00")
	// size - formatted in octal:
	size := fmt.Sprintf("%011o\x00", length)
	copy(header[124:], size)
	// mtime - Unix seconds-based timestamp in octal:
	mtime := fmt.Sprintf("%011o\x00", time.Now().Unix())
	copy(header[136:], mtime)
	// chksum - this is assumed to be all <space> character for checksum computation, and we recompute it at the end:
	copy(header[148:], "        ")
	// typeflag - 0 for normal file:
	header[156] = '0'
	// magic+version - this identifies the archive as ustar and not GNU tar (note lack of 0x20):
	copy(header[257:], "ustar\x0000")
	// uname - not likely to match a real user name on the host:
	copy(header[265:], "microtape\x00")
	// gname - also a made-up group name:
	copy(header[297:], "microtape\x00")
	// CHKSUM - now that we have all the fields, we can compute the sum:
	checksumValue := sumBytes(header)
	chksum := fmt.Sprintf("%06o\x00 ", checksumValue)
	copy(header[148:], chksum)
	return header
}
