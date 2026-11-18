package checksum

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrMismatch indicates bit-rot or payload corruption detected during checksum verification.
var ErrMismatch = errors.New("crc32c checksum mismatch: data corruption detected")

// castagnoliTable is initialized once and leverages hardware CRC32C instructions
// (SSE4.2 on x86_64, CRC on ARM64) inside Go's standard library.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Sum32C computes the Castagnoli CRC32C digest of data.
func Sum32C(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// Verify checks data against an expected CRC32C digest, returning ErrMismatch if corrupted.
func Verify(data []byte, expected uint32) error {
	actual := Sum32C(data)
	if actual != expected {
		return fmt.Errorf("%w: expected 0x%08x, got 0x%08x", ErrMismatch, expected, actual)
	}
	return nil
}

// FormatHex formats a uint32 CRC32C digest as a lowercase 8-character hex string.
func FormatHex(sum uint32) string {
	return fmt.Sprintf("%08x", sum)
}

// FormatBase64 formats a uint32 CRC32C digest in big-endian Base64, matching S3's
// x-amz-checksum-crc32c header specification.
func FormatBase64(sum uint32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], sum)
	return base64.StdEncoding.EncodeToString(buf[:])
}

// ParseBase64 parses a big-endian Base64 CRC32C digest.
func ParseBase64(encoded string) (uint32, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, fmt.Errorf("invalid base64 crc32c: %w", err)
	}
	if len(raw) != 4 {
		return 0, fmt.Errorf("invalid crc32c byte length %d: expected 4", len(raw))
	}
	return binary.BigEndian.Uint32(raw), nil
}
