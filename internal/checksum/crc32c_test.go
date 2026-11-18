package checksum

import (
	"errors"
	"testing"
)

func TestSumAndVerify(t *testing.T) {
	payload := []byte(`{"service":"checkout","latency_ms":42,"ok":true}`)
	sum := Sum32C(payload)
	if sum == 0 {
		t.Fatal("expected non-zero crc32c checksum")
	}

	if err := Verify(payload, sum); err != nil {
		t.Fatalf("expected valid payload to pass verification: %v", err)
	}

	// Simulate single-bit flip (bit-rot) in storage.
	corrupted := append([]byte(nil), payload...)
	corrupted[12] ^= 0x04

	err := Verify(corrupted, sum)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("expected ErrMismatch on flipped bit, got %v", err)
	}
}

func TestBase64RoundTrip(t *testing.T) {
	sum := Sum32C([]byte("cindersearch-segment-payload"))
	encoded := FormatBase64(sum)
	decoded, err := ParseBase64(encoded)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if decoded != sum {
		t.Fatalf("round-trip mismatch: got 0x%08x, want 0x%08x", decoded, sum)
	}
}

func BenchmarkCRC32C1KB(b *testing.B) {
	buf := make([]byte, 1024)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Sum32C(buf)
	}
}
