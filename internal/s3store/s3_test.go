package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

func TestS3PutRangeAndZeroHopPresignedFetch(t *testing.T) {
	srv := NewMockServer("ak-test", "sk-test", "us-east-1")
	defer srv.Close()

	client, err := NewClient(Config{
		Endpoint:  srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-data",
		AccessKey: "ak-test",
		SecretKey: "sk-test",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	ctx := context.Background()
	doc1 := `{"service":"auth","ok":true}`
	doc2 := `{"service":"payments","amount":99.5}`
	combined := []byte(doc1 + doc2)

	if _, err := client.PutObject(ctx, "logs/shard-0/seg-1.docs", combined); err != nil {
		t.Fatalf("put object: %v", err)
	}

	// Zero-hop presigned URL direct fetch using HTTP Range header for doc2.
	presigned, err := client.PresignGetURL("logs/shard-0/seg-1.docs", 2*time.Minute)
	if err != nil {
		t.Fatalf("presign url: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, presigned, nil)
	if err != nil {
		t.Fatalf("new http request: %v", err)
	}
	start := len(doc1)
	end := len(doc1) + len(doc2) - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("direct s3 fetch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content, got %d (range %d-%d)", resp.StatusCode, start, end)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read direct s3 body: %v", err)
	}
	if string(got) != doc2 {
		t.Fatalf("direct fetch mismatch: got %q, want %q", string(got), doc2)
	}

	// Tampering with presigned URL must be rejected with 403 Forbidden.
	tampered := strings.Replace(presigned, "seg-1.docs", "seg-999.docs", 1)
	badResp, err := http.Get(tampered)
	if err != nil {
		t.Fatalf("tampered request: %v", err)
	}
	_ = badResp.Body.Close()
	if badResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for tampered presigned URL, got %d", badResp.StatusCode)
	}

	// Bit-rot inside S3 object must trigger ErrMismatch on full read.
	if err := srv.CorruptObject("cindersearch-data", "logs/shard-0/seg-1.docs", 4); err != nil {
		t.Fatalf("corrupt s3 object: %v", err)
	}
	if _, _, err := client.GetObject(ctx, "logs/shard-0/seg-1.docs"); !errors.Is(err, checksum.ErrMismatch) {
		t.Fatalf("expected ErrMismatch on corrupted S3 object, got %v", err)
	}
}
