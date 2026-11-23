//go:build integration

package s3store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/s3store"
)

func TestRealS3OrMinIOObjectLifecycleAndPresignedRange(t *testing.T) {
	endpoint := os.Getenv("CINDER_TEST_S3_ENDPOINT")
	bucket := os.Getenv("CINDER_TEST_S3_BUCKET")
	region := os.Getenv("CINDER_TEST_S3_REGION")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if region == "" {
		region = "us-east-1"
	}
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("skipping real S3/MinIO integration test: set CINDER_TEST_S3_ENDPOINT, CINDER_TEST_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
	}

	client, err := s3store.NewClient(s3store.Config{
		Endpoint:  endpoint,
		Region:    region,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := fmt.Sprintf("_integration_test/probe-%d.docs", time.Now().UnixNano())
	payload := []byte(`{"id":"1","msg":"hello"}` + `{"id":"2","msg":"zero-hop-s3-range-verified"}`)
	defer func() {
		_ = client.DeleteObject(context.Background(), key)
	}()

	// 1. PutObject with CRC32C checksum
	crc, err := client.PutObject(ctx, key, payload)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if crc != checksum.Sum32C(payload) {
		t.Fatalf("PutObject CRC32C mismatch: got %x want %x", crc, checksum.Sum32C(payload))
	}

	// 2. GetObject round-trip
	got, gotCRC, err := client.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if !bytes.Equal(got, payload) || gotCRC != crc {
		t.Fatalf("GetObject mismatch: len=%d crc=%x want len=%d crc=%x", len(got), gotCRC, len(payload), crc)
	}

	// 3. GetObjectRange partial read of the second document
	offset := int64(len(`{"id":"1","msg":"hello"}`))
	length := int64(len(`{"id":"2","msg":"zero-hop-s3-range-verified"}`))
	slice, err := client.GetObjectRange(ctx, key, offset, length)
	if err != nil {
		t.Fatalf("GetObjectRange failed: %v", err)
	}
	wantSlice := payload[offset : offset+length]
	if !bytes.Equal(slice, wantSlice) {
		t.Fatalf("GetObjectRange mismatch: got %q want %q", string(slice), string(wantSlice))
	}

	// 4. ListObjects prefix discovery
	objs, err := client.ListObjects(ctx, "_integration_test/")
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	found := false
	for _, obj := range objs {
		if obj.Key == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ListObjects to contain %q, got %+v", key, objs)
	}

	// 5. Direct client -> S3 Zero-Hop Presigned HTTP 206 Range fetch
	presigned, err := client.PresignGetURL(key, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetURL failed: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, presigned, nil)
	if err != nil {
		t.Fatalf("new presigned req: %v", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("presigned direct GET failed: %v", err)
	}
	directBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected HTTP 206 Partial Content from presigned S3 Range GET, got %d (%s)", resp.StatusCode, string(directBody))
	}
	if !bytes.Equal(directBody, wantSlice) {
		t.Fatalf("presigned Range body mismatch: got %q want %q", string(directBody), string(wantSlice))
	}
	if checksum.Sum32C(directBody) != checksum.Sum32C(wantSlice) {
		t.Fatalf("presigned Range CRC32C mismatch")
	}

	// 6. DeleteObject
	if err := client.DeleteObject(ctx, key); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
}
