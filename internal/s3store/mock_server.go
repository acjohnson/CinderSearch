package s3store

import (
	"crypto/hmac"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

type storedObject struct {
	data      []byte
	crc32c    uint32
	etag      string
	updatedAt time.Time
}

// MockServer is an embedded, concurrency-safe S3-compatible HTTP server supporting
// PUT, GET, HTTP Range (206 Partial Content), LIST, DELETE, CRC32C verification,
// and real AWS SigV4 presigned URL signature validation.
type MockServer struct {
	mu        sync.RWMutex
	objects   map[string]map[string]storedObject // bucket -> key -> object
	accessKey string
	secretKey string
	region    string
	server    *httptest.Server
	faultFn   func(method, bucket, key string) (int, string)
}

// NewMockServer starts an embedded S3 HTTP test server on localhost.
func NewMockServer(accessKey, secretKey, region string) *MockServer {
	if accessKey == "" {
		accessKey = "cindersearch-access"
	}
	if secretKey == "" {
		secretKey = "cindersearch-secret"
	}
	if region == "" {
		region = "us-east-1"
	}
	m := &MockServer{
		objects:   make(map[string]map[string]storedObject),
		accessKey: accessKey,
		secretKey: secretKey,
		region:    region,
	}
	m.server = httptest.NewServer(m)
	return m
}

// URL returns the base HTTP endpoint of the embedded S3 server.
func (m *MockServer) URL() string {
	return m.server.URL
}

// Close shuts down the embedded S3 HTTP server.
func (m *MockServer) Close() {
	m.server.Close()
}

// SetFaultInjector installs (or clears when nil) an HTTP fault hook for testing S3 outage resilience.
// Returning a status >= 400 short-circuits the request with that HTTP status and message.
func (m *MockServer) SetFaultInjector(fn func(method, bucket, key string) (int, string)) {
	m.mu.Lock()
	m.faultFn = fn
	m.mu.Unlock()
}

// CorruptObject intentionally flips a bit in an S3 object for testing bit-rot resilience.
func (m *MockServer) CorruptObject(bucket, key string, byteOffset int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[bucket]
	if !ok {
		return ErrNotFound
	}
	obj, ok := b[key]
	if !ok {
		return ErrNotFound
	}
	if byteOffset < 0 || byteOffset >= len(obj.data) {
		byteOffset = 0
	}
	obj.data[byteOffset] ^= 0x55
	b[key] = obj
	return nil
}

// ServeHTTP handles S3 path-style requests: /{bucket} and /{bucket}/{key...}.
func (m *MockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Validate presigned URL signatures when query params carry X-Amz-Signature.
	if r.URL.Query().Get("X-Amz-Signature") != "" {
		if err := m.verifyPresignedURL(r); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing bucket", http.StatusBadRequest)
		return
	}
	bucket := parts[0]
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	m.mu.RLock()
	faultFn := m.faultFn
	m.mu.RUnlock()
	if faultFn != nil {
		if status, msg := faultFn(r.Method, bucket, key); status >= 400 {
			http.Error(w, msg, status)
			return
		}
	}

	if key == "" {
		if r.Method == http.MethodGet {
			m.handleList(w, r, bucket)
			return
		}
		http.Error(w, "unsupported bucket method", http.StatusMethodNotAllowed)
		return
	}

	switch r.Method {
	case http.MethodPut:
		m.handlePut(w, r, bucket, key)
	case http.MethodGet:
		m.handleGet(w, r, bucket, key)
	case http.MethodHead:
		m.handleHead(w, bucket, key)
	case http.MethodDelete:
		m.handleDelete(w, bucket, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *MockServer) handlePut(w http.ResponseWriter, r *http.Request, bucket, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	actualCRC := checksum.Sum32C(data)
	if hdr := r.Header.Get("x-amz-checksum-crc32c"); hdr != "" {
		expected, err := checksum.ParseBase64(hdr)
		if err != nil || expected != actualCRC {
			http.Error(w, "x-amz-checksum-crc32c mismatch", http.StatusBadRequest)
			return
		}
	}

	etag := `"` + sha256Hex(data)[:32] + `"`
	m.mu.Lock()
	if _, ok := m.objects[bucket]; !ok {
		m.objects[bucket] = make(map[string]storedObject)
	}
	m.objects[bucket][key] = storedObject{
		data:      data,
		crc32c:    actualCRC,
		etag:      etag,
		updatedAt: time.Now().UTC(),
	}
	m.mu.Unlock()

	w.Header().Set("ETag", etag)
	w.Header().Set("x-amz-checksum-crc32c", checksum.FormatBase64(actualCRC))
	w.WriteHeader(http.StatusOK)
}

func (m *MockServer) handleGet(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m.mu.RLock()
	b := m.objects[bucket]
	obj, ok := b[key]
	m.mu.RUnlock()
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}

	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Accept-Ranges", "bytes")

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		w.Header().Set("x-amz-checksum-crc32c", checksum.FormatBase64(obj.crc32c))
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)
		return
	}

	start, end, err := parseByteRange(rangeHeader, len(obj.data))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	slice := obj.data[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(slice)
}

func (m *MockServer) handleHead(w http.ResponseWriter, bucket, key string) {
	m.mu.RLock()
	b := m.objects[bucket]
	obj, ok := b[key]
	m.mu.RUnlock()
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.Header().Set("x-amz-checksum-crc32c", checksum.FormatBase64(obj.crc32c))
	w.WriteHeader(http.StatusOK)
}

func (m *MockServer) handleDelete(w http.ResponseWriter, bucket, key string) {
	m.mu.Lock()
	if b, ok := m.objects[bucket]; ok {
		delete(b, key)
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (m *MockServer) handleList(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	m.mu.RLock()
	b := m.objects[bucket]
	var keys []string
	for k := range b {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var resp listBucketXML
	for _, k := range keys {
		obj := b[k]
		resp.Contents = append(resp.Contents, struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
			ETag string `xml:"ETag"`
		}{
			Key:  k,
			Size: int64(len(obj.data)),
			ETag: obj.etag,
		})
	}
	m.mu.RUnlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (m *MockServer) verifyPresignedURL(r *http.Request) error {
	q := r.URL.Query()
	sig := q.Get("X-Amz-Signature")
	amzDate := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	cred := q.Get("X-Amz-Credential")

	if sig == "" || amzDate == "" || expiresStr == "" || cred == "" {
		return fmt.Errorf("missing presigned signature parameters")
	}

	issuedAt, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	ttlSec, err := strconv.Atoi(expiresStr)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Expires: %w", err)
	}
	if time.Now().UTC().After(issuedAt.Add(time.Duration(ttlSec) * time.Second)) {
		return fmt.Errorf("presigned url expired")
	}

	credParts := strings.Split(cred, "/")
	if len(credParts) < 5 || credParts[0] != m.accessKey {
		return fmt.Errorf("invalid access key in credential")
	}
	dateStamp := credParts[1]
	region := credParts[2]
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, region)

	qNoSig := r.URL.Query()
	qNoSig.Del("X-Amz-Signature")
	canonicalQuery := encodeCanonicalQuery(qNoSig)
	canonicalHeaders := "host:" + r.Host + "\n"
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.EscapedPath()),
		canonicalQuery,
		canonicalHeaders,
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	sigKey := deriveSigningKey(m.secretKey, dateStamp, region, "s3")
	expectedSig := hex.EncodeToString(hmacSHA256(sigKey, stringToSign))
	if !hmac.Equal([]byte(expectedSig), []byte(sig)) {
		return fmt.Errorf("presigned signature mismatch")
	}
	return nil
}

func parseByteRange(hdr string, totalLen int) (int, int, error) {
	if !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed range header")
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil || start < 0 || start >= totalLen {
		return 0, 0, fmt.Errorf("invalid range start")
	}
	end := totalLen - 1
	if parts[1] != "" {
		end, err = strconv.Atoi(parts[1])
		if err != nil || end < start {
			return 0, 0, fmt.Errorf("invalid range end")
		}
		if end >= totalLen {
			end = totalLen - 1
		}
	}
	return start, end, nil
}
