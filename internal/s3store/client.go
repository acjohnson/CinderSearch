package s3store

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

// ErrNotFound is returned when an object key does not exist in S3.
var ErrNotFound = errors.New("s3 object not found")

// Config configures the pure-Go S3 REST & SigV4 client.
type Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Timeout   time.Duration
}

// ObjectMeta describes metadata returned by HEAD or LIST operations.
type ObjectMeta struct {
	Key       string
	Size      int64
	ETag      string
	CRC32C    uint32
	UpdatedAt time.Time
}

// Client implements S3 object storage operations and AWS SigV4 presigned URL generation
// using only the Go standard library.
type Client struct {
	cfg        Config
	httpClient *http.Client
}

// NewClient creates an S3 REST client for AWS S3, MinIO, or CinderSearch's embedded S3 server.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.AccessKey == "" {
		cfg.AccessKey = "cindersearch-access"
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = "cindersearch-secret"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	if endpoint == "" {
		scheme := "https"
		if !cfg.UseSSL {
			scheme = "http"
		}
		endpoint = fmt.Sprintf("%s://s3.%s.amazonaws.com", scheme, cfg.Region)
	}
	cfg.Endpoint = endpoint

	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

// Endpoint returns the configured S3 endpoint URL.
func (c *Client) Endpoint() string {
	return c.cfg.Endpoint
}

// Bucket returns the configured S3 bucket name.
func (c *Client) Bucket() string {
	return c.cfg.Bucket
}

// PutObject uploads data to S3 with an explicit CRC32C checksum header.
func (c *Client) PutObject(ctx context.Context, key string, data []byte) (uint32, error) {
	sum := checksum.Sum32C(data)
	u := c.objectURL(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-amz-checksum-crc32c", checksum.FormatBase64(sum))

	c.signRequest(req, data, time.Now().UTC())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("s3 put %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("s3 put %s failed (%d): %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return sum, nil
}

// GetObject downloads an entire object from S3 and verifies its CRC32C if returned by the server.
func (c *Client) GetObject(ctx context.Context, key string) ([]byte, uint32, error) {
	u := c.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	c.signRequest(req, nil, time.Now().UTC())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, 0, fmt.Errorf("s3 get %s failed (%d): %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read s3 body %s: %w", key, err)
	}
	actualSum := checksum.Sum32C(data)
	if hdr := resp.Header.Get("x-amz-checksum-crc32c"); hdr != "" {
		expected, err := checksum.ParseBase64(hdr)
		if err != nil {
			return nil, 0, err
		}
		if expected != actualSum {
			return nil, 0, fmt.Errorf("%w: s3 object %s expected 0x%08x, got 0x%08x", checksum.ErrMismatch, key, expected, actualSum)
		}
	}
	return data, actualSum, nil
}

// GetObjectRange performs an HTTP byte-range read [start, end] (inclusive) from S3.
func (c *Client) GetObjectRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid byte range %d-%d", start, end)
	}
	u := c.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	c.signRequest(req, nil, time.Now().UTC())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 range get %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("s3 range get %s failed (%d): %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	expectedLen := int(end - start + 1)
	if resp.StatusCode == http.StatusOK && len(data) > expectedLen {
		return data[start : end+1], nil
	}
	return data, nil
}

// DeleteObject deletes an object from S3.
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	u := c.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	c.signRequest(req, nil, time.Now().UTC())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("s3 delete %s failed (%d)", key, resp.StatusCode)
	}
	return nil
}

type listBucketXML struct {
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
		ETag string `xml:"ETag"`
	} `xml:"Contents"`
}

// ListObjects lists object keys matching prefix in S3.
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	u := fmt.Sprintf("%s/%s?list-type=2&prefix=%s", c.cfg.Endpoint, url.PathEscape(c.cfg.Bucket), url.QueryEscape(prefix))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	c.signRequest(req, nil, time.Now().UTC())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("s3 list %s failed (%d)", prefix, resp.StatusCode)
	}

	var parsed listBucketXML
	if err := xml.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode s3 list xml: %w", err)
	}

	out := make([]ObjectMeta, 0, len(parsed.Contents))
	for _, item := range parsed.Contents {
		out = append(out, ObjectMeta{
			Key:  item.Key,
			Size: item.Size,
			ETag: strings.Trim(item.ETag, `"`),
		})
	}
	return out, nil
}

// PresignGetURL generates a standard AWS SigV4 presigned GET URL so clients can fetch
// exact document byte ranges directly from S3 with zero middleman proxy hop.
func (c *Client) PresignGetURL(key string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.cfg.Region)

	rawURL := c.objectURL(key)
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	q := parsed.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", c.cfg.AccessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(int(ttl.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")

	canonicalQuery := encodeCanonicalQuery(q)
	canonicalHeaders := "host:" + parsed.Host + "\n"
	canonicalRequest := strings.Join([]string{
		http.MethodGet,
		canonicalURI(parsed.EscapedPath()),
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

	sigKey := deriveSigningKey(c.cfg.SecretKey, dateStamp, c.cfg.Region, "s3")
	signature := hex.EncodeToString(hmacSHA256(sigKey, stringToSign))

	parsed.RawQuery = canonicalQuery + "&X-Amz-Signature=" + signature
	return parsed.String(), nil
}

func (c *Client) objectURL(key string) string {
	cleaned := strings.TrimLeft(key, "/")
	parts := strings.Split(cleaned, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return fmt.Sprintf("%s/%s/%s", c.cfg.Endpoint, url.PathEscape(c.cfg.Bucket), strings.Join(parts, "/"))
}

func (c *Client) signRequest(req *http.Request, payload []byte, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	payloadHash := sha256Hex(payload)

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	signedHeaderKeys := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if req.Header.Get("x-amz-checksum-crc32c") != "" {
		signedHeaderKeys = append(signedHeaderKeys, "x-amz-checksum-crc32c")
	}
	sort.Strings(signedHeaderKeys)

	var canonHeaders strings.Builder
	for _, k := range signedHeaderKeys {
		val := req.Host
		if k != "host" {
			val = strings.TrimSpace(req.Header.Get(k))
		}
		canonHeaders.WriteString(k)
		canonHeaders.WriteByte(':')
		canonHeaders.WriteString(val)
		canonHeaders.WriteByte('\n')
	}

	signedHeadersStr := strings.Join(signedHeaderKeys, ";")
	canonicalQuery := encodeCanonicalQuery(req.URL.Query())
	canonicalReq := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL.EscapedPath()),
		canonicalQuery,
		canonHeaders.String(),
		signedHeadersStr,
		payloadHash,
	}, "\n")

	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.cfg.Region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalReq)),
	}, "\n")

	sigKey := deriveSigningKey(c.cfg.SecretKey, dateStamp, c.cfg.Region, "s3")
	signature := hex.EncodeToString(hmacSHA256(sigKey, stringToSign))

	auth := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.cfg.AccessKey,
		scope,
		signedHeadersStr,
		signature,
	)
	req.Header.Set("Authorization", auth)
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func encodeCanonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, awsEscape(k)+"="+awsEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func awsEscape(s string) string {
	escaped := url.QueryEscape(s)
	return strings.ReplaceAll(escaped, "+", "%20")
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}
