package cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

// Stats reports NVMe read cache hit/miss/eviction and bit-rot healing metrics.
type Stats struct {
	Entries       int   `json:"entries"`
	UsedBytes     int64 `json:"used_bytes"`
	MaxBytes      int64 `json:"max_bytes"`
	Hits          int64 `json:"hits"`
	Misses        int64 `json:"misses"`
	Evictions     int64 `json:"evictions"`
	BitRotHealed  int64 `json:"bitrot_healed"`
}

type cacheEntry struct {
	key      string
	fileName string
	size     int64
	crc32c   uint32
	elem     *list.Element
}

type metaSidecar struct {
	Key    string `json:"key"`
	CRC32C uint32 `json:"crc32c"`
}

// NVMeCache implements a bounded-capacity local NVMe SSD read cache with LRU eviction
// and on-read / background-scrub CRC32C verification to prevent serving bit-rotted bytes.
type NVMeCache struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
	used     int64
	entries  map[string]*cacheEntry
	lru      *list.List // front = most recently used

	hits         atomic.Int64
	misses       atomic.Int64
	evictions    atomic.Int64
	bitRotHealed atomic.Int64
}

// NewNVMeCache initializes an NVMe-backed cache directory bounded by maxBytes.
func NewNVMeCache(dir string, maxBytes int64) (*NVMeCache, error) {
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024 // 256 MB default
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create nvme cache dir: %w", err)
	}
	return &NVMeCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[string]*cacheEntry),
		lru:      list.New(),
	}, nil
}

// Get retrieves cached bytes from NVMe if present and intact. If the local NVMe file
// suffers bit-rot (CRC32C mismatch), it automatically quarantines/deletes the corrupted
// entry and returns ErrMismatch so the caller re-hydrates from S3.
func (c *NVMeCache) Get(key string) ([]byte, bool, error) {
	c.mu.Lock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return nil, false, nil
	}
	filePath := filepath.Join(c.dir, entry.fileName)
	expectedCRC := entry.crc32c
	c.mu.Unlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		c.removeEntry(key)
		c.misses.Add(1)
		return nil, false, nil
	}

	if err := checksum.Verify(data, expectedCRC); err != nil {
		// Bit-rot detected on NVMe SSD: evict corrupted file immediately.
		c.removeEntry(key)
		c.bitRotHealed.Add(1)
		c.misses.Add(1)
		return nil, false, err
	}

	c.mu.Lock()
	if cur, exists := c.entries[key]; exists {
		c.lru.MoveToFront(cur.elem)
	}
	c.mu.Unlock()

	c.hits.Add(1)
	return data, true, nil
}

// GetOrFetch checks the NVMe cache first and falls back to loader on cache miss or
// NVMe bit-rot, automatically re-populating NVMe with verified bytes from S3.
func (c *NVMeCache) GetOrFetch(ctx context.Context, key string, expectedCRC uint32, loader func(context.Context) ([]byte, uint32, error)) ([]byte, bool, error) {
	data, hit, err := c.Get(key)
	if hit && err == nil {
		if expectedCRC != 0 {
			if verifyErr := checksum.Verify(data, expectedCRC); verifyErr == nil {
				return data, true, nil
			}
			c.removeEntry(key)
			c.bitRotHealed.Add(1)
		} else {
			return data, true, nil
		}
	}

	fetched, fetchedCRC, fetchErr := loader(ctx)
	if fetchErr != nil {
		return nil, false, fetchErr
	}
	if expectedCRC != 0 {
		if verifyErr := checksum.Verify(fetched, expectedCRC); verifyErr != nil {
			return nil, false, verifyErr
		}
		fetchedCRC = expectedCRC
	} else if fetchedCRC == 0 {
		fetchedCRC = checksum.Sum32C(fetched)
	}

	_ = c.Put(key, fetched, fetchedCRC)
	return fetched, false, nil
}

// Put writes a verified blob onto the NVMe cache and evicts LRU entries if over budget.
func (c *NVMeCache) Put(key string, data []byte, expectedCRC uint32) error {
	if expectedCRC == 0 {
		expectedCRC = checksum.Sum32C(data)
	} else if err := checksum.Verify(data, expectedCRC); err != nil {
		return err
	}

	fileName := hashKey(key)
	filePath := filepath.Join(c.dir, fileName)
	metaPath := filePath + ".meta"

	// Write atomically via temp file + rename so concurrent readers never observe partial files.
	tmpPath := filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write nvme cache file: %w", err)
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename nvme cache file: %w", err)
	}

	metaBytes, _ := json.Marshal(metaSidecar{Key: key, CRC32C: expectedCRC})
	_ = os.WriteFile(metaPath, metaBytes, 0o644)

	c.mu.Lock()
	defer c.mu.Unlock()

	if old, exists := c.entries[key]; exists {
		c.used -= old.size
		c.lru.Remove(old.elem)
		delete(c.entries, key)
	}

	size := int64(len(data))
	elem := c.lru.PushFront(key)
	c.entries[key] = &cacheEntry{
		key:      key,
		fileName: fileName,
		size:     size,
		crc32c:   expectedCRC,
		elem:     elem,
	}
	c.used += size

	for c.used > c.maxBytes && c.lru.Len() > 1 {
		tail := c.lru.Back()
		if tail == nil {
			break
		}
		evictKey := tail.Value.(string)
		c.removeLocked(evictKey)
		c.evictions.Add(1)
	}
	return nil
}

// CorruptFileForTest intentionally flips a byte in the NVMe cached file to verify bit-rot recovery.
func (c *NVMeCache) CorruptFileForTest(key string) error {
	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if !ok {
		return errors.New("cache key not present")
	}
	p := filepath.Join(c.dir, entry.fileName)
	data, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("empty cache file")
	}
	data[len(data)/2] ^= 0x7F
	return os.WriteFile(p, data, 0o644)
}

// Scrub verifies all NVMe cached files against their stored CRC32C digests, purging corrupted files.
// Returns the list of corrupted keys detected and evicted.
func (c *NVMeCache) Scrub() []string {
	c.mu.Lock()
	snapshot := make([]*cacheEntry, 0, len(c.entries))
	for _, e := range c.entries {
		snapshot = append(snapshot, e)
	}
	c.mu.Unlock()

	var corrupted []string
	for _, e := range snapshot {
		p := filepath.Join(c.dir, e.fileName)
		data, err := os.ReadFile(p)
		if err != nil || checksum.Verify(data, e.crc32c) != nil {
			corrupted = append(corrupted, e.key)
			c.removeEntry(e.key)
			c.bitRotHealed.Add(1)
		}
	}
	return corrupted
}

// Stats returns a thread-safe snapshot of NVMe cache usage and bit-rot recovery counts.
func (c *NVMeCache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries:      len(c.entries),
		UsedBytes:    c.used,
		MaxBytes:     c.maxBytes,
		Hits:         c.hits.Load(),
		Misses:       c.misses.Load(),
		Evictions:    c.evictions.Load(),
		BitRotHealed: c.bitRotHealed.Load(),
	}
}

func (c *NVMeCache) removeEntry(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key)
}

func (c *NVMeCache) removeLocked(key string) {
	entry, ok := c.entries[key]
	if !ok {
		return
	}
	c.used -= entry.size
	c.lru.Remove(entry.elem)
	delete(c.entries, key)
	_ = os.Remove(filepath.Join(c.dir, entry.fileName))
	_ = os.Remove(filepath.Join(c.dir, entry.fileName+".meta"))
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16]) + ".bin"
}
