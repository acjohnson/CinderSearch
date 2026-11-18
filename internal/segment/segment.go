package segment

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

const indexMagic uint32 = 0x53494458 // "SIDX"

// Posting records a term occurrence in a document along with token positions
// to support exact match_phrase and slop proximity queries.
type Posting struct {
	DocIdx    uint32
	Freq      int
	Positions []int
}

// DocMeta stores per-document identity, S3 byte-range location, and CRC32C digest.
type DocMeta struct {
	DocID   string
	Deleted bool
	Seq     uint64
	Offset  int64
	Length  int64
	CRC32C  uint32
	RawJSON []byte // Kept in memtable; nil in S3 index file to avoid duplicating _source bytes.
}

// DocValues holds typed columnar field values for fast range filters, sorting, and aggs.
type DocValues struct {
	Keywords map[string][]string
	Numbers  map[string]float64
	Dates    map[string]int64 // Unix milliseconds
	Bools    map[string]bool
	Present  map[string]struct{}
}

// Segment is an immutable (or in-progress) search unit containing a positional
// inverted index, columnar DocValues, and packed _source byte offsets.
type Segment struct {
	ID            string
	IndexName     string
	ShardID       int
	MinSeq        uint64
	MaxSeq        uint64
	DocsObjectKey string
	IdxObjectKey  string
	IdxCRC32C     uint32
	DocsCRC32C    uint32

	Docs         []DocMeta
	DocIDToIdx   map[string]uint32
	Tombstones   map[string]uint64 // DocID -> delete Seq
	Postings     map[string][]Posting
	FieldLengths map[string][]int // field -> len(Docs) token lengths for BM25
	DocVals      []DocValues
	AvgFieldLen  map[string]float64
	LiveDocs     int
}

// InputDoc represents an unindexed document ready to be compiled into a Segment.
type InputDoc struct {
	DocID   string
	Seq     uint64
	Deleted bool
	RawJSON []byte
	CRC32C  uint32
}

// Analyze tokenizes text into lowercase alphanumeric terms preserving position order.
func Analyze(text string) []string {
	var tokens []string
	var cur strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur.WriteRune(unicode.ToLower(r))
		} else if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

// Build compiles a batch of InputDocs into a Segment and produces the packed
// raw _source binary blob (docsBlob) alongside its CRC32C and the serialized index bytes.
func Build(id, indexName string, shardID int, inputs []InputDoc, idxKey, docsKey string) (*Segment, []byte, []byte, error) {
	seg := &Segment{
		ID:            id,
		IndexName:     indexName,
		ShardID:       shardID,
		IdxObjectKey:  idxKey,
		DocsObjectKey: docsKey,
		DocIDToIdx:    make(map[string]uint32),
		Tombstones:    make(map[string]uint64),
		Postings:      make(map[string][]Posting),
		FieldLengths:  make(map[string][]int),
		AvgFieldLen:   make(map[string]float64),
	}

	// Deduplicate by DocID keeping highest Seq.
	latest := make(map[string]InputDoc, len(inputs))
	for _, in := range inputs {
		if seg.MinSeq == 0 || in.Seq < seg.MinSeq {
			seg.MinSeq = in.Seq
		}
		if in.Seq > seg.MaxSeq {
			seg.MaxSeq = in.Seq
		}
		prev, ok := latest[in.DocID]
		if !ok || in.Seq >= prev.Seq {
			latest[in.DocID] = in
		}
	}

	docIDs := make([]string, 0, len(latest))
	for id := range latest {
		docIDs = append(docIDs, id)
	}
	sort.Strings(docIDs)

	var docsBuf bytes.Buffer
	for _, docID := range docIDs {
		in := latest[docID]
		if in.Deleted {
			seg.Tombstones[docID] = in.Seq
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(in.RawJSON, &obj); err != nil {
			return nil, nil, nil, fmt.Errorf("invalid json for doc %s: %w", docID, err)
		}

		canonical := append([]byte(nil), in.RawJSON...)
		docCRC := in.CRC32C
		if docCRC == 0 {
			docCRC = checksum.Sum32C(canonical)
		} else if err := checksum.Verify(canonical, docCRC); err != nil {
			return nil, nil, nil, err
		}

		offset := int64(docsBuf.Len())
		docsBuf.Write(canonical)
		length := int64(len(canonical))

		docIdx := uint32(len(seg.Docs))
		seg.DocIDToIdx[docID] = docIdx
		seg.Docs = append(seg.Docs, DocMeta{
			DocID:  docID,
			Seq:    in.Seq,
			Offset: offset,
			Length: length,
			CRC32C: docCRC,
		})
		seg.LiveDocs++

		dv := DocValues{
			Keywords: make(map[string][]string),
			Numbers:  make(map[string]float64),
			Dates:    make(map[string]int64),
			Bools:    make(map[string]bool),
			Present:  make(map[string]struct{}),
		}

		flat := make(map[string][]any)
		flattenJSON("", obj, flat)

		allPos := 0
		for field, vals := range flat {
			dv.Present[field] = struct{}{}
			fieldPos := 0
			for _, v := range vals {
				switch typed := v.(type) {
				case string:
					dv.Keywords[field] = append(dv.Keywords[field], typed)
					if ts, ok := parseDateMillis(typed); ok {
						dv.Dates[field] = ts
					}
					toks := Analyze(typed)
					for _, tok := range toks {
						addPosting(seg.Postings, field, tok, docIdx, fieldPos)
						addPosting(seg.Postings, "_all", tok, docIdx, allPos)
						fieldPos++
						allPos++
					}
				case float64:
					dv.Numbers[field] = typed
					s := strconv.FormatFloat(typed, 'f', -1, 64)
					dv.Keywords[field] = append(dv.Keywords[field], s)
					addPosting(seg.Postings, field, s, docIdx, fieldPos)
					fieldPos++
				case bool:
					dv.Bools[field] = typed
					s := strconv.FormatBool(typed)
					dv.Keywords[field] = append(dv.Keywords[field], s)
					addPosting(seg.Postings, field, s, docIdx, fieldPos)
					fieldPos++
				}
			}
			ensureFieldLenSlice(seg.FieldLengths, field, int(docIdx)+1)
			seg.FieldLengths[field][docIdx] = fieldPos
		}
		ensureFieldLenSlice(seg.FieldLengths, "_all", int(docIdx)+1)
		seg.FieldLengths["_all"][docIdx] = allPos

		// Index _id as keyword for ids/term queries.
		dv.Keywords["_id"] = []string{docID}
		dv.Present["_id"] = struct{}{}

		seg.DocVals = append(seg.DocVals, dv)
	}

	// Pad all FieldLengths slices to len(seg.Docs) and compute averages for BM25.
	for field, lens := range seg.FieldLengths {
		ensureFieldLenSlice(seg.FieldLengths, field, len(seg.Docs))
		lens = seg.FieldLengths[field]
		var total int
		var nonZero int
		for _, l := range lens {
			if l > 0 {
				total += l
				nonZero++
			}
		}
		if nonZero > 0 {
			seg.AvgFieldLen[field] = float64(total) / float64(nonZero)
		} else {
			seg.AvgFieldLen[field] = 1
		}
	}

	docsBytes := docsBuf.Bytes()
	seg.DocsCRC32C = checksum.Sum32C(docsBytes)

	idxBytes, err := seg.MarshalIndex()
	if err != nil {
		return nil, nil, nil, err
	}
	return seg, idxBytes, docsBytes, nil
}

// MarshalIndex serializes the segment's index structures with a magic header and CRC32C checksum.
func (s *Segment) MarshalIndex() ([]byte, error) {
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(s); err != nil {
		return nil, fmt.Errorf("encode segment index: %w", err)
	}
	payload := body.Bytes()
	crc := checksum.Sum32C(payload)
	s.IdxCRC32C = crc

	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out[0:4], indexMagic)
	binary.BigEndian.PutUint32(out[4:8], crc)
	copy(out[8:], payload)
	return out, nil
}

// UnmarshalIndex verifies the segment index CRC32C and decodes the Segment.
func UnmarshalIndex(data []byte) (*Segment, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("corrupted segment index: too short (%d bytes)", len(data))
	}
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != indexMagic {
		return nil, fmt.Errorf("invalid segment index magic 0x%08x", magic)
	}
	expectedCRC := binary.BigEndian.Uint32(data[4:8])
	payload := data[8:]
	if err := checksum.Verify(payload, expectedCRC); err != nil {
		return nil, err
	}
	var seg Segment
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&seg); err != nil {
		return nil, fmt.Errorf("decode segment index: %w", err)
	}
	seg.IdxCRC32C = expectedCRC
	return &seg, nil
}

// TermKey builds the composite inverted index key for a field and term.
func TermKey(field, term string) string {
	return field + "\x00" + term
}

func addPosting(postings map[string][]Posting, field, term string, docIdx uint32, pos int) {
	key := TermKey(field, term)
	list := postings[key]
	if len(list) > 0 && list[len(list)-1].DocIdx == docIdx {
		list[len(list)-1].Freq++
		list[len(list)-1].Positions = append(list[len(list)-1].Positions, pos)
		postings[key] = list
		return
	}
	postings[key] = append(list, Posting{
		DocIdx:    docIdx,
		Freq:      1,
		Positions: []int{pos},
	})
}

func ensureFieldLenSlice(m map[string][]int, field string, targetLen int) {
	s := m[field]
	for len(s) < targetLen {
		s = append(s, 0)
	}
	m[field] = s
}

func flattenJSON(prefix string, v any, out map[string][]any) {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			next := k
			if prefix != "" {
				next = prefix + "." + k
			}
			flattenJSON(next, child, out)
		}
	case []any:
		for _, item := range val {
			flattenJSON(prefix, item, out)
		}
	case string, float64, bool:
		if prefix != "" {
			out[prefix] = append(out[prefix], val)
		}
	}
}

func parseDateMillis(s string) (int64, bool) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().UnixMilli(), true
		}
	}
	return 0, false
}
