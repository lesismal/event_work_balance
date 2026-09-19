// Package hpack implements HPACK (RFC 7541), the header compression HTTP/2
// uses: an Encoder for header blocks this side sends and a Decoder for those
// it receives. Each HTTP/2 connection keeps one of each, since both sides of
// a connection track the other's dynamic table.
package hpack

import (
	"errors"
)

// HeaderField is one header, with its name lowercased as HTTP/2 requires.
type HeaderField struct {
	Name, Value string
}

// size is the field's size as RFC 7541 section 4.1 counts it against a
// dynamic table.
func (f HeaderField) size() int { return len(f.Name) + len(f.Value) + 32 }

// DefaultTableSize is the dynamic table size both sides start from.
const DefaultTableSize = 4096

var (
	ErrInvalidIndex      = errors.New("hpack: invalid table index")
	ErrIntegerOverflow   = errors.New("hpack: integer overflow")
	ErrTruncated         = errors.New("hpack: truncated header block")
	ErrInvalidHuffman    = errors.New("hpack: invalid huffman encoding")
	ErrStringTooLong     = errors.New("hpack: string too long")
	ErrInvalidSizeUpdate = errors.New("hpack: invalid dynamic table size update")
)

// dynamicTable holds entries newest last, so index 1 of the dynamic part is
// the final element.
type dynamicTable struct {
	entries []HeaderField
	size    int
	maxSize int
}

func (t *dynamicTable) get(index int) (HeaderField, bool) {
	if index < 1 || index > len(t.entries) {
		return HeaderField{}, false
	}
	return t.entries[len(t.entries)-index], true
}

func (t *dynamicTable) add(f HeaderField) {
	t.size += f.size()
	t.entries = append(t.entries, f)
	t.evict()
}

func (t *dynamicTable) setMaxSize(n int) {
	t.maxSize = n
	t.evict()
}

// evict drops the oldest entries until the table fits. An entry larger than
// the whole table empties it, as RFC 7541 section 4.4 says.
func (t *dynamicTable) evict() {
	n := 0
	for t.size > t.maxSize && n < len(t.entries) {
		t.size -= t.entries[n].size()
		n++
	}
	if n == 0 {
		return
	}
	copy(t.entries, t.entries[n:])
	for i := len(t.entries) - n; i < len(t.entries); i++ {
		t.entries[i] = HeaderField{}
	}
	t.entries = t.entries[:len(t.entries)-n]
}

// field resolves a combined index: the static table first, then the dynamic.
func (t *dynamicTable) field(index uint64) (HeaderField, bool) {
	if index == 0 {
		return HeaderField{}, false
	}
	if index <= uint64(len(staticTable)) {
		return staticTable[index-1], true
	}
	rest := index - uint64(len(staticTable))
	if rest > uint64(len(t.entries)) {
		return HeaderField{}, false
	}
	return t.get(int(rest))
}

// Decoder decodes the header blocks one peer sends.
type Decoder struct {
	table dynamicTable
	// allowedMaxSize is the table size this side advertised in
	// SETTINGS_HEADER_TABLE_SIZE; the peer may shrink its use below it but
	// never grow past it.
	allowedMaxSize int
	// MaxStringLength bounds one name or value. Zero means no bound beyond
	// the block's own length.
	MaxStringLength int
}

// NewDecoder returns a decoder for a peer allowed a dynamic table of up to
// maxTableSize bytes.
func NewDecoder(maxTableSize int) *Decoder {
	d := &Decoder{allowedMaxSize: maxTableSize}
	d.table.maxSize = maxTableSize
	return d
}

// Decode decodes one complete header block, calling emit for each field in
// order. The dynamic table is updated as the block says, so every block a
// peer sends has to go through Decode, in order, even one whose fields are
// then ignored.
func (d *Decoder) Decode(block []byte, emit func(HeaderField) error) error {
	start := true
	for len(block) > 0 {
		b := block[0]
		switch {
		case b&0x80 != 0:
			// Indexed header field.
			index, rest, err := readInt(block, 7)
			if err != nil {
				return err
			}
			block = rest
			f, ok := d.table.field(index)
			if !ok {
				return ErrInvalidIndex
			}
			if err = emit(f); err != nil {
				return err
			}
		case b&0xc0 == 0x40:
			// Literal with incremental indexing.
			f, rest, err := d.readLiteral(block, 6)
			if err != nil {
				return err
			}
			block = rest
			d.table.add(f)
			if err = emit(f); err != nil {
				return err
			}
		case b&0xe0 == 0x20:
			// Dynamic table size update, only allowed before the first field.
			if !start {
				return ErrInvalidSizeUpdate
			}
			size, rest, err := readInt(block, 5)
			if err != nil {
				return err
			}
			if size > uint64(d.allowedMaxSize) {
				return ErrInvalidSizeUpdate
			}
			block = rest
			d.table.setMaxSize(int(size))
			continue
		default:
			// Literal without indexing (0000) or never indexed (0001).
			f, rest, err := d.readLiteral(block, 4)
			if err != nil {
				return err
			}
			block = rest
			if err = emit(f); err != nil {
				return err
			}
		}
		start = false
	}
	return nil
}

// readLiteral reads a literal field whose name index has an n-bit prefix.
func (d *Decoder) readLiteral(block []byte, n uint8) (HeaderField, []byte, error) {
	index, rest, err := readInt(block, n)
	if err != nil {
		return HeaderField{}, nil, err
	}
	var f HeaderField
	if index > 0 {
		named, ok := d.table.field(index)
		if !ok {
			return HeaderField{}, nil, ErrInvalidIndex
		}
		f.Name = named.Name
	} else if f.Name, rest, err = d.readString(rest); err != nil {
		return HeaderField{}, nil, err
	}
	if f.Value, rest, err = d.readString(rest); err != nil {
		return HeaderField{}, nil, err
	}
	return f, rest, nil
}

func (d *Decoder) readString(block []byte) (string, []byte, error) {
	if len(block) == 0 {
		return "", nil, ErrTruncated
	}
	huffman := block[0]&0x80 != 0
	length, rest, err := readInt(block, 7)
	if err != nil {
		return "", nil, err
	}
	if length > uint64(len(rest)) {
		return "", nil, ErrTruncated
	}
	raw := rest[:length]
	rest = rest[length:]
	if !huffman {
		if d.MaxStringLength > 0 && len(raw) > d.MaxStringLength {
			return "", nil, ErrStringTooLong
		}
		return string(raw), rest, nil
	}
	s, err := huffmanDecode(raw, d.MaxStringLength)
	return s, rest, err
}

// readInt reads an integer with an n-bit prefix (RFC 7541 section 5.1).
func readInt(block []byte, n uint8) (uint64, []byte, error) {
	if len(block) == 0 {
		return 0, nil, ErrTruncated
	}
	mask := uint64(1)<<n - 1
	value := uint64(block[0]) & mask
	block = block[1:]
	if value < mask {
		return value, block, nil
	}
	var shift uint
	for i, b := range block {
		value += uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, block[i+1:], nil
		}
		shift += 7
		// Nothing HTTP/2 encodes comes close to 2^56.
		if shift >= 56 {
			return 0, nil, ErrIntegerOverflow
		}
	}
	return 0, nil, ErrTruncated
}

// appendInt appends value with an n-bit prefix, OR-ing first into the first
// byte for the representation's leading bits.
func appendInt(dst []byte, first byte, n uint8, value uint64) []byte {
	mask := uint64(1)<<n - 1
	if value < mask {
		return append(dst, first|byte(value))
	}
	dst = append(dst, first|byte(mask))
	value -= mask
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

// Encoder encodes the header blocks this side sends. It is not safe for
// concurrent use: a connection encodes its blocks in the order it sends them.
type Encoder struct {
	table dynamicTable
	// pendingSize, when set, is a table size change the next block has to
	// announce, and minPending the smallest size taken since the last block,
	// which RFC 7541 section 4.2 requires announcing too.
	pendingSize bool
	minPending  int
}

// NewEncoder returns an encoder with the default table size.
func NewEncoder() *Encoder {
	e := &Encoder{}
	e.table.maxSize = DefaultTableSize
	return e
}

// SetMaxTableSize applies the peer's SETTINGS_HEADER_TABLE_SIZE. The encoder
// never uses more than the default table size, so a larger allowance changes
// nothing.
func (e *Encoder) SetMaxTableSize(n int) {
	if n > DefaultTableSize {
		n = DefaultTableSize
	}
	if n == e.table.maxSize {
		return
	}
	if !e.pendingSize || n < e.minPending {
		e.minPending = n
	}
	e.pendingSize = true
	e.table.setMaxSize(n)
}

// Begin starts a header block, announcing any pending table size change.
// Call it once per block, before the block's first AppendField.
func (e *Encoder) Begin(dst []byte) []byte {
	if !e.pendingSize {
		return dst
	}
	e.pendingSize = false
	if e.minPending < e.table.maxSize {
		dst = appendInt(dst, 0x20, 5, uint64(e.minPending))
	}
	return appendInt(dst, 0x20, 5, uint64(e.table.maxSize))
}

// AppendField appends one field, which must have a lowercase name.
func (e *Encoder) AppendField(dst []byte, name, value string, sensitive bool) []byte {
	nameIndex := 0
	if i, ok := staticPairIndex[HeaderField{name, value}]; ok {
		return appendInt(dst, 0x80, 7, uint64(i))
	}
	if i, ok := staticNameIndex[name]; ok {
		nameIndex = i
	}
	for i := len(e.table.entries) - 1; i >= 0; i-- {
		f := e.table.entries[i]
		if f.Name != name {
			continue
		}
		index := len(staticTable) + len(e.table.entries) - i
		if f.Value == value {
			return appendInt(dst, 0x80, 7, uint64(index))
		}
		if nameIndex == 0 {
			nameIndex = index
		}
	}
	// Index what is likely to repeat. Values that are secret or large would
	// either leak through the table or push out everything useful in it.
	f := HeaderField{name, value}
	if sensitive {
		dst = appendInt(dst, 0x10, 4, uint64(nameIndex))
	} else if f.size() > e.table.maxSize/2 {
		dst = appendInt(dst, 0x00, 4, uint64(nameIndex))
	} else {
		dst = appendInt(dst, 0x40, 6, uint64(nameIndex))
		e.table.add(f)
	}
	if nameIndex == 0 {
		dst = appendString(dst, name)
	}
	return appendString(dst, value)
}

// appendString appends s, Huffman coded when that is shorter.
func appendString(dst []byte, s string) []byte {
	if n := huffmanLen(s); n < len(s) {
		dst = appendInt(dst, 0x80, 7, uint64(n))
		return huffmanAppend(dst, s)
	}
	dst = appendInt(dst, 0x00, 7, uint64(len(s)))
	return append(dst, s...)
}

var (
	staticPairIndex = make(map[HeaderField]int, len(staticTable))
	staticNameIndex = make(map[string]int, len(staticTable))
)

func init() {
	for i := len(staticTable) - 1; i >= 0; i-- {
		f := staticTable[i]
		staticNameIndex[f.Name] = i + 1
		staticPairIndex[f] = i + 1
	}
	buildHuffmanTree()
}
