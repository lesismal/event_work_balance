package hpack

// huffmanNode is a node of the decoding tree: a leaf when children is zero.
type huffmanNode struct {
	children [2]uint16
	sym      uint8
}

// huffmanTree is the decoding tree; the root is element 0. 256 leaves need
// 255 inner nodes.
var huffmanTree []huffmanNode

func buildHuffmanTree() {
	huffmanTree = make([]huffmanNode, 1, 512)
	for sym := 0; sym < 256; sym++ {
		code, length := huffmanCodes[sym], huffmanCodeLen[sym]
		node := 0
		for i := int(length) - 1; i >= 0; i-- {
			bit := (code >> uint(i)) & 1
			next := huffmanTree[node].children[bit]
			if next == 0 {
				huffmanTree = append(huffmanTree, huffmanNode{})
				next = uint16(len(huffmanTree) - 1)
				huffmanTree[node].children[bit] = next
			}
			node = int(next)
		}
		huffmanTree[node].sym = uint8(sym)
	}
}

// huffmanDecode decodes s. Padding must be at most seven bits, all ones, the
// start of EOS (RFC 7541 section 5.2); EOS itself never decodes.
func huffmanDecode(s []byte, maxLen int) (string, error) {
	out := make([]byte, 0, len(s)*8/5)
	node := 0
	// depth counts the bits since the last symbol, and ones whether they
	// were all ones, which decides whether they are valid padding.
	depth, ones := 0, true
	for _, b := range s {
		for i := 7; i >= 0; i-- {
			bit := (b >> uint(i)) & 1
			next := huffmanTree[node].children[bit]
			if next == 0 {
				// Only the path of 30 ones, EOS, runs off the tree.
				return "", ErrInvalidHuffman
			}
			node = int(next)
			depth++
			ones = ones && bit == 1
			if huffmanTree[node].children[0] == 0 {
				out = append(out, huffmanTree[node].sym)
				if maxLen > 0 && len(out) > maxLen {
					return "", ErrStringTooLong
				}
				node, depth, ones = 0, 0, true
			}
		}
	}
	if depth > 7 || !ones {
		return "", ErrInvalidHuffman
	}
	return string(out), nil
}

// huffmanLen is the length of s once Huffman coded.
func huffmanLen(s string) int {
	bits := 0
	for i := 0; i < len(s); i++ {
		bits += int(huffmanCodeLen[s[i]])
	}
	return (bits + 7) / 8
}

// huffmanAppend appends s Huffman coded, padded with ones.
func huffmanAppend(dst []byte, s string) []byte {
	var acc uint64
	var n uint
	for i := 0; i < len(s); i++ {
		c := s[i]
		acc = acc<<huffmanCodeLen[c] | uint64(huffmanCodes[c])
		n += uint(huffmanCodeLen[c])
		for n >= 8 {
			n -= 8
			dst = append(dst, byte(acc>>n))
		}
	}
	if n > 0 {
		dst = append(dst, byte(acc<<(8-n))|byte(0xff>>n))
	}
	return dst
}
