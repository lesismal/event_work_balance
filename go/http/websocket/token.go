package websocket

import "strings"

func validToken(value string) bool {
	return validTokenBytes([]byte(value))
}

func validTokenBytes(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	const separators = "()<>@,;:\\\"/[]?={} \t"
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e || strings.ContainsRune(separators, rune(value[i])) {
			return false
		}
	}
	return true
}
