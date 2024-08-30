package utils

import (
	"crypto/md5"
)

const (
	DkvHashBit = 128
)

func HashKey(key string) []byte {
	hashed := md5.New()
	hashed.Write([]byte(key))
	hashBytes := hashed.Sum(nil) // This will give you a 16-byte (128-bit) slice
	return hashBytes
}
