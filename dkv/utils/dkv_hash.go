package utils

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

func HashKey(key string) uint64 {
	hashed := md5.New()
	hashed.Write([]byte(key))
	hashBytes := hashed.Sum(nil)
	hashHex := hex.EncodeToString(hashBytes)

	// Convert the first 16 characters (64 bits) of the hash to a uint64
	val, _ := strconv.ParseUint(hashHex[:16], 16, 64)
	return val
}
