package common

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

func CalcFileSha256(file *os.File) ([]byte, error) {
	hash := sha256.New()
	_, err := io.Copy(hash, file)
	if err != nil {
		return nil, err
	}
	return hash.Sum(nil), nil
}

func CalcFilePathSha256(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %v", filePath, err)
	}
	defer file.Close()
	return CalcFileSha256(file)
}
