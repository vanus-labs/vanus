// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"io"
)

const nonceSize = 12

func AESEncrypt(value, key string) (string, error) {
	keyByte := paddingKey(key, aes.BlockSize)
	// Get the AES block cipher
	aesBlock, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}

	// Get the GCM cipher mode
	gcm, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	_, _ = io.CopyN(&out, rand.Reader, nonceSize)
	nonce := out.Bytes()
	cryptoByte := gcm.Seal(nil, nonce, []byte(value), nil)
	out.Write(cryptoByte)
	return hex.EncodeToString(out.Bytes()), nil
}

func AESDecrypt(value, key string) (string, error) {
	cryptoByte, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	keyByte := paddingKey(key, aes.BlockSize)
	// Get the AES block cipher
	aesBlock, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}

	// Get the GCM cipher mode
	gcm, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return "", err
	}
	nonce := cryptoByte[:nonceSize]
	ciphertext := cryptoByte[nonceSize:]
	origData, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(origData), nil
}

// paddingKey lt size append 0, gt size will discard.
func paddingKey(key string, size int) []byte {
	for len(key) < size {
		key += "0"
	}
	keyByte := []byte(key)
	if len(keyByte) > size {
		keyByte = keyByte[:size]
	}
	return keyByte
}
