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
	"encoding/hex"
)

func AesEncrypt(value, key string) (string, error) {
	keyByte := paddingKey(key, aes.BlockSize)
	c, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}
	blockMode := cipher.NewCBCEncrypter(c, keyByte)
	origByte := []byte(value)
	origByte = pkcs5Padding(origByte, c.BlockSize())
	cryptoByte := make([]byte, len(origByte))
	blockMode.CryptBlocks(cryptoByte, origByte)
	return hex.EncodeToString(cryptoByte), nil
}

func AesDecrypt(value, key string) (string, error) {
	cryptoByte, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	keyByte := paddingKey(key, aes.BlockSize)
	c, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}
	blockMode := cipher.NewCBCDecrypter(c, keyByte)
	origData := make([]byte, len(cryptoByte))
	blockMode.CryptBlocks(origData, cryptoByte)
	origData = pkcs5UnPadding(origData)
	return string(origData), nil
}

func pkcs5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pkcs5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
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
