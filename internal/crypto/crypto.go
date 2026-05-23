package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

const (
	PBKDF2Iterations = 100000
	KeyLength        = 32
	NonceSize        = 12
)

var (
	ErrCiphertextTooShort = errors.New("ciphertext too short")
	ErrDecryptionFailed   = errors.New("decryption failed")
)

// DeriveKey derives a 32-byte key from a passphrase and a salt using PBKDF2-SHA256.
func DeriveKey(passphrase string, salt []byte) []byte {
	return pbkdf2.Key([]byte(passphrase), salt, PBKDF2Iterations, KeyLength, sha256.New)
}

// GenerateRandomBytes generates cryptographically secure random bytes of the specified size.
func GenerateRandomBytes(size int) ([]byte, error) {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return b, nil
}

// ComputeHMAC computes the HMAC-SHA256 of a message using the passphrase as the key.
func ComputeHMAC(message []byte, passphrase string) []byte {
	mac := hmac.New(sha256.New, []byte(passphrase))
	mac.Write(message)
	return mac.Sum(nil)
}

// VerifyHMAC verifies if the provided HMAC matches the expected HMAC-SHA256.
func VerifyHMAC(message []byte, expectedMAC []byte, passphrase string) bool {
	computed := ComputeHMAC(message, passphrase)
	return hmac.Equal(computed, expectedMAC)
}

// EncryptGCM encrypts plaintext using AES-GCM with the derived key.
// The returned slice is structured as: [Nonce (12 bytes)][Ciphertext + Auth Tag]
func EncryptGCM(plaintext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create aes block cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcm: %w", err)
	}

	nonce, err := GenerateRandomBytes(NonceSize)
	if err != nil {
		return nil, err
	}

	// Seal appends the ciphertext and auth tag to dst (nonce in this case)
	ciphertext := aesgcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptGCM decrypts ciphertext using AES-GCM with the derived key.
// Expects ciphertext in format: [Nonce (12 bytes)][Ciphertext + Auth Tag]
func DecryptGCM(ciphertext []byte, key []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize {
		return nil, ErrCiphertextTooShort
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create aes block cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcm: %w", err)
	}

	nonce := ciphertext[:NonceSize]
	actualCiphertext := ciphertext[NonceSize:]

	plaintext, err := aesgcm.Open(nil, nonce, actualCiphertext, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}
