package crypto

import (
	"bytes"
	"testing"
)

func TestDeriveKey(t *testing.T) {
	passphrase := "super_secret_passphrase"
	salt1 := []byte("salt_one")
	salt2 := []byte("salt_two")

	key1 := DeriveKey(passphrase, salt1, 0)
	key2 := DeriveKey(passphrase, salt1, 0)
	key3 := DeriveKey(passphrase, salt2, 0)

	if len(key1) != KeyLength {
		t.Errorf("expected key length %d, got %d", KeyLength, len(key1))
	}

	if !bytes.Equal(key1, key2) {
		t.Error("derived keys with same passphrase and salt should be equal")
	}

	if bytes.Equal(key1, key3) {
		t.Error("derived keys with different salts should not be equal")
	}
}

func TestHMAC(t *testing.T) {
	passphrase := "secret_key"
	salt := []byte("hmac_salt")
	message := []byte("hello world")
	derivedKey := DeriveKey(passphrase, salt, 0)
	wrongKey := DeriveKey("wrong_key", salt, 0)

	mac := ComputeHMAC(message, derivedKey)
	if len(mac) != 32 { // SHA-256 HMAC is 32 bytes
		t.Errorf("expected HMAC length 32, got %d", len(mac))
	}

	if !VerifyHMAC(message, mac, derivedKey) {
		t.Error("HMAC verification should succeed")
	}

	// Tampered message
	if VerifyHMAC([]byte("hello world!"), mac, derivedKey) {
		t.Error("HMAC verification should fail for tampered message")
	}

	// Wrong key
	if VerifyHMAC(message, mac, wrongKey) {
		t.Error("HMAC verification should fail with wrong key")
	}
}

func TestAESGCM(t *testing.T) {
	key := DeriveKey("my_secret_pass", []byte("some_salt"), 0)
	plaintext := []byte("this is highly confidential data packet")

	ciphertext, err := EncryptGCM(plaintext, key)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	if len(ciphertext) <= NonceSize {
		t.Fatalf("ciphertext too short: %d", len(ciphertext))
	}

	decrypted, err := DecryptGCM(ciphertext, key)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("decrypted content does not match plaintext. Expected: %s, got: %s", plaintext, decrypted)
	}

	// Test tampering
	ciphertext[NonceSize] ^= 0xFF // Flip bits in ciphertext
	_, err = DecryptGCM(ciphertext, key)
	if err == nil {
		t.Error("decryption should have failed for tampered ciphertext")
	}
}
