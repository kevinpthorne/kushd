package bootid

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadHostBootID(t *testing.T) {
	root := t.TempDir()
	if _, err := Read(root); err == nil {
		t.Fatal("missing host mount must not fall back to container boot ID")
	}
	path := filepath.Join(root, "proc/sys/kernel/random/boot_id")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	id := "11111111-1111-4111-8111-111111111111"
	if err := os.WriteFile(path, []byte(id+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := Read(root)
	if err != nil || got != id {
		t.Fatalf("Read = %q, %v", got, err)
	}
	if err := os.WriteFile(path, []byte("not-a-boot-id"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(root); err == nil {
		t.Fatal("malformed identity was accepted")
	}
}
