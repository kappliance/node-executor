package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kappliance/node-executor/internal/install"
)

func TestCommands(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := install.Digest(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"copy", "install"} {
		t.Run(command, func(t *testing.T) {
			directory := t.TempDir()
			args := []string{command, "--directory=" + directory, "--name=helper", "--source=" + source}
			if command == "install" {
				args = append(args, "--sha256="+digest)
			}
			for range 2 {
				if err := run(context.Background(), args); err != nil {
					t.Fatal(err)
				}
			}
			bytes, err := os.ReadFile(filepath.Join(directory, "helper"))
			if err != nil || string(bytes) != "executable" {
				t.Fatalf("installed contents = %q, error = %v", bytes, err)
			}
		})
	}
}

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"exec", "echo"},
		{"install"},
		{"copy"},
		{"install", "--directory=x", "--name=tool", "--source=x"},
		{"copy", "--directory=x", "--name=tool", "--source=x", "--sha256=" + strings.Repeat("a", 64)},
		{"copy", "--directory=x", "--name=tool", "--source=x", "--previous-sha256=" + strings.Repeat("a", 64)},
		{"copy", "--directory=x", "--name=tool", "--source=x", "extra"},
		{"copy", "--unknown=x"},
		{"install", "--sha256"},
	} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("accepted invalid arguments: %v", args)
		}
	}
}

func TestGuardedUpgrade(t *testing.T) {
	directory, sources := t.TempDir(), t.TempDir()
	destination, source := filepath.Join(directory, "tool"), filepath.Join(sources, "source")
	if err := os.WriteFile(destination, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	previous, err := install.Digest(destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := install.Digest(source)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"install", "--directory=" + directory, "--name=tool", "--source=" + source, "--sha256=" + digest}
	if err := run(context.Background(), args); err == nil {
		t.Fatal("unguarded CLI upgrade accepted")
	}
	if err := run(context.Background(), append(args, "--previous-sha256="+previous)); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(destination)
	if err != nil || string(bytes) != "new" {
		t.Fatalf("upgraded contents = %q, error = %v", bytes, err)
	}
}
