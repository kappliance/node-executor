// Package install atomically installs verified executables into a directory.
package install

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	maxSize  = 256 << 20
	lockName = ".node-executor.lock"
)

// Install verifies source and atomically replaces name under directory.
// Replacing different contents requires their expected previous digest.
func Install(ctx context.Context, directory, name, source, desiredSHA256, expectedPreviousSHA256 string) error {
	if name == "" || name == "." || name == ".." || name == lockName || strings.ContainsAny(name, "/\\\x00") {
		return errors.New("invalid executable name")
	}
	if !validDigest(desiredSHA256) || (expectedPreviousSHA256 != "" && !validDigest(expectedPreviousSHA256)) {
		return errors.New("SHA256 must be exactly 64 lowercase hexadecimal characters")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	src, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer src.Close()
	if err := regularFile(src); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return fmt.Errorf("open installation directory: %w", err)
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	lock, err := openAt(dir, lockName, syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		return fmt.Errorf("open installation lock: %w", err)
	}
	defer lock.Close()
	if err := regularFile(lock); err != nil {
		return fmt.Errorf("installation lock: %w", err)
	}
	if err := acquireLock(ctx, lock); err != nil {
		return err
	}
	lockedInfo, err := lock.Stat()
	if err != nil {
		return err
	}
	lockInfo, err := root.Lstat(lockName)
	if err != nil || !os.SameFile(lockedInfo, lockInfo) {
		return errors.New("installation lock changed")
	}
	if _, err := checkDestination(ctx, dir, name, desiredSHA256, expectedPreviousSHA256); err != nil {
		return err
	}
	temporary := ".node-executor-" + rand.Text() + ".tmp"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary executable: %w", err)
	}
	defer root.Remove(temporary)
	defer file.Close()
	digest, err := copyDigest(ctx, file, src)
	if err != nil {
		return fmt.Errorf("copy source: %w", err)
	}
	if digest != desiredSHA256 {
		return errors.New("source SHA256 mismatch")
	}
	if err := file.Chmod(0o755); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync executable: %w", err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	unchanged, err := checkDestination(ctx, dir, name, desiredSHA256, expectedPreviousSHA256)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !unchanged {
		if err := root.Rename(temporary, name); err != nil {
			return fmt.Errorf("replace executable: %w", err)
		}
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync installation directory: %w", err)
	}
	return ctx.Err()
}

// Digest returns the SHA256 of a regular source file of at most 256 MiB.
func Digest(source string) (string, error) {
	file, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if err := regularFile(file); err != nil {
		return "", err
	}
	return copyDigest(context.Background(), io.Discard, file)
}

func validDigest(digest string) bool {
	return len(digest) == 64 && strings.Trim(digest, "0123456789abcdef") == ""
}

func regularFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("must be a regular file")
	}
	if info.Size() > maxSize {
		return errors.New("file exceeds 256 MiB")
	}
	return nil
}

// Root.OpenFile follows symlinks; openat preserves the kernel's O_NOFOLLOW check.
func openAt(dir *os.File, name string, flags int) (*os.File, error) {
	fd, err := syscall.Openat(int(dir.Fd()), name, flags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func acquireLock(ctx context.Context, file *os.File) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("lock installation directory: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func checkDestination(ctx context.Context, dir *os.File, name, desired, previous string) (bool, error) {
	file, err := openAt(dir, name, syscall.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		if previous != "" {
			return false, errors.New("expected previous executable is missing")
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("open destination: %w", err)
	}
	defer file.Close()
	if err := regularFile(file); err != nil {
		return false, fmt.Errorf("destination: %w", err)
	}
	digest, err := copyDigest(ctx, io.Discard, file)
	if err != nil {
		return false, fmt.Errorf("hash destination: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if digest == desired && info.Mode() == 0o755 {
		return true, nil
	}
	if (previous != "" && digest != previous) || (previous == "" && digest != desired) {
		return false, errors.New("destination SHA256 does not match expected previous SHA256")
	}
	return false, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func copyDigest(ctx context.Context, dst io.Writer, src io.Reader) (string, error) {
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, hash), io.LimitReader(contextReader{ctx, src}, maxSize+1))
	if err != nil {
		return "", err
	}
	if n > maxSize {
		return "", errors.New("file exceeds 256 MiB")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
