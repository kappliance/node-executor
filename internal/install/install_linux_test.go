package install

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func writeFile(t *testing.T, directory, name, contents string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func sum(contents string) string {
	hash := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(hash[:])
}

func assertFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	bytes, err := os.ReadFile(path)
	if err != nil || string(bytes) != contents {
		t.Fatalf("contents = %q, error = %v; want %q", bytes, err, contents)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode() != mode {
		t.Fatalf("unexpected file mode: %v, %v", info, err)
	}
}

func assertNoTemporary(t *testing.T, directory string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(directory, ".node-executor-*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files left behind: %v, %v", files, err)
	}
}

func TestInstallGuardAndNoop(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "new executable", 0o600)
	destination := writeFile(t, directory, "tool", "old executable", 0o600)
	oldDigest, newDigest := sum("old executable"), sum("new executable")
	for _, previous := range []string{"", sum("incorrect previous")} {
		if err := Install(context.Background(), directory, "tool", source, newDigest, previous); err == nil {
			t.Fatal("unguarded replacement succeeded")
		}
		assertFile(t, destination, "old executable", 0o600)
	}
	if err := Install(context.Background(), directory, "tool", source, newDigest, oldDigest); err != nil {
		t.Fatal(err)
	}
	assertFile(t, destination, "new executable", 0o755)
	before, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := Install(context.Background(), directory, "tool", source, newDigest, oldDigest); err != nil {
			t.Fatalf("repeated install: %v", err)
		}
	}
	after, err := os.Stat(destination)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("no-op replaced the destination inode")
	}
	assertNoTemporary(t, directory)
}

func TestSourceMismatchPreservesDestination(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "bad contents", 0o600)
	destination := writeFile(t, directory, "tool", "old contents", 0o755)
	before, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), directory, "tool", source, sum("new contents"), sum("old contents")); err == nil {
		t.Fatal("incorrect source digest accepted")
	}
	assertFile(t, destination, "old contents", 0o755)
	after, err := os.Stat(destination)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("failed install changed the destination inode")
	}
	assertNoTemporary(t, directory)
}

func TestNoopStillVerifiesSource(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "incorrect", 0o600)
	destination := writeFile(t, directory, "tool", "desired", 0o755)
	if err := Install(context.Background(), directory, "tool", source, sum("desired"), ""); err == nil {
		t.Fatal("incorrect source accepted for an existing desired executable")
	}
	assertFile(t, destination, "desired", 0o755)
	assertNoTemporary(t, directory)
}

type cancelWriter struct {
	cancel context.CancelFunc
}

func (w cancelWriter) Write(p []byte) (int, error) {
	w.cancel()
	return len(p), nil
}

func TestCancellationDuringCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := copyDigest(ctx, cancelWriter{cancel}, strings.NewReader(strings.Repeat("x", 128<<10)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("copy cancellation returned %v", err)
	}
}

func TestFirstInstallAndModeRepair(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "executable", 0o600)
	if err := Install(context.Background(), directory, "tool", source, sum("executable"), ""); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "tool")
	assertFile(t, destination, "executable", 0o755)
	if err := os.Chmod(destination, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), directory, "tool", source, sum("executable"), sum("wrong")); err == nil {
		t.Fatal("wrong previous digest accepted for mode repair")
	}
	assertFile(t, destination, "executable", 0o600)
	if err := Install(context.Background(), directory, "tool", source, sum("executable"), ""); err != nil {
		t.Fatal(err)
	}
	assertFile(t, destination, "executable", 0o755)
}

func TestInvalidNamesAndDigests(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "contents", 0o600)
	for _, name := range []string{"", ".", "..", "../escape", "a/b", "a\\b", "nul\x00name", lockName} {
		t.Run("name_"+name, func(t *testing.T) {
			if err := Install(context.Background(), directory, name, source, sum("contents"), ""); err == nil {
				t.Fatal("invalid name accepted")
			}
		})
	}
	for _, digest := range []string{"", strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("g", 64), "sha256:" + sum("contents")} {
		t.Run("digest_"+digest, func(t *testing.T) {
			if err := Install(context.Background(), directory, "tool", source, digest, ""); err == nil {
				t.Fatal("invalid source digest accepted")
			}
			if digest != "" {
				if err := Install(context.Background(), directory, "tool", source, sum("contents"), digest); err == nil {
					t.Fatal("invalid previous digest accepted")
				}
			}
		})
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 {
		t.Fatal("invalid arguments changed the installation directory")
	}
}

func TestRejectDestinationAndLockSpecialFiles(t *testing.T) {
	for _, name := range []string{"tool", lockName} {
		for _, kind := range []string{"symlink", "dangling", "directory", "fifo"} {
			t.Run(name+"_"+kind, func(t *testing.T) {
				directory := t.TempDir()
				source := writeFile(t, t.TempDir(), "source", "new", 0o600)
				victim := writeFile(t, directory, "victim", "untouched", 0o600)
				path := filepath.Join(directory, name)
				var err error
				switch kind {
				case "symlink":
					err = os.Symlink("victim", path)
				case "dangling":
					err = os.Symlink("absent", path)
				case "directory":
					err = os.Mkdir(path, 0o700)
				case "fifo":
					err = syscall.Mkfifo(path, 0o600)
				}
				if err != nil {
					t.Fatal(err)
				}
				before, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := Install(context.Background(), directory, "tool", source, sum("new"), ""); err == nil {
					t.Fatal("special file accepted")
				}
				after, err := os.Lstat(path)
				if err != nil || !os.SameFile(before, after) {
					t.Fatal("special file was replaced")
				}
				assertFile(t, victim, "untouched", 0o600)
			})
		}
	}
}

func TestRejectSourceSpecialAndOversizedFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "directory", "fifo", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			directory, sources := t.TempDir(), t.TempDir()
			source := filepath.Join(sources, "source")
			var err error
			switch kind {
			case "symlink":
				writeFile(t, sources, "real", "contents", 0o600)
				err = os.Symlink("real", source)
			case "directory":
				err = os.Mkdir(source, 0o700)
			case "fifo":
				err = syscall.Mkfifo(source, 0o600)
			case "oversized":
				writeFile(t, sources, "source", "", 0o600)
				err = os.Truncate(source, maxSize+1)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := Install(context.Background(), directory, "tool", source, sum("contents"), ""); err == nil {
				t.Fatal("invalid source accepted")
			}
			if _, err := Digest(source); err == nil {
				t.Fatal("invalid digest source accepted")
			}
		})
	}
}

func TestMissingExpectedPrevious(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "new", 0o600)
	if err := Install(context.Background(), directory, "tool", source, sum("new"), sum("old")); err == nil {
		t.Fatal("missing expected previous executable accepted")
	}
	if _, err := os.Stat(filepath.Join(directory, "tool")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("destination was created")
	}
}

func TestCancellation(t *testing.T) {
	directory := t.TempDir()
	source := writeFile(t, t.TempDir(), "source", "new", 0o600)
	destination := writeFile(t, directory, "tool", "old", 0o755)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Install(ctx, directory, "tool", source, sum("new"), sum("old")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation returned %v", err)
	}
	assertFile(t, destination, "old", 0o755)
	lock, err := os.OpenFile(filepath.Join(directory, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := Install(ctx, directory, "tool", source, sum("new"), sum("old")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait cancellation returned %v", err)
	}
	assertFile(t, destination, "old", 0o755)
	assertNoTemporary(t, directory)
}

func TestConcurrentInstallers(t *testing.T) {
	directory := t.TempDir()
	destination := writeFile(t, directory, "tool", "old", 0o755)
	const workers = 12
	var group sync.WaitGroup
	results := make(chan error, workers)
	start := make(chan struct{})
	for i := range workers {
		contents := strings.Repeat(string(rune('a'+i)), 4096)
		source := writeFile(t, t.TempDir(), "source", contents, 0o600)
		group.Go(func() {
			<-start
			results <- Install(context.Background(), directory, "tool", source, sum(contents), sum("old"))
		})
	}
	close(start)
	group.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("guarded concurrent replacements succeeded %d times; want 1", succeeded)
	}
	bytes, err := os.ReadFile(destination)
	if err != nil || len(bytes) != 4096 || string(bytes) != strings.Repeat(string(bytes[0]), 4096) {
		t.Fatalf("destination is not one complete executable: length %d, error %v", len(bytes), err)
	}
	assertNoTemporary(t, directory)
}
