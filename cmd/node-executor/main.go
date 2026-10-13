package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/kappliance/node-executor/internal/install"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "node-executor:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 || (args[0] != "install" && args[0] != "copy") {
		return errors.New("expected install or copy subcommand")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	directory := flags.String("directory", "", "Installation directory")
	name := flags.String("name", "", "Executable basename")
	source := flags.String("source", "", "Source executable")
	var digest, previous string
	if args[0] == "install" {
		flags.StringVar(&digest, "sha256", "", "Source SHA256")
		flags.StringVar(&previous, "previous-sha256", "", "Expected existing SHA256")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *directory == "" || *name == "" || *source == "" || (args[0] == "install" && digest == "") {
		return errors.New("directory, name, source and install SHA256 are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if args[0] == "copy" {
		var err error
		digest, err = install.Digest(*source)
		if err != nil {
			return fmt.Errorf("hash source: %w", err)
		}
	}
	return install.Install(ctx, *directory, *name, *source, digest, previous)
}
