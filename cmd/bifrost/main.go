package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/matan-yadgar/bifrost/internal/bridge"
	"github.com/matan-yadgar/bifrost/internal/config"
	githubapi "github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
	"github.com/matan-yadgar/bifrost/internal/instance"
	"github.com/matan-yadgar/bifrost/internal/logfile"
)

func main() {
	defaultConfig, err := config.DefaultPath()
	if err != nil {
		log.Fatal(err)
	}
	configPath := flag.String("config", defaultConfig, "path to bifrost JSON config")
	runOnce := flag.Bool("once", false, "poll once and exit")
	serveUI := flag.Bool("ui", false, "serve the local Bifrost control UI")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *serveUI && *runOnce {
		log.Fatal("-ui and -once cannot be used together")
	}
	if *serveUI {
		err = runUI(ctx, *configPath, os.Stderr)
	} else {
		err = runDaemon(ctx, *configPath, *runOnce, os.Stderr)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func runDaemon(ctx context.Context, configPath string, runOnce bool, terminal io.Writer) error {
	runtimeConfig, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger, persistentLog, err := newPersistentLogger(runtimeConfig.LogDirectory, terminal)
	if err != nil {
		return err
	}
	defer func() {
		if err := persistentLog.Close(); err != nil {
			_, _ = fmt.Fprintf(terminal, "close log file: %v\n", err)
		}
	}()
	return runPoller(ctx, runtimeConfig, runOnce, logger)
}

func runPoller(ctx context.Context, runtimeConfig config.Runtime, runOnce bool, logger *log.Logger) error {
	environment := githubapi.WithoutAuthTokens(os.Environ())
	agentHarness, err := harness.New(runtimeConfig.Harness.Type, runtimeConfig.Harness.Command, runtimeConfig.Harness.Args, environment)
	if err != nil {
		return err
	}
	instanceLock, err := instance.Acquire(runtimeConfig.StateFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := instanceLock.Close(); err != nil {
			logger.Printf("release instance lock: %v", err)
		}
	}()
	token, err := githubapi.AuthToken(ctx)
	if err != nil {
		return err
	}
	githubClient := githubapi.NewClient(token)

	repositories := make([]bridge.Repository, 0, len(runtimeConfig.Repositories))
	for _, repository := range runtimeConfig.Repositories {
		repositories = append(repositories, bridge.Repository{
			Name: repository.Name, Authors: repository.Authors, WorkingDirectory: repository.WorkingDirectory,
		})
	}
	monitor := bridge.New(githubClient, agentHarness, repositories, runtimeConfig.StateFile, runtimeConfig.DispatchTimeout, runtimeConfig.MonitorCI, logger)

	poll := func() (bridge.CycleResult, error) {
		started := time.Now()
		result, err := monitor.RunOnce(ctx)
		logger.Printf("poll completed: prs=%d threads=%d dispatches=%d deferred_prs=%d deferred_threads=%d duration=%s", result.PullRequests, result.Threads, result.Dispatches, result.Deferred, result.DeferredThreads, time.Since(started).Round(time.Millisecond))
		return result, err
	}
	if runOnce {
		result, err := poll()
		if err != nil {
			return err
		}
		if err := incompleteDeliveryError(result); err != nil {
			return err
		}
		return nil
	}

	logger.Printf("bifrost started: interval=%s dispatch_timeout=%s harness=%s log_directory=%s", runtimeConfig.PollInterval, runtimeConfig.DispatchTimeout, agentHarness.Name(), runtimeConfig.LogDirectory)
	if _, err := poll(); err != nil {
		logger.Printf("poll failed: %v", err)
	}
	ticker := time.NewTicker(runtimeConfig.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Print("bifrost stopped")
			return nil
		case <-ticker.C:
			if _, err := poll(); err != nil {
				logger.Printf("poll failed: %v", err)
			}
		}
	}
}

func newPersistentLogger(directory string, terminal io.Writer) (*log.Logger, io.WriteCloser, error) {
	persistentLog, err := logfile.New(directory)
	if err != nil {
		return nil, nil, err
	}
	return log.New(logfile.NewOutput(terminal, persistentLog), "", log.LstdFlags), persistentLog, nil
}

func incompleteDeliveryError(result bridge.CycleResult) error {
	if result.Deferred == 0 && result.DeferredThreads == 0 {
		return nil
	}
	return fmt.Errorf("incomplete delivery: %d pull requests and %d review threads were deferred", result.Deferred, result.DeferredThreads)
}
