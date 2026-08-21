package main

import (
	"context"
	"flag"
	"fmt"
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
)

func main() {
	defaultConfig, err := config.DefaultPath()
	if err != nil {
		log.Fatal(err)
	}
	configPath := flag.String("config", defaultConfig, "path to bifrost JSON config")
	runOnce := flag.Bool("once", false, "poll once and exit")
	flag.Parse()

	runtimeConfig, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	instanceLock, err := instance.Acquire(runtimeConfig.StateFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := instanceLock.Close(); err != nil {
			log.Printf("release instance lock: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	token, err := githubapi.AuthToken(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var agentHarness harness.Harness
	switch runtimeConfig.Harness.Type {
	case "codex":
		agentHarness = harness.NewCodex(runtimeConfig.Harness.Command, runtimeConfig.Harness.Args, githubapi.WithoutAuthTokens(os.Environ()))
	default:
		log.Fatalf("unsupported harness %q", runtimeConfig.Harness.Type)
	}
	if err := bridge.ImportLegacyMappings(runtimeConfig.StateFile, runtimeConfig.LegacyMappingDirectory, runtimeConfig.LegacyMappingFile, agentHarness.Name()); err != nil {
		log.Fatal(err)
	}
	repositories := make([]bridge.Repository, 0, len(runtimeConfig.Repositories))
	for _, repository := range runtimeConfig.Repositories {
		repositories = append(repositories, bridge.Repository{
			Name: repository.Name, Authors: repository.Authors, WorkingDirectory: repository.WorkingDirectory,
		})
	}
	monitor := bridge.New(githubapi.NewClient(token), agentHarness, repositories, runtimeConfig.StateFile, runtimeConfig.DispatchTimeout, log.Default())

	poll := func() (bridge.CycleResult, error) {
		started := time.Now()
		result, err := monitor.RunOnce(ctx)
		log.Printf("poll completed: prs=%d threads=%d dispatches=%d deferred_prs=%d deferred_threads=%d duration=%s", result.PullRequests, result.Threads, result.Dispatches, result.Deferred, result.DeferredThreads, time.Since(started).Round(time.Millisecond))
		return result, err
	}
	if *runOnce {
		result, err := poll()
		if err != nil {
			log.Fatal(err)
		}
		if err := incompleteDeliveryError(result); err != nil {
			log.Fatal(err)
		}
		return
	}

	log.Printf("bifrost started: interval=%s dispatch_timeout=%s harness=%s", runtimeConfig.PollInterval, runtimeConfig.DispatchTimeout, agentHarness.Name())
	if _, err := poll(); err != nil {
		log.Printf("poll failed: %v", err)
	}
	ticker := time.NewTicker(runtimeConfig.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "bifrost stopped")
			return
		case <-ticker.C:
			if _, err := poll(); err != nil {
				log.Printf("poll failed: %v", err)
			}
		}
	}
}

func incompleteDeliveryError(result bridge.CycleResult) error {
	if result.Deferred == 0 && result.DeferredThreads == 0 {
		return nil
	}
	return fmt.Errorf("incomplete delivery: %d pull requests and %d review threads were deferred", result.Deferred, result.DeferredThreads)
}
