package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/matan-yadgar/bifrost/internal/ci"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	wakeTask := flag.String("wake-task", "", "wake the original task by UUID using native Codex resume/queue")
	message := flag.String("message", "", "message for -wake-task")
	readTask := flag.String("read-task", "", "read recent history by task UUID or exact name, without resuming it")
	releaseTask := flag.String("release-task", "", "release CI holds for this task UUID")
	retryCI := flag.Bool("retry-ci", false, "clear a failed coordinator batch after investigation")
	flag.Parse()

	runtimeConfig, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	environment := githubapi.WithoutAuthTokens(os.Environ())
	if *wakeTask != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		agent := harness.NewCodex(runtimeConfig.Harness.Command, runtimeConfig.Harness.Args, environment)
		delivery, err := agent.WakeTask(ctx, *wakeTask, *message)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"task_id": *wakeTask, "delivery": delivery}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *readTask != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		agent := harness.NewCodex(runtimeConfig.Harness.Command, runtimeConfig.Harness.Args, environment)
		history, err := agent.ReadTask(ctx, *readTask)
		if err != nil {
			log.Fatal(err)
		}
		if err := writeTaskHistory(os.Stdout, history); err != nil {
			log.Fatal(err)
		}
		return
	}
	instanceLock, err := instance.Acquire(runtimeConfig.StateFile)
	if err != nil {
		log.Fatal(err)
	}
	logger, persistentLog, err := newPersistentLogger(runtimeConfig.LogDirectory, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := persistentLog.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close log file: %v\n", err)
		}
	}()
	defer func() {
		if err := instanceLock.Close(); err != nil {
			logger.Printf("release instance lock: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	executable, err := os.Executable()
	if err != nil {
		logger.Fatal(err)
	}
	absoluteConfig, err := filepath.Abs(*configPath)
	if err != nil {
		logger.Fatal(err)
	}
	var token string
	if *releaseTask == "" && !*retryCI {
		token, err = githubapi.AuthToken(ctx)
		if err != nil {
			logger.Fatal(err)
		}
	}
	githubClient := githubapi.NewClient(token)
	ciMonitor := ci.New(githubClient, runtimeConfig, environment, executable, absoluteConfig, logger)
	if *releaseTask != "" {
		if err := ciMonitor.ReleaseTask(*releaseTask); err != nil {
			logger.Fatal(err)
		}
		return
	}
	if *retryCI {
		if err := ciMonitor.Retry(ctx); err != nil {
			logger.Fatal(err)
		}
		return
	}

	var agentHarness harness.Harness
	switch runtimeConfig.Harness.Type {
	case "codex":
		agentHarness = harness.NewCodex(runtimeConfig.Harness.Command, runtimeConfig.Harness.Args, githubapi.WithoutAuthTokens(os.Environ()))
	default:
		logger.Fatalf("unsupported harness %q", runtimeConfig.Harness.Type)
	}
	repositories := make([]bridge.Repository, 0, len(runtimeConfig.Repositories))
	for _, repository := range runtimeConfig.Repositories {
		repositories = append(repositories, bridge.Repository{
			Name: repository.Name, Authors: repository.Authors, WorkingDirectory: repository.WorkingDirectory,
		})
	}
	monitor := bridge.New(githubClient, agentHarness, repositories, runtimeConfig.StateFile, runtimeConfig.DispatchTimeout, logger)

	poll := func() (bridge.CycleResult, error) {
		started := time.Now()
		var ciError error
		if runtimeConfig.MonitorCI {
			ciError = ciMonitor.RunOnce(ctx)
		}
		result, err := monitor.RunOnce(ctx)
		logger.Printf("poll completed: prs=%d threads=%d dispatches=%d deferred_prs=%d deferred_threads=%d duration=%s", result.PullRequests, result.Threads, result.Dispatches, result.Deferred, result.DeferredThreads, time.Since(started).Round(time.Millisecond))
		return result, errors.Join(ciError, err)
	}
	if *runOnce {
		result, err := poll()
		if err != nil {
			logger.Fatal(err)
		}
		if err := incompleteDeliveryError(result); err != nil {
			logger.Fatal(err)
		}
		return
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
			return
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

func writeTaskHistory(output io.Writer, raw json.RawMessage) error {
	var result struct {
		Revision string `json:"revision"`
		Thread   struct {
			ID     string          `json:"id"`
			Status json.RawMessage `json:"status"`
			Turns  []struct {
				ID     string            `json:"id"`
				Status string            `json:"status"`
				Items  []json.RawMessage `json:"items"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	turns := result.Thread.Turns
	if len(turns) > 8 {
		turns = turns[len(turns)-8:]
	}
	for index := range turns {
		var messages []json.RawMessage
		for _, item := range turns[index].Items {
			var header struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(item, &header); err != nil {
				return err
			}
			if header.Type == "userMessage" || header.Type == "agentMessage" {
				messages = append(messages, item)
			}
		}
		turns[index].Items = messages
	}
	result.Thread.Turns = turns
	return json.NewEncoder(output).Encode(result)
}
