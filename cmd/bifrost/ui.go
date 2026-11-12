package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/matan-yadgar/bifrost/internal/config"
	"github.com/matan-yadgar/bifrost/internal/prmanager"
)

const controlUIAddress = "127.0.0.1:7420"

type controlUI struct {
	configPath          string
	logger              *log.Logger
	token               string
	logs                *liveLogs
	run                 func(context.Context) error
	prManagerStatus     func() prmanager.Status
	activatePRManager   func() error
	deactivatePRManager func()

	mutex     sync.Mutex
	active    bool
	stopping  bool
	lastError string
	cancel    context.CancelFunc
	done      chan struct{}
}

type controlUIStatus struct {
	Active    bool             `json:"active"`
	Stopping  bool             `json:"stopping"`
	LastError string           `json:"last_error,omitempty"`
	PRManager prmanager.Status `json:"pr_manager"`
}

type liveLogs struct {
	mutex       sync.Mutex
	lines       []string
	subscribers map[chan string]struct{}
}

func newLiveLogs() *liveLogs {
	return &liveLogs{subscribers: make(map[chan string]struct{})}
}

func (logs *liveLogs) Write(data []byte) (int, error) {
	line := string(data)
	logs.mutex.Lock()
	defer logs.mutex.Unlock()
	logs.lines = append(logs.lines, line)
	if len(logs.lines) > 500 {
		logs.lines = logs.lines[len(logs.lines)-500:]
	}
	for subscriber := range logs.subscribers {
		select {
		case subscriber <- line:
		default:
		}
	}
	return len(data), nil
}

func (logs *liveLogs) snapshot() []string {
	logs.mutex.Lock()
	defer logs.mutex.Unlock()
	return append([]string(nil), logs.lines...)
}

func (logs *liveLogs) subscribe() (<-chan string, func()) {
	subscriber := make(chan string, 128)
	logs.mutex.Lock()
	logs.subscribers[subscriber] = struct{}{}
	logs.mutex.Unlock()
	return subscriber, func() {
		logs.mutex.Lock()
		delete(logs.subscribers, subscriber)
		logs.mutex.Unlock()
	}
}

func runUI(ctx context.Context, configPath string, terminal io.Writer) error {
	runtimeConfig, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logs := newLiveLogs()
	logger, persistentLog, err := newPersistentLogger(runtimeConfig.LogDirectory, io.MultiWriter(terminal, logs))
	if err != nil {
		return err
	}
	defer func() {
		if err := persistentLog.Close(); err != nil {
			_, _ = fmt.Fprintf(terminal, "close log file: %v\n", err)
		}
	}()
	token, err := newControlToken()
	if err != nil {
		return err
	}
	manager := newPRManager(runtimeConfig, logger)
	ui := &controlUI{
		configPath:          configPath,
		logger:              logger,
		token:               token,
		logs:                logs,
		prManagerStatus:     manager.Status,
		activatePRManager:   manager.Activate,
		deactivatePRManager: manager.Deactivate,
		run: func(runContext context.Context) error {
			return runPoller(runContext, runtimeConfig, false, logger, manager)
		},
	}
	listener, err := net.Listen("tcp", controlUIAddress)
	if err != nil {
		return fmt.Errorf("listen for control UI on http://%s: %w", controlUIAddress, err)
	}
	defer func() { _ = listener.Close() }()
	server := &http.Server{Handler: ui.handler()}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	logger.Printf("Bifrost control UI available at http://%s", controlUIAddress)
	ui.activate()

	select {
	case <-ctx.Done():
		ui.deactivate()
		<-ui.finished()
		return server.Shutdown(context.Background())
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func newControlToken() (string, error) {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate control UI token: %w", err)
	}
	return hex.EncodeToString(data), nil
}

func (ui *controlUI) activate() {
	ui.mutex.Lock()
	if ui.active || ui.stopping {
		ui.mutex.Unlock()
		return
	}
	runContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ui.active = true
	ui.lastError = ""
	ui.cancel = cancel
	ui.done = done
	ui.mutex.Unlock()
	go func() {
		err := ui.run(runContext)
		ui.mutex.Lock()
		ui.active = false
		ui.stopping = false
		ui.cancel = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			ui.lastError = err.Error()
			ui.logger.Printf("bifrost stopped: %v", err)
		} else {
			ui.logger.Print("bifrost deactivated")
		}
		close(done)
		ui.mutex.Unlock()
	}()
}

func (ui *controlUI) deactivate() {
	ui.mutex.Lock()
	defer ui.mutex.Unlock()
	if !ui.active || ui.stopping {
		return
	}
	ui.stopping = true
	ui.logger.Print("bifrost deactivation requested")
	ui.cancel()
}

func (ui *controlUI) finished() <-chan struct{} {
	ui.mutex.Lock()
	defer ui.mutex.Unlock()
	if ui.done == nil {
		finished := make(chan struct{})
		close(finished)
		return finished
	}
	return ui.done
}

func (ui *controlUI) status() controlUIStatus {
	ui.mutex.Lock()
	defer ui.mutex.Unlock()
	status := controlUIStatus{Active: ui.active, Stopping: ui.stopping, LastError: ui.lastError}
	if ui.prManagerStatus != nil {
		status.PRManager = ui.prManagerStatus()
	}
	return status
}

func (ui *controlUI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", ui.handlePage)
	mux.HandleFunc("GET /api/status", ui.handleStatus)
	mux.HandleFunc("GET /api/config", ui.handleConfig)
	mux.HandleFunc("GET /api/logs", ui.handleLogs)
	mux.HandleFunc("GET /api/logs/stream", ui.handleLogStream)
	mux.HandleFunc("POST /api/activate", ui.handleActivate)
	mux.HandleFunc("POST /api/deactivate", ui.handleDeactivate)
	mux.HandleFunc("POST /api/pr-manager/activate", ui.handlePRManagerActivate)
	mux.HandleFunc("POST /api/pr-manager/deactivate", ui.handlePRManagerDeactivate)
	return mux
}

func (ui *controlUI) handlePage(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(writer, controlUIPage, html.EscapeString(ui.configPath), ui.token)
}

func (ui *controlUI) handleStatus(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, ui.status())
}

func (ui *controlUI) handleConfig(writer http.ResponseWriter, _ *http.Request) {
	contents, err := os.ReadFile(ui.configPath)
	if err != nil {
		http.Error(writer, fmt.Sprintf("read config: %v", err), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(contents)
}

func (ui *controlUI) handleLogs(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, struct {
		Lines []string `json:"lines"`
	}{Lines: ui.logs.snapshot()})
}

func (ui *controlUI) handleLogStream(writer http.ResponseWriter, request *http.Request) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	subscriber, unsubscribe := ui.logs.subscribe()
	defer unsubscribe()
	for {
		select {
		case <-request.Context().Done():
			return
		case line := <-subscriber:
			data, _ := json.Marshal(line)
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (ui *controlUI) handleActivate(writer http.ResponseWriter, request *http.Request) {
	if !ui.authorized(writer, request) {
		return
	}
	ui.activate()
	writeJSON(writer, ui.status())
}

func (ui *controlUI) handleDeactivate(writer http.ResponseWriter, request *http.Request) {
	if !ui.authorized(writer, request) {
		return
	}
	ui.deactivate()
	writeJSON(writer, ui.status())
}

func (ui *controlUI) handlePRManagerActivate(writer http.ResponseWriter, request *http.Request) {
	if !ui.authorized(writer, request) {
		return
	}
	if !ui.status().Active {
		http.Error(writer, "activate Bifrost before PR Manager", http.StatusConflict)
		return
	}
	if ui.activatePRManager == nil {
		http.Error(writer, "PR Manager is unavailable", http.StatusNotImplemented)
		return
	}
	if err := ui.activatePRManager(); err != nil {
		ui.logger.Printf("PR Manager activation failed: %v", err)
		writeJSON(writer, ui.status())
		return
	}
	writeJSON(writer, ui.status())
}

func (ui *controlUI) handlePRManagerDeactivate(writer http.ResponseWriter, request *http.Request) {
	if !ui.authorized(writer, request) {
		return
	}
	if ui.deactivatePRManager != nil {
		ui.deactivatePRManager()
	}
	writeJSON(writer, ui.status())
}

func (ui *controlUI) authorized(writer http.ResponseWriter, request *http.Request) bool {
	if request.Header.Get("X-Bifrost-Token") == ui.token {
		return true
	}
	http.Error(writer, "forbidden", http.StatusForbidden)
	return false
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

const controlUIPage = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bifrost</title><style>
body{background:#10131a;color:#e8edf5;font:15px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;max-width:1000px;margin:40px auto;padding:0 20px}h1{margin-bottom:6px}.muted{color:#a7b0bf}button{border:0;border-radius:6px;padding:9px 14px;font:inherit;font-weight:600;cursor:pointer;margin-right:8px}#activate{background:#2fbd78;color:#062515}#deactivate{background:#e36363;color:#310606}pre{overflow:auto;background:#181d27;border:1px solid #2a3240;border-radius:8px;padding:14px;white-space:pre-wrap;word-break:break-word}#status{font-weight:700}#error{color:#ff9b9b}.section{margin-top:30px}</style></head>
<body><h1>Bifrost</h1><div class="muted">%s</div><p>Status: <span id="status">Loading…</span> <span id="error"></span></p><p><button id="activate">Activate</button><button id="deactivate">Deactivate</button></p><p>PR Manager: <span id="pr-manager-status">Loading…</span> <span id="pr-manager-error"></span></p><p><button id="pr-manager-activate">Enable PR Manager</button><button id="pr-manager-deactivate">Disable PR Manager</button></p>
<div class="section"><h2>Live activity</h2><pre id="logs"></pre></div><div class="section"><h2>Config</h2><pre id="config">Loading…</pre></div>
<script>const token=%q,logs=document.querySelector('#logs');function append(line){logs.textContent+=line;logs.scrollTop=logs.scrollHeight}async function state(){let s=await fetch('/api/status').then(r=>r.json()),text=s.stopping?'Stopping…':s.active?'Active':'Inactive',pm=s.pr_manager;document.querySelector('#status').textContent=text;document.querySelector('#error').textContent=s.last_error?' — '+s.last_error:'';document.querySelector('#activate').disabled=s.active||s.stopping;document.querySelector('#deactivate').disabled=!s.active||s.stopping;document.querySelector('#pr-manager-status').textContent=pm.connected?'Connected':pm.running?'Connecting…':pm.last_error?'Error':pm.enabled?'Waiting':'Inactive';document.querySelector('#pr-manager-error').textContent=pm.last_error?' — '+pm.last_error:'';document.querySelector('#pr-manager-activate').disabled=!s.active||s.stopping||pm.running;document.querySelector('#pr-manager-deactivate').disabled=!pm.enabled}async function action(path){await fetch(path,{method:'POST',headers:{'X-Bifrost-Token':token}});state()}fetch('/api/logs').then(r=>r.json()).then(v=>v.lines.forEach(append));fetch('/api/config').then(r=>r.json()).then(v=>document.querySelector('#config').textContent=JSON.stringify(v,null,2));new EventSource('/api/logs/stream').onmessage=e=>append(JSON.parse(e.data));document.querySelector('#activate').onclick=()=>action('/api/activate');document.querySelector('#deactivate').onclick=()=>action('/api/deactivate');document.querySelector('#pr-manager-activate').onclick=()=>action('/api/pr-manager/activate');document.querySelector('#pr-manager-deactivate').onclick=()=>action('/api/pr-manager/deactivate');state();setInterval(state,1000)</script></body></html>`
