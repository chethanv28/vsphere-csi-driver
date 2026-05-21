/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package server is the cns-health HTTP server: dashboard, JSON API,
// /metrics endpoint, and /healthz. Stdlib only — no router framework.
package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/agent"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/findings"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/observe"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/state"
)

//go:embed templates/*.html
var templatesFS embed.FS

// Server holds references to the world model. It is intentionally
// read-only with respect to the agent: the HTTP server never mutates the
// state, it only renders it.
type Server struct {
	Agent     *agent.Agent
	State     *state.Store
	Findings  *findings.Store
	Metrics   *observe.MetricsObserver
	StartedAt time.Time

	tmpl *template.Template
}

// New constructs a Server with pre-parsed templates.
func New(ag *agent.Agent, st *state.Store, fs *findings.Store, mo *observe.MetricsObserver) (*Server, error) {
	tmpl, err := template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	return &Server{
		Agent:     ag,
		State:     st,
		Findings:  fs,
		Metrics:   mo,
		StartedAt: time.Now().UTC(),
		tmpl:      tmpl,
	}, nil
}

// Handler returns the configured http.ServeMux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/state", s.handleAPIState)
	mux.HandleFunc("/api/findings", s.handleAPIFindings)
	mux.HandleFunc("/api/metrics", s.handleAPIMetricsSummary)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// ListenAndServe starts the HTTP server. It returns once ctx is cancelled
// or ListenAndServe fails.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	klog.InfoS("cns-health http server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

type indexData struct {
	Driver        state.CSIDriverInfo
	Health        state.HealthSnapshot
	Inventory     state.VolumeInventory
	Findings      []findings.Finding
	MetricsBy     map[string]observe.MetricsSummary
	Now           time.Time
	StartedAt     time.Time
	Uptime        string
	LastReconcile time.Time
	ReconcileN    uint64
	LastError     string
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	now := time.Now().UTC()
	data := indexData{
		Driver:        s.State.Driver(),
		Health:        s.State.Health(),
		Inventory:     s.State.Inventory(),
		Findings:      s.Findings.List(),
		MetricsBy:     s.Metrics.Summaries(),
		Now:           now,
		StartedAt:     s.StartedAt,
		Uptime:        now.Sub(s.StartedAt).Round(time.Second).String(),
		LastReconcile: s.Agent.LastReconcile(),
		ReconcileN:    s.Agent.ReconcileCount(),
		LastError:     s.Agent.LastError(),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		klog.ErrorS(err, "render index template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleAPIState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"driver":    s.State.Driver(),
		"health":    s.State.Health(),
		"inventory": s.State.Inventory(),
		"agent": map[string]any{
			"uptime":        time.Since(s.StartedAt).Round(time.Second).String(),
			"lastReconcile": s.Agent.LastReconcile(),
			"reconciles":    s.Agent.ReconcileCount(),
			"lastError":     s.Agent.LastError(),
		},
	})
}

func (s *Server) handleAPIFindings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.Findings.List())
}

func (s *Server) handleAPIMetricsSummary(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.Metrics.Summaries())
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.Agent.ReconcileCount() == 0 {
		http.Error(w, "no reconcile yet", http.StatusServiceUnavailable)
		return
	}
	if time.Since(s.Agent.LastReconcile()) > 5*time.Minute {
		http.Error(w, "stale", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		klog.ErrorS(err, "encode json")
	}
}
