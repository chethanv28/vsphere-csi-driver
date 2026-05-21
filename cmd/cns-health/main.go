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

// Command cns-health is the entry point for the agentic CSI observability
// container. It wires the agent (observe → reason → act → remember loop)
// together with the HTTP server (dashboard + JSON API + /metrics).
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/agent"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/findings"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/observe"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/rules"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/server"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/state"
)

func main() {
	var (
		listenAddr        = flag.String("listen-addr", ":8080", "HTTP listen address for dashboard and JSON API")
		driverNamespace   = flag.String("driver-namespace", envOr("CSI_NAMESPACE", "vmware-system-csi"), "Namespace where the CSI driver pods run")
		csiDriverName     = flag.String("csi-driver-name", "csi.vsphere.vmware.com", "CSI driver name to identify in informers")
		metricsEndpoints  = flag.String("metrics-endpoints", "", "Comma-separated http://host:port/metrics URLs to scrape. Defaults to the in-cluster vsphere-csi-controller Service on 2112 and 2113 if empty.")
		reconcileEvery    = flag.Duration("reconcile-every", 30*time.Second, "Reconcile loop interval")
		metricScrapeEvery = flag.Duration("metric-scrape-every", 60*time.Second, "Metric scrape interval")
		kubeconfig        = flag.String("kubeconfig", "", "Out-of-cluster kubeconfig path (optional)")
	)
	klog.InitFlags(nil)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadKubeConfig(*kubeconfig)
	if err != nil {
		klog.Fatalf("load kube config: %v", err)
	}
	cfg.UserAgent = "cns-health/0.1"

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("kubernetes client: %v", err)
	}

	stateStore := state.New()
	findStore := findings.NewStore(24 * time.Hour)

	endpoints := parseEndpoints(*metricsEndpoints, *driverNamespace)
	metricsObs := observe.NewMetricsObserver(endpoints)
	k8sObs := observe.NewK8sObserver(client, *driverNamespace, *csiDriverName, stateStore)

	ag := &agent.Agent{
		K8s:               k8sObs,
		Metrics:           metricsObs,
		State:             stateStore,
		Findings:          findStore,
		Engine:            rules.NewEngine(),
		ReconcileEvery:    *reconcileEvery,
		MetricScrapeEvery: *metricScrapeEvery,
	}

	srv, err := server.New(ag, stateStore, findStore, metricsObs)
	if err != nil {
		klog.Fatalf("http server setup: %v", err)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- ag.Run(ctx) }()
	go func() { errCh <- srv.ListenAndServe(ctx, *listenAddr) }()

	klog.InfoS("cns-health started",
		"listenAddr", *listenAddr,
		"driverNamespace", *driverNamespace,
		"csiDriverName", *csiDriverName,
		"metricsEndpoints", endpoints,
	)

	select {
	case <-ctx.Done():
		klog.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			klog.ErrorS(err, "component exited")
		}
		cancel()
	}
	klog.Info("cns-health stopped")
}

func loadKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
}

// parseEndpoints returns the list of Prometheus endpoints to scrape.
// If the user passed nothing, default to the in-cluster vsphere-csi-controller
// Service on both the controller (2112) and syncer (2113) ports.
func parseEndpoints(raw, namespace string) []string {
	if raw != "" {
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if t := strings.TrimSpace(p); t != "" {
				out = append(out, t)
			}
		}
		return out
	}
	host := "vsphere-csi-controller." + namespace + ".svc.cluster.local"
	return []string{
		"http://" + host + ":2112/metrics",
		"http://" + host + ":2113/metrics",
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
