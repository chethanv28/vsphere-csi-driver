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

package observe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MetricsSummary is a small distilled view of the CSI driver's Prometheus
// output. The dashboard renders this directly; the rules engine consults it
// to decide whether driver behaviour has drifted from baseline.
type MetricsSummary struct {
	Endpoint              string             `json:"endpoint"`
	Reachable             bool               `json:"reachable"`
	ScrapeErr             string             `json:"scrapeErr,omitempty"`
	ScrapedAt             time.Time          `json:"scrapedAt"`
	OpsPerMinByOp         map[string]float64 `json:"opsPerMinByOp,omitempty"`
	ErrorsPerMinByFault   map[string]float64 `json:"errorsPerMinByFault,omitempty"`
	P95LatencySecondsByOp map[string]float64 `json:"p95LatencySecondsByOp,omitempty"`
	InaccessibleVolumes   float64            `json:"inaccessibleVolumes"`
	AccessibleVolumes     float64            `json:"accessibleVolumes"`
	CSIInfo               map[string]string  `json:"csiInfo,omitempty"`
	SyncerInfo            map[string]string  `json:"syncerInfo,omitempty"`
}

// MetricsObserver scrapes the CSI driver's Prometheus endpoints and
// derives short-window operation summaries. It maintains a rolling
// snapshot of the previous scrape to compute per-minute rates without
// requiring a real Prometheus server.
type MetricsObserver struct {
	endpoints []string
	client    *http.Client

	mu          sync.RWMutex
	current     map[string]MetricsSummary
	prevSamples map[string]rawSample
}

type rawSample struct {
	at                  time.Time
	csiOpsCountTotal    float64 // sum of vsphere_csi_volume_ops_histogram_count
	csiErrCountByFault  map[string]float64
	csiOpsCountByOp     map[string]float64
	csiBucketsByOpLE    map[string]map[float64]float64 // op -> le -> cumulative count
	csiOpsSumByOp       map[string]float64
	inaccessibleVolumes float64
	accessibleVolumes   float64
	info                map[string]map[string]string
}

// NewMetricsObserver constructs an observer that periodically scrapes the
// given URL list. Typical values for vSphere CSI: controller :2112, syncer
// :2113. The observer keeps each endpoint's data separately keyed by URL.
func NewMetricsObserver(endpoints []string) *MetricsObserver {
	return &MetricsObserver{
		endpoints: endpoints,
		client:    &http.Client{Timeout: 5 * time.Second},
		current:   make(map[string]MetricsSummary),
		prevSamples: make(map[string]rawSample),
	}
}

// Scrape contacts each endpoint once and updates the in-memory snapshot.
func (m *MetricsObserver) Scrape(ctx context.Context) {
	for _, ep := range m.endpoints {
		summary := m.scrapeOne(ctx, ep)
		m.mu.Lock()
		m.current[ep] = summary
		m.mu.Unlock()
	}
}

// Summaries returns a snapshot of the most recent per-endpoint summaries.
func (m *MetricsObserver) Summaries() map[string]MetricsSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]MetricsSummary, len(m.current))
	for k, v := range m.current {
		out[k] = v
	}
	return out
}

// AnyReachable returns true if at least one endpoint was reachable on the
// last scrape. The rules engine uses this to detect "driver alive but not
// answering" vs "driver appears down."
func (m *MetricsObserver) AnyReachable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.current {
		if s.Reachable {
			return true
		}
	}
	return false
}

func (m *MetricsObserver) scrapeOne(ctx context.Context, url string) MetricsSummary {
	now := time.Now().UTC()
	summary := MetricsSummary{Endpoint: url, ScrapedAt: now}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		summary.ScrapeErr = err.Error()
		return summary
	}
	resp, err := m.client.Do(req)
	if err != nil {
		summary.ScrapeErr = err.Error()
		return summary
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		summary.ScrapeErr = fmt.Sprintf("http %d", resp.StatusCode)
		return summary
	}

	parsed, err := parsePrometheusText(resp.Body)
	if err != nil {
		summary.ScrapeErr = err.Error()
		return summary
	}
	summary.Reachable = true

	sample := buildSample(parsed, now)
	m.mu.Lock()
	prev, hasPrev := m.prevSamples[url]
	m.prevSamples[url] = sample
	m.mu.Unlock()

	summary.AccessibleVolumes = sample.accessibleVolumes
	summary.InaccessibleVolumes = sample.inaccessibleVolumes
	if info := sample.info["vsphere_csi_info"]; info != nil {
		summary.CSIInfo = info
	}
	if info := sample.info["vsphere_syncer_info"]; info != nil {
		summary.SyncerInfo = info
	}

	if hasPrev {
		dt := sample.at.Sub(prev.at).Seconds()
		if dt > 0 {
			summary.OpsPerMinByOp = perMin(diffMap(sample.csiOpsCountByOp, prev.csiOpsCountByOp), dt)
			summary.ErrorsPerMinByFault = perMin(diffMap(sample.csiErrCountByFault, prev.csiErrCountByFault), dt)
			summary.P95LatencySecondsByOp = computeP95Diffs(sample.csiBucketsByOpLE, prev.csiBucketsByOpLE)
		}
	}
	return summary
}

// parsedSeries is the result of one scrape: a map of metric-name to
// time series with labels.
type parsedSeries struct {
	name   string
	labels map[string]string
	value  float64
}

func parsePrometheusText(r io.Reader) ([]parsedSeries, error) {
	var out []parsedSeries
	scanner := bufio.NewScanner(r)
	// Each text-format line may be very long; bump the buffer.
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 8<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series, ok := parseOneLine(line)
		if !ok {
			continue
		}
		out = append(out, series)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("empty metrics output")
	}
	return out, nil
}

// parseOneLine handles the standard Prometheus text-format line shape:
//
//	metric_name{label_a="x",label_b="y"} 1.234
//	metric_name 1.234
//
// without dragging in a full text-format parser.
func parseOneLine(line string) (parsedSeries, bool) {
	var ps parsedSeries
	ps.labels = make(map[string]string)
	openBrace := strings.Index(line, "{")
	if openBrace == -1 {
		// metric value
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return ps, false
		}
		ps.name = parts[0]
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return ps, false
		}
		ps.value = v
		return ps, true
	}
	closeBrace := strings.LastIndex(line, "}")
	if closeBrace == -1 || closeBrace < openBrace {
		return ps, false
	}
	ps.name = strings.TrimSpace(line[:openBrace])
	labelBlob := line[openBrace+1 : closeBrace]
	for _, kv := range splitLabels(labelBlob) {
		i := strings.Index(kv, "=")
		if i < 0 {
			continue
		}
		k := strings.TrimSpace(kv[:i])
		v := strings.Trim(strings.TrimSpace(kv[i+1:]), "\"")
		ps.labels[k] = v
	}
	rest := strings.TrimSpace(line[closeBrace+1:])
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ps, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return ps, false
	}
	ps.value = v
	return ps, true
}

// splitLabels splits a label blob on commas but respects quoted strings.
func splitLabels(s string) []string {
	var out []string
	var cur strings.Builder
	inQ := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			inQ = !inQ
			cur.WriteByte(c)
		case ',':
			if inQ {
				cur.WriteByte(c)
			} else {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func buildSample(parsed []parsedSeries, at time.Time) rawSample {
	s := rawSample{
		at:                 at,
		csiErrCountByFault: make(map[string]float64),
		csiOpsCountByOp:    make(map[string]float64),
		csiBucketsByOpLE:   make(map[string]map[float64]float64),
		csiOpsSumByOp:      make(map[string]float64),
		info:               make(map[string]map[string]string),
	}
	for _, ts := range parsed {
		switch ts.name {
		case "vsphere_csi_volume_ops_histogram_count":
			op := ts.labels["optype"]
			s.csiOpsCountByOp[op] += ts.value
			s.csiOpsCountTotal += ts.value
			if status := ts.labels["status"]; status == "fail" {
				ft := ts.labels["faulttype"]
				if ft == "" {
					ft = "Unknown"
				}
				s.csiErrCountByFault[ft] += ts.value
			}
		case "vsphere_csi_volume_ops_histogram_sum":
			op := ts.labels["optype"]
			s.csiOpsSumByOp[op] += ts.value
		case "vsphere_csi_volume_ops_histogram_bucket":
			op := ts.labels["optype"]
			le, err := strconv.ParseFloat(ts.labels["le"], 64)
			if err != nil {
				continue
			}
			if _, ok := s.csiBucketsByOpLE[op]; !ok {
				s.csiBucketsByOpLE[op] = make(map[float64]float64)
			}
			s.csiBucketsByOpLE[op][le] += ts.value
		case "vsphere_volume_health_gauge":
			switch ts.labels["volume_health_type"] {
			case "accessible-volumes":
				s.accessibleVolumes = ts.value
			case "inaccessible-volumes":
				s.inaccessibleVolumes = ts.value
			}
		case "vsphere_csi_info":
			if s.info[ts.name] == nil {
				s.info[ts.name] = make(map[string]string)
			}
			for k, v := range ts.labels {
				s.info[ts.name][k] = v
			}
		case "vsphere_syncer_info":
			if s.info[ts.name] == nil {
				s.info[ts.name] = make(map[string]string)
			}
			for k, v := range ts.labels {
				s.info[ts.name][k] = v
			}
		}
	}
	return s
}

// diffMap returns curr - prev for each shared key. Negative diffs are
// treated as zero (counter resets).
func diffMap(curr, prev map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(curr))
	for k, v := range curr {
		d := v - prev[k]
		if d < 0 {
			d = 0
		}
		out[k] = d
	}
	return out
}

func perMin(diffs map[string]float64, dtSec float64) map[string]float64 {
	if dtSec <= 0 {
		return nil
	}
	out := make(map[string]float64, len(diffs))
	for k, v := range diffs {
		out[k] = v / dtSec * 60.0
	}
	return out
}

// computeP95Diffs walks the diff of cumulative bucket counts between
// the current and previous samples, returning a p95-second estimate per op.
func computeP95Diffs(curr, prev map[string]map[float64]float64) map[string]float64 {
	out := make(map[string]float64, len(curr))
	for op, bucketsNow := range curr {
		bucketsPrev := prev[op]
		// Sort bucket boundaries ascending.
		bounds := make([]float64, 0, len(bucketsNow))
		for le := range bucketsNow {
			bounds = append(bounds, le)
		}
		sort.Float64s(bounds)
		var total, cum float64
		incremental := make([]float64, len(bounds))
		for i, le := range bounds {
			d := bucketsNow[le] - bucketsPrev[le]
			if d < 0 {
				d = 0
			}
			incremental[i] = d
			total += d
		}
		if total == 0 {
			continue
		}
		target := total * 0.95
		for i, le := range bounds {
			cum += incremental[i]
			if cum >= target {
				out[op] = le
				break
			}
		}
	}
	return out
}
