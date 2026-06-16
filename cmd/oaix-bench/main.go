package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Duration time.Duration
	Status   int
	Error    string
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8000", "gateway base URL")
	apiKey := flag.String("api-key", "", "service API key")
	total := flag.Int("streams", 1000, "total stream requests")
	concurrency := flag.Int("concurrency", 1000, "concurrent stream requests")
	timeout := flag.Duration("timeout", 60*time.Second, "benchmark timeout")
	flag.Parse()
	if *total <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "streams and concurrency must be positive")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        *concurrency + 100,
			MaxIdleConnsPerHost: *concurrency + 100,
			MaxConnsPerHost:     *concurrency + 100,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	results := make([]result, *total)
	jobs := make(chan int)
	var ok atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	workers := *concurrency
	if workers > *total {
		workers = *total
	}
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = runOne(ctx, client, *target, *apiKey, index)
				if results[index].Error == "" && results[index].Status >= 200 && results[index].Status < 300 {
					ok.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for index := 0; index < *total; index++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			printSummary(started, results, ok.Load(), failed.Load()+int64(*total-index), ctx.Err())
			os.Exit(1)
		case jobs <- index:
		}
	}
	close(jobs)
	wg.Wait()
	printSummary(started, results, ok.Load(), failed.Load(), nil)
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

func runOne(ctx context.Context, client *http.Client, target, apiKey string, index int) result {
	body := []byte(fmt.Sprintf(`{"model":"gpt-5","input":"bench-%d","stream":true}`, index))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/responses", bytes.NewReader(body))
	if err != nil {
		return result{Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Request-ID", fmt.Sprintf("bench-%d", index))
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return result{Duration: time.Since(started), Error: err.Error()}
	}
	defer resp.Body.Close()
	_, copyErr := io.Copy(io.Discard, resp.Body)
	if copyErr != nil {
		return result{Duration: time.Since(started), Status: resp.StatusCode, Error: copyErr.Error()}
	}
	return result{Duration: time.Since(started), Status: resp.StatusCode}
}

func printSummary(started time.Time, results []result, ok, failed int64, err error) {
	durations := make([]time.Duration, 0, len(results))
	statusCounts := map[int]int{}
	errorSamples := make([]string, 0, 5)
	for _, item := range results {
		if item.Duration > 0 {
			durations = append(durations, item.Duration)
		}
		if item.Status != 0 {
			statusCounts[item.Status]++
		}
		if item.Error != "" && len(errorSamples) < 5 {
			errorSamples = append(errorSamples, item.Error)
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	payload := map[string]any{
		"total":          len(results),
		"ok":             ok,
		"failed":         failed,
		"elapsed_ms":     time.Since(started).Milliseconds(),
		"p50_ms":         percentile(durations, 0.50).Milliseconds(),
		"p95_ms":         percentile(durations, 0.95).Milliseconds(),
		"p99_ms":         percentile(durations, 0.99).Milliseconds(),
		"status_counts":  statusCounts,
		"error_samples":  errorSamples,
		"terminal_error": "",
	}
	if err != nil {
		payload["terminal_error"] = err.Error()
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(payload)
}

func percentile(values []time.Duration, ratio float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * ratio)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
