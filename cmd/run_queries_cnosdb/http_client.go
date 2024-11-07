package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/cnosdb/tsdb-comparisons/pkg/query"
)

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client       *http.Client
	url          []byte
	urlPrefixLen int
	basicAuth    string
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	debug                int
	prettyPrintResponses bool
	database             string
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 1024,
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(url string) *HTTPClient {
	return &HTTPClient{
		client:       getHttpClient(),
		url:          []byte(url),
		urlPrefixLen: len(url),
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	w.url = w.url[:w.urlPrefixLen]
	w.url = append(w.url, []byte(url.QueryEscape(opts.database))...)

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), string(w.url), bytes.NewReader(q.Body))
	if err != nil {
		panic(err)
	}
	if basicAuth != "" {
		req.Header.Add(fasthttp.HeaderAuthorization, basicAuth)
	}

	// Perform the request while tracking latency:
	start := time.Now()
	resp, err := w.client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respMsg, err := io.ReadAll(resp.Body)
		if err == nil {
			panic(fmt.Sprintf("query request returned non-200 code: %d: %s", resp.StatusCode, respMsg))
		} else {
			panic(fmt.Sprintf("query request returned non-200 code: %d", resp.StatusCode))
		}
	}

	var body []byte
	body, err = io.ReadAll(resp.Body)

	if err != nil {
		panic(err)
	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	if opts != nil {
		// Print debug messages, if applicable:
		switch opts.debug {
		case 1:
			_, _ = fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		case 2:
			_, _ = fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		case 3:
			_, _ = fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			_, _ = fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		case 4:
			_, _ = fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			_, _ = fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
			_, _ = fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(body))
		default:
		}

		// Pretty print JSON responses, if applicable:
		if opts.prettyPrintResponses {
			// Assumes the response is JSON! This holds for Influx
			// and Elastic.

			prefix := fmt.Sprintf("ID %d: ", q.GetID())
			var v interface{}
			var line []byte
			full := make(map[string]interface{})
			full["sql"] = string(q.RawQuery)
			_ = json.Unmarshal(body, &v)
			full["result"] = v
			line, err = json.MarshalIndent(full, prefix, "  ")
			if err != nil {
				return
			}
			fmt.Println(string(line) + "\n")
		}
	}

	return lag, err
}
