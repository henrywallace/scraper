package scraper_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/henrywallace/scraper"
	"github.com/henrywallace/scraper/blob"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestScraperLiveDoTwice(t *testing.T) {
	ts := setup(t)
	setLive(t)
	req, err := http.NewRequest("GET", "https://httpbin.org/anything", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	// First request should hit live web, without browser automation as we
	// didn't request it.
	page, err := ts.scraper.Do(ts.ctx, req)
	if err != nil {
		t.Fatalf("failed to do request: %v", err)
	}
	if page.Request.Method != "GET" {
		t.Errorf("request method %q != GET", page.Request.Method)
	}
	if len(page.Response.Body) == 0 {
		t.Errorf("response body is empty")
	}
	if page.Meta.Source != "http.plain" {
		t.Errorf("expected source to be not http.plain, got: %v", page.Meta.Source)
	}

	// Second request should hit cache.
	page, err = ts.scraper.Do(ts.ctx, req)
	if err != nil {
		t.Fatalf("failed to do request: %v", err)
	}
	if page.Request.Method != "GET" {
		t.Errorf("request method %q != GET", page.Request.Method)
	}
	if len(page.Response.Body) == 0 {
		t.Errorf("response body is empty")
	}
	if page.Meta.Source != "cache" {
		t.Errorf("expected source to be not cache, got: %v", page.Meta.Source)
	}
}

func TestScraperDoBrowser(t *testing.T) {
	ts := setup(t)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		content := `
		<html>
		  <head>
		    <title>Test Page</title>
		    <script type="text/javascript">
		      document.addEventListener('DOMContentLoaded', function() {
		          document.getElementById('content').textContent = 'This is JavaScript-rendered content!';
		      });
		    </script>
		  </head>
		  <body>
		      <div id="content">Initial content</div>
		  </body>
		</html>
		`
		n, err := w.Write([]byte(content))
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
		if n < len(content) {
			t.Fatalf("failed to write full response: %d < %d", n, len(content))
		}
	}))
	t.Cleanup(svr.Close)

	req, err := http.NewRequest("GET", svr.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	page, err := ts.scraper.Do(ts.ctx, req, &scraper.OptDoBrowser{})
	if err != nil {
		t.Fatalf("failed to do request: %v", err)
	}
	if page.Request.Method != "GET" {
		t.Errorf("request method %q != GET", page.Request.Method)
	}
	if page.Meta.Source != "http.browser" {
		t.Errorf("expected source to be not http.plain, got: %v", page.Meta.Source)
	}
	if strings.Contains(string(page.Response.Body), `<div id="content">Initial content</div>`) {
		t.Errorf("response body did not contain rendered content: %v", string(page.Response.Body))
	}
	if !strings.Contains(string(page.Response.Body), `<div id="content">This is JavaScript-rendered content!</div>`) {
		t.Errorf("response body did not contain rendered content: %v", string(page.Response.Body))
	}
}

func setLive(t *testing.T) {
	t.Helper()
	val := os.Getenv("TEST_LIVE_HTTP")
	if val == "" {
		t.Skip("skipping, to run test set TEST_LIVE_HTTP=true")
	}
	p, err := strconv.ParseBool(val)
	if err != nil {
		t.Fatalf("failed to parse TEST_LIVE_HTTP=%q: %v", val, err)
	}
	if !p {
		t.Skip("skipping because TEST_LIVE_HTTP=false")
	}
}

type testState struct {
	ctx     context.Context
	bucket  *blob.Bucket
	scraper *scraper.Scraper
}

func setup(t *testing.T) testState {
	t.Helper()

	ctx := context.Background()
	setupLogger()

	bucketURL, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketURL) })
	cacheDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })
	bucket, err := blob.NewBucket(ctx, bucketURL, &blob.OptBucketCacheDir{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	sc, err := scraper.NewScraper(ctx, bucket)
	if err != nil {
		t.Fatalf("failed to create scraper: %v", err)
	}
	t.Cleanup(sc.Close)

	return testState{
		ctx:     ctx,
		bucket:  bucket,
		scraper: sc,
	}
}

var logLevel zerolog.Level

func init() {
	lvl := zerolog.Disabled
	if env := os.Getenv("LOG_LEVEL"); env != "" {
		var err error
		lvl, err = zerolog.ParseLevel(env)
		if err != nil {
			panic(fmt.Sprintf("failed to parse LOG_LEVEL=%q: %v", env, err))
		}
	}
	logLevel = lvl
}

func setupLogger() {
	lg := log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(logLevel).
		With().
		Caller().
		Logger()
	log.Logger = lg
	zerolog.DefaultContextLogger = &log.Logger
}
