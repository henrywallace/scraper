package scraper_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/henrywallace/scraper"
	"github.com/henrywallace/scraper/blob"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestScraperDoTwice(t *testing.T) {
	ts := setup(t)
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
	cacheDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	bucket, err := blob.NewBucket(ctx, bucketURL, &blob.OptBucketCacheDir{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

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
