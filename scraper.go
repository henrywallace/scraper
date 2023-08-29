package scraper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"go.uber.org/ratelimit"

	"github.com/henrywallace/scraper/blob"
)

var veryStart = time.Now()
var requests atomic.Uint64

var envRateLimit = "SCRAPER_RATE_LIMIT"
var rateLimitOverride ratelimit.Limiter
var defaultRateLimit = ratelimit.New(100)

var reNumbericPrefix = regexp.MustCompile(`^\d+`)

func init() {
	rateLimitRaw, ok := os.LookupEnv(envRateLimit)
	if !ok {
		log.Debug().Msgf("%s not set, using default", envRateLimit)
		return
	}
	switch strings.ToLower(rateLimitRaw) {
	case "none", "unlimited", "disabled", "off", "nolimit":
		rateLimitOverride = ratelimit.NewUnlimited()
		return
	}

	parts := strings.SplitN(rateLimitRaw, "/", 2)
	rate, err := strconv.ParseInt(parts[0], 10, 0)
	if err != nil {
		log.Fatal().Msgf("failed to parse %s=%q: %v", envRateLimit, rateLimitRaw, err)
	}
	var opts []ratelimit.Option
	if len(parts) == 2 {
		per := parts[1]
		if !reNumbericPrefix.MatchString(per) {
			per = fmt.Sprintf("1%s", per)
		}
		dur, err := time.ParseDuration(per)
		if err != nil {
			log.Fatal().Msgf("failed to parse %s=%q: %v", envRateLimit, rateLimitRaw, err)
		}
		opts = append(opts, ratelimit.Per(dur))
	}
	rateLimitOverride = ratelimit.New(int(rate), opts...)
}

type Scraper struct {
	httpClient       *retryablehttp.Client
	bucket           *blob.Bucket
	mu               *sync.Mutex
	pw               *playwright.Playwright
	browser          playwright.Browser
	alwaysDoBrowser  bool
	startBrowser     func() error
	requestBodyLimit int64 // no limit when <= 0
	respBodyLimit    int64 // no limit when <= 0
}

func NewScraper(
	ctx context.Context,
	blob *blob.Bucket,
	opts ...Option,
) (*Scraper, error) {
	httpClient := retryablehttp.NewClient()
	httpClient.HTTPClient = cleanhttp.DefaultClient() // not pooled
	httpClient.Logger = leveledLogger{log.Ctx(ctx)}
	httpClient.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, i int) {
		if rateLimitOverride != nil {
			rateLimitOverride.Take()
		} else {
			val, ok := req.Context().Value(ctxKeyLimiter{}).(ctxValLimiter)
			if ok {
				val.Limiter.Take()
			} else {
				defaultRateLimit.Take()
			}
		}
		requests.Add(1)
	}
	s := &Scraper{
		httpClient:       httpClient,
		bucket:           blob,
		mu:               new(sync.Mutex),
		pw:               nil,
		browser:          nil,
		alwaysDoBrowser:  false,
		startBrowser:     nil,
		requestBodyLimit: 10e6,  // 10 MB
		respBodyLimit:    100e6, // 100 MB
	}
	s.startBrowser = sync.OnceValue(s.newBrowser)
	for _, opt := range opts {
		opt.option(s)
	}
	// If always do browser, then fail fast, until waiting for the first
	// Do.
	if s.alwaysDoBrowser {
		if err := s.startBrowser(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Scraper) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeBrowser()
}

type Option interface {
	option(s *Scraper) optionSeal
}

type optionSeal struct{}

type option struct {
	fn func(s *Scraper)
}

func (o *option) option(s *Scraper) optionSeal {
	o.fn(s)
	return optionSeal{}
}

func OptScraperAlwaysDoBrowser() Option {
	return &option{func(s *Scraper) {
		s.alwaysDoBrowser = true
	}}
}

// FetchStatusNotOKError is returned when the fetch status is not 200 OK. The
// Page contains the response and status.
type FetchStatusNotOKError struct {
	Page *Page
}

func (e *FetchStatusNotOKError) Error() string {
	return fmt.Sprintf("bad fetch status: %d", e.Page.Response.StatusCode)
}

func errPageStatusNotOK(page *Page) error {
	if page.Response.StatusCode != 200 {
		return &FetchStatusNotOKError{
			Page: page,
		}
	}
	return nil
}

// FetchThrottledError is returned when the fetch is throttled.
type FetchThrottledError struct{}

func (e *FetchThrottledError) Error() string {
	return "fetch throtted"
}

func (s *Scraper) Do(
	ctx context.Context,
	req *http.Request,
	options ...DoOption,
) (page *Page, err error) {
	opts := doOptions{
		Replace:          false,
		ReSilentThrottle: nil,
		Limiter:          nil,
	}
	browser := false
	for _, opt := range options {
		switch opt := opt.(type) {
		case *OptDoReplace:
			opts.Replace = true
		case *OptDoSilentThrottle:
			opts.ReSilentThrottle = opt.PageBytesRegexp
		case *OptDoLimiter:
			opts.Limiter = opt.Limiter
		case *OptDoBrowser:
			browser = true
		default:
			panic(fmt.Sprintf("invalid fetch option: %T", opt))
		}
	}
	fn := s.fetchPlain
	if browser {
		fn = s.fetchBrowser
	}
	return s.do(ctx, req, opts, fn)
}

func (s *Scraper) newBrowser() (err error) {
	start := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Debug().Msg("starting playwright instance")
	pw, err := playwright.Run(&playwright.RunOptions{
		Verbose: true,
	})
	if err != nil {
		return fmt.Errorf("failed to run playwright instance: %w", err)
	}
	s.pw = pw
	log.Debug().Msg("launching headless chromium browser")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless:        playwright.Bool(true),
		ChromiumSandbox: playwright.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("failed to launch browser: %w", err)
	}
	if !browser.IsConnected() {
		return fmt.Errorf("browser is not connected")
	}
	s.browser = browser

	log.Debug().
		Stringer("dur", time.Since(start).Round(time.Microsecond)).
		Msg("browser ready")
	return nil
}

func (s *Scraper) closeBrowser() {
	if s.browser != nil {
		if err := s.browser.Close(); err != nil {
			log.Err(err).Msg("failed to close browser")
		}
		s.browser = nil
	}
	if s.pw != nil {
		if err := s.pw.Stop(); err != nil {
			log.Err(err).Msg("failed to close playwright instance")
		}
		s.pw = nil
	}
}

type doOptions struct {
	Replace          bool
	ReSilentThrottle *regexp.Regexp
	Limiter          Limiter
}

// ctx is not added to req
// already read body from req
type fetchFn func(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error)

// fetchPlain retrieves the content of a given HTTP request and returns it
// structured in a Page.
//
// To prevent potential DoS we use http.MaxBytesReader to cap the size of the
// request body that can be read.
func (s *Scraper) fetchPlain(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error) {
	start := time.Now()

	// Retry, as reading the body can fail outside the purview of the
	// retryablehttp api. Or, the read body could indicate that the request
	// should be retried. The alternative of adding it to the CheckRetry
	// func would be too awkward as it would involve conditionally
	// forwarding an already read body.
	var resp *http.Response
	var body []byte
	attemptsMax := 5
	waitMin := 1 * time.Second
	waitMax := 1 * time.Minute
	waitJitter := 1 * time.Second
	wait := func(attempt int) error {
		d := time.Duration(math.Pow(2, float64(attempt))) * waitMin
		d += time.Duration(rand.Intn(int(waitJitter)))
		if d > waitMax {
			d = waitMax
		}
		t := time.After(d)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t:
				return nil
			}
		}
	}
	for i := 0; i < attemptsMax; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rreq, err := retryablehttp.FromRequest(req)
		if err != nil {
			return nil, err
		}
		resp, err = s.httpClient.Do(rreq)
		if err != nil {
			return nil, fmt.Errorf("failed to perform http get: %w", err)
		}
		rdr := resp.Body
		if s.respBodyLimit > 0 {
			rdr = http.MaxBytesReader(nil, resp.Body, s.respBodyLimit)
		}
		body, err = io.ReadAll(rdr)
		resp.Body.Close()
		lastAttempt := i >= attemptsMax-1
		if err != nil {
			if lastAttempt {
				return nil, fmt.Errorf("failed to read http resp body: %w", err)
			}
			log.Warn().Err(err).Int("attempt", i).Msg("failed to read http resp body, retrying")
			if err := wait(i); err != nil {
				return nil, err
			}
			continue
		}
		if opts.ReSilentThrottle != nil && opts.ReSilentThrottle.Match(body) {
			n := requests.Load()
			rate := float64(n) / (time.Since(veryStart).Minutes())
			log.Warn().
				Str("rate", fmt.Sprintf("%0.3f/m", rate)).
				Msg("silently throttled")
			if lastAttempt {
				return nil, &FetchThrottledError{}
			}
			log.Warn().Int("attempt", i).Msg("response is silently throttled, retrying")
			if err := wait(i); err != nil {
				return nil, err
			}
			continue
		}
		break
	}

	redirect := ""
	if resp.Request.URL.String() != req.URL.String() {
		redirect = resp.Request.URL.String()
	}
	dur := time.Since(start)
	return &Page{
		Meta: PageMeta{
			Version:     LatestPageVersion,
			Source:      "http.plain",
			RetrieveDur: time.Since(start),
			ScrapedAt:   time.Now(),
			FetchDur:    dur,
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: redirect,
			Method:        req.Method,
			Header:        resp.Request.Header,
			Body:          reqBody,
		},
		Response: PageResponse{
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body:       body,
		},
	}, nil
}

func (s *Scraper) fetchBrowser(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error) {
	if s.browser == nil {
		// As scrapers do not always do browser requests, we lazily
		// start the browser.
		if err := s.startBrowser(); err != nil {
			return nil, fmt.Errorf("failed to start browser: %w", err)
		}
	}
	if !s.browser.IsConnected() {
		return nil, fmt.Errorf("browser is not connected")
	}
	if req.Method != "GET" {
		return nil, fmt.Errorf("browser only supports requests with GET method")
	}

	context, err := s.browser.NewContext(playwright.BrowserNewContextOptions{
		// From docs when using Browser.Route: "We recommend disabling
		// Service Workers when using request interception by setting
		// `Browser.newContext.serviceWorkers` to `'block'`."
		ServiceWorkers: playwright.ServiceWorkerPolicyBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create context: %w", err)
	}
	defer context.Close()

	fulfill := func(route playwright.Route, fn func() (*Page, error)) {
		page, err := fn()
		if err != nil {
			log.Err(err).Msg("failed to fulfill route")
			err2 := route.Fulfill(playwright.RouteFulfillOptions{
				Status: playwright.Int(http.StatusInternalServerError),
			})
			if err2 != nil {
				log.Err(err2).Msgf(
					"failed to fulfill route (%d)",
					http.StatusInternalServerError,
				)
			}
			return
		}
		headers := make(map[string]string)
		for key := range page.Response.Header {
			headers[key] = page.Response.Header.Get(key)
		}
		err = route.Fulfill(playwright.RouteFulfillOptions{
			Body: page.Response.Body,
			// ContentType: why is this separate from headers?
			Headers: headers,
			Status:  playwright.Int(page.Response.StatusCode),
		})
		if err != nil {
			log.Err(err).Msgf(
				"failed to fulfill route (%d)",
				page.Response.StatusCode,
			)
			return
		}
	}
	err = context.Route("**/*", func(route playwright.Route) {
		fulfill(route, func() (*Page, error) {
			req := route.Request()
			r, err := http.NewRequest(req.Method(), req.URL(), nil)
			if err != nil {
				return nil, fmt.Errorf("failed to make new request: %w", err)
			}
			// req.Headers() does not include security headers such
			// as, as opposed to req.AllHeaders(). And the headers
			// are lower-cased.
			r.Header = make(http.Header)
			for k, v := range req.Headers() {
				r.Header.Set(k, v)
			}
			return s.fetchPlain(ctx, r, reqBody, opts)
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to intercept browser routes: %w", err)
	}

	browserPage, err := context.NewPage()
	if err != nil {
		return nil, fmt.Errorf("could not create page: %w", err)
	}

	browserPage.On("request", func(req playwright.Request) {
		log.Debug().
			Str("url", req.URL()).
			Str("method", req.Method()).
			Msg("browser making page request")
	})
	resp, err := browserPage.Goto(req.URL.String(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
	})
	if err != nil {
		return nil, fmt.Errorf("page failed to goto url: %w", err)
	}
	html, err := browserPage.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get content from page: %w", err)
	}
	header := make(http.Header)
	allHeaders, err := resp.AllHeaders()
	if err != nil {
		return nil, fmt.Errorf("failed to get page response headers: %w", err)
	}
	for key, value := range allHeaders {
		header.Set(key, value)
	}
	return &Page{
		Meta: PageMeta{
			Version:   LatestPageVersion,
			Source:    "http.browser",
			ScrapedAt: time.Now(),
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: resp.URL(),
			Method:        req.Method,
			Header:        header,
			Body:          reqBody,
		},
		Response: PageResponse{
			StatusCode: resp.Status(),
			Header:     header,
			Body:       []byte(html),
		},
	}, nil
}

func (s *Scraper) do(
	ctx context.Context,
	req *http.Request,
	opts doOptions,
	fetchFn fetchFn,
) (page *Page, err error) {
	start := time.Now()

	bkey, reqBody, err := s.blobKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob key: %w", err)
	}

	if !opts.Replace {
		b, err := s.bucket.GetBlob(ctx, bkey)
		if !errors.As(err, lo.ToPtr(&blob.NotFoundError{})) {
			if err != nil {
				return nil, fmt.Errorf("failed to read from blob: %w", err)
			}
			page := new(Page)
			if err := json.Unmarshal(b.Data, page); err != nil {
				return nil, fmt.Errorf("failed to unmarshal page: %w", err)
			}
			if err := errPageStatusNotOK(page); err != nil {
				return nil, err
			}
			page.Meta.Source = b.Source
			return page, nil
		}
	}

	if opts.Limiter != nil {
		rctx := req.Context()
		rctx = context.WithValue(rctx, ctxKeyLimiter{}, ctxValLimiter{opts.Limiter})
		req = req.WithContext(rctx)
	}
	page, err = fetchFn(ctx, req, reqBody, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page: %w", err)
	}
	b, err := json.Marshal(page)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal page: %w", err)
	}
	if err := s.bucket.SetBlob(ctx, bkey, b); err != nil {
		return nil, fmt.Errorf("failed to write page: %w", err)
	}
	if err := errPageStatusNotOK(page); err != nil {
		return nil, err
	}
	log.Info().
		Stringer("url", req.URL).
		Int("status", page.Response.StatusCode).
		Int("resp_bytes", len(page.Response.Body)).
		Stringer("dur", time.Since(start).Round(time.Millisecond)).
		Str("content_type", page.Response.Header.Get("Content-Type")).
		Str("req_body", string(reqBody)).
		Msg("fetched http page")
	return page, nil
}

func (s *Scraper) blobKey(req *http.Request) (string, []byte, error) {
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString(req.URL.String()); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString("."); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString(req.Method); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString("."); err != nil {
		return "", nil, err
	}
	if err := req.Header.WriteSubset(buf, nil); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString("."); err != nil {
		return "", nil, err
	}
	body, err := s.peekRequestBody(req)
	if err != nil {
		return "", nil, err
	}
	if _, err := buf.Write(body); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString("."); err != nil {
		return "", nil, err
	}
	h := sha256.Sum256(buf.Bytes())
	henc := base64.RawURLEncoding.EncodeToString(h[:])
	bkey := filepath.Join(req.URL.Hostname(), henc) + ".json"
	return bkey, body, nil
}

// Preserve and limit the original request body for multiple reads:
// once for persistence on the Page, and then again to restore the
// original request's then drained body.
func (s *Scraper) peekRequestBody(req *http.Request) ([]byte, error) {
	var body []byte
	// MaxBytesReader panics if req.Body is nil, so we must protect against
	// it.
	if req.Body != nil {
		rdr := req.Body
		if s.requestBodyLimit > 0 {
			rdr = http.MaxBytesReader(nil, req.Body, s.requestBodyLimit)
		}
		var err error
		body, err = io.ReadAll(rdr)
		if err != nil {
			return nil, fmt.Errorf("failed to read http req body: %w", err)
		}
	}
	// Best to always do this even when req.Body == nil, to avoid subtle
	// downstream differences between the original body and io.NopCloser.
	req.Body = io.NopCloser(bytes.NewBuffer(body))
	return body, nil
}

type DoOption interface {
	doOption()
}

type OptDoReplace struct{}

type OptDoSilentThrottle struct {
	PageBytesRegexp *regexp.Regexp
}

type ctxKeyLimiter struct{}
type ctxValLimiter struct {
	Limiter Limiter
}

type OptDoLimiter struct {
	Limiter Limiter
}

type Limiter interface {
	Take() time.Time
}

type OptDoBrowser struct{}

func (o *OptDoReplace) doOption()        {}
func (o *OptDoSilentThrottle) doOption() {}
func (o *OptDoLimiter) doOption()        {}
func (o *OptDoBrowser) doOption()        {}

var _ retryablehttp.LeveledLogger = (*leveledLogger)(nil)

type leveledLogger struct{ log *zerolog.Logger }

func (l leveledLogger) fields(keysAndValues []any) *zerolog.Logger {
	log := l.log.With().CallerWithSkipFrameCount(3)
	for i := 0; i < len(keysAndValues); i += 2 {
		key := fmt.Sprintf("%v", keysAndValues[i])
		val := keysAndValues[i+1]
		switch val := val.(type) {
		case int:
			log = log.Int(key, val)
		case string:
			log = log.Str(key, val)
		case fmt.Stringer:
			log = log.Stringer(key, val)
		default:
			log = log.Interface(key, val)
		}
	}
	return lo.ToPtr(log.Logger())
}

func (l leveledLogger) Error(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Error().Msg(msg)
}

func (l leveledLogger) Warn(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Warn().Msg(msg)
}

func (l leveledLogger) Info(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Trace().Msg(msg)
}

func (l leveledLogger) Debug(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Trace().Msg(msg)
}

const LatestPageVersion = 0

type Page struct {
	Meta     PageMeta     `json:"meta"`
	Request  PageRequest  `json:"request"`
	Response PageResponse `json:"response"`
}

type PageMeta struct {
	Version     uint16        `json:"version"`
	Source      string        `json:"-"`
	RetrieveDur time.Duration `json:"-"`
	ScrapedAt   time.Time     `json:"scraped_at"`
	FetchDur    time.Duration `json:"fetch_dur"`
}

type PageRequest struct {
	URL           string      `json:"url"`
	RedirectedURL string      `json:"redirected_url,omitempty"`
	Method        string      `json:"method"`
	Header        http.Header `json:"header,omitempty"`
	Body          []byte      `json:"body,omitempty"`
}

type PageResponse struct {
	StatusCode int         `json:"status_code"`
	Header     http.Header `json:"header,omitempty"`
	Body       []byte      `json:"body,omitempty"`
}
