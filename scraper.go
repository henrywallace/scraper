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
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/ratelimit"

	"github.com/henrywallace/scraper/blob"
)

var veryStart = time.Now()
var requests atomic.Uint64

var envRateLimit = "SCRAPER_RATE_LIMIT"
var rateLimitOverride ratelimit.Limiter

var reNumbericPrefix = regexp.MustCompile(`^\d+`)

func init() {
	rateLimitRaw, ok := os.LookupEnv(envRateLimit)
	if !ok {
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
			if err != nil {
				log.Fatal().Msgf("failed to parse %s=%q: %v", envRateLimit, rateLimitRaw, err)
			}
		}
		opts = append(opts, ratelimit.Per(dur))
	}
	rateLimitOverride = ratelimit.New(int(rate), opts...)
}

type Scraper struct {
	httpClient      *retryablehttp.Client
	bucket          *blob.Bucket
	mu              *sync.Mutex
	pw              *playwright.Playwright
	browser         playwright.Browser
	alwaysDoBrowser bool
	browserReady    chan struct{}
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
			}
		}
		requests.Add(1)
	}
	s := &Scraper{
		httpClient:      httpClient,
		bucket:          blob,
		mu:              new(sync.Mutex),
		browser:         nil,
		alwaysDoBrowser: false,
		browserReady:    make(chan struct{}),
		pw:              nil,
	}
	for _, opt := range opts {
		opt.option(s)
	}
	p := pool.New().WithErrors()
	p.Go(func() error {
		return s.startBrowser(doOptions{
			Replace:          false,
			ReSilentThrottle: nil,
			Limiter:          nil,
		})
	})
	if err := p.Wait(); err != nil {
		return nil, err
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

func (s *Scraper) startBrowser(_ doOptions) (err error) {
	start := time.Now()

	s.mu.Lock()
	defer func() {
		if err == nil {
			close(s.browserReady)
		}
		s.mu.Unlock()
	}()

	// proxyUsername := cryptoRandomString(4)
	// proxyPassword := cryptoRandomString(20)
	// proxyUsername := "user"
	// proxyPassword := "pass"
	// handler := http.NewServeMux()
	// handler.HandleFunc("/", &proxyHandler{
	// 	proxyUsername: proxyUsername,
	// 	proxyPassword: proxyPassword,
	// })
	// proxy := goproxy.NewProxyHttpServer()
	// proxy.OnRequest().HandleConnectFunc(func(host string, ctx *goproxy.ProxyCtx) (*goproxy.ConnectAction, string) {
	// 	return goproxy.MitmConnect, host
	// })
	// proxy.OnRequest().DoFunc(func(req *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
	// 	// Your custom logic goes here, e.g. interacting with the cache

	// 	// Forward the request as-is
	// 	return req, nil
	// })

	// server := &http.Server{
	// 	Addr: "localhost:8000",
	// 	Handler: &proxyHandler{
	// 		opts: opts,
	// 		s:    s,
	// 	},
	// 	// ReadTimeout:    10 * time.Second,
	// 	// WriteTimeout:   10 * time.Second,
	// 	// MaxHeaderBytes: 1 << 20,
	// }

	// // serverReady := make(chan struct{})
	// serverErrs := make(chan error)
	// go func() {
	// 	log.Field("addr", server.Addr).
	// 		Debugf(ctx, "starting scraper proxy server for browser")
	// 	if err := server.ListenAndServe(); err != nil {
	// 		serverErrs <- fmt.Errorf("failed to start proxy server: %w", err)
	// 		return
	// 	}
	// }()

	// go func() {
	// 	time.Sleep(4 * time.Second)
	// 	conn, err := net.DialTimeout("tcp", server.Addr, 2*time.Second)
	// 	if err != nil {
	// 		serverErrs <- fmt.Errorf("failed to connect to proxy server: %w", err)
	// 		return
	// 	}
	// 	if err := conn.Close(); err != nil {
	// 		serverErrs <- fmt.Errorf("failed to close proxy connection: %w", err)
	// 		return
	// 	}
	// 	close(serverReady)
	// }()
	// serverReady:
	// for {
	// 	select {
	// 	case err := <-serverErrs:
	// 		return err
	// 	case <-serverReady:
	// 		break serverReady
	// 	}
	// }

	log.Debug().Msg("starting playwright instance")
	pw, err := playwright.Run(&playwright.RunOptions{
		Verbose: true,
	})
	if err != nil {
		fmt.Printf("failed to run playwright instance: %#v\n", err)
		return fmt.Errorf("failed to run playwright instance: %w", err)
	}
	s.pw = pw
	log.Debug().Msg("starting headless chromium browser")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(true),
		// Proxy: &playwright.BrowserTypeLaunchOptionsProxy{
		// 	Server:   playwright.String(proxyURL),
		// 	Username: playwright.String(proxyUsername),
		// 	Password: &proxyPassword,
		// 	Bypass:   playwright.String("clients2.google.com"),
		// },
		// TracesDir: playwright.String("traces"),
	})
	if err != nil {
		return err
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

// const urlSafeChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// func cryptoRandomString(length int) string {
// 	result := make([]byte, length)
// 	charCount := big.NewInt(int64(len(urlSafeChars)))

// 	for i := 0; i < length; i++ {
// 		index, err := crand.Int(crand.Reader, charCount)
// 		if err != nil {
// 			panic(err)
// 		}
// 		result[i] = urlSafeChars[index.Int64()]
// 	}

// 	return string(result)

// }

// func authenticate(
// 	authHeader string,
// 	expectedUsername string,
// 	expectedPassword string,
// ) bool {
// 	if authHeader == "" {
// 		return false
// 	}
// 	if !strings.HasPrefix(authHeader, "Basic ") {
// 		return false
// 	}
// 	encodedCredentials := strings.TrimPrefix(authHeader, "Basic ")
// 	decodedCredentials, err := base64.StdEncoding.DecodeString(encodedCredentials)
// 	if err != nil {
// 		return false
// 	}
// 	credentials := strings.SplitN(string(decodedCredentials), ":", 2)
// 	if len(credentials) != 2 {
// 		return false
// 	}
// 	username := credentials[0]
// 	password := credentials[1]
// 	return username == expectedUsername && password == expectedPassword
// }

// type proxyHandler struct {
// 	opts doOptions
// 	s    *Scraper
// }

// func (p *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	ctx := r.Context()
// 	p.s.log.Field("addr", r.RemoteAddr).
// 		Field("method", r.Method).
// 		Field("url", r.URL.String()).
// 		Field("host", r.Host).
// 		Fieldf("headers", "%v", r.Header).
// 		Debugf(ctx, "proxying request")

// 	if r.URL.Scheme != "http" && r.URL.Scheme != "https" {
// 		msg := "unsupported protocol scheme " + r.URL.Scheme
// 		p.s.log.Errorf(ctx, msg)
// 		http.Error(w, msg, http.StatusBadRequest)
// 		return
// 	}

// 	// Make a new request using the incoming request's URL and headers
// 	req, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
// 	if err != nil {
// 		p.s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to make new request")
// 		http.Error(w, "Bad request", http.StatusBadRequest)
// 		return
// 	}
// 	req.Header = r.Header

// 	page, err := p.s.do(ctx, req, p.opts, p.s.fetchPlain)
// 	if err != nil {
// 		p.s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to do scrape request")
// 		http.Error(w, "Bad request", http.StatusInternalServerError)
// 		return
// 	}

// 	// Copy the response headers and body to the proxy response
// 	for k, v := range page.Response.Header {
// 		w.Header()[k] = v
// 	}
// 	w.WriteHeader(page.Response.StatusCode)
// 	_, err = io.Copy(w, bytes.NewReader(page.Response.Body))
// 	if err != nil {
// 		p.s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to copy response")
// 		http.Error(w, "Bad request", http.StatusBadRequest)
// 		return
// 	}
// }

type doOptions struct {
	Replace          bool
	ReSilentThrottle *regexp.Regexp
	Limiter          Limiter
}

type fetchFn func(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error)

func (s *Scraper) fetchPlain(
	_ context.Context,
	req *http.Request,
	reqBody []byte,
	_ doOptions,
) (*Page, error) {
	rreq, err := retryablehttp.FromRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(rreq)
	if err != nil {
		return nil, fmt.Errorf("failed to perform http get: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read http resp body: %w", err)
	}
	resp.Body.Close()
	redirect := ""
	if resp.Request.URL.String() != req.URL.String() {
		redirect = resp.Request.URL.String()
	}
	return &Page{
		ScrapedAt: time.Now(),
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
	_ []byte,
	opts doOptions,
) (*Page, error) {
	if s.browser == nil {
		panic("headless browser not running")
	}
	if req.Method != "GET" {
		return nil, fmt.Errorf("browser only supports requests with GET method")
	}

	context, err := s.browser.NewContext(playwright.BrowserNewContextOptions{})
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
			// ContentType: why is this separate?
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
			// It is currently unsuppored to get all headers with
			// ongoing MITM route. Blocks forever.
			// https://github.com/playwright-community/playwright-go/blob/2586b38296886f7dcf63b305cb23791a4d84617d/request.go#L145
			r.Header = make(http.Header)
			for k, v := range req.Headers() {
				r.Header.Set(k, v)
			}
			return s.do(ctx, r, opts, s.fetchPlain)
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to set browser context route: %w", err)
	}

	page, err := context.NewPage()
	if err != nil {
		return nil, fmt.Errorf("could not create page: %w", err)
	}

	page.On("request", func(req playwright.Request) {
		log.Debug().
			Str("url", req.URL()).
			Str("method", req.Method()).
			Msg("making page request")
	})
	page.On("response", func(resp playwright.Response) {
		b, _ := resp.Body()
		log.Debug().
			Str("url", resp.URL()).
			Int("status", resp.Status()).
			// Stringer("dur", time.Since(resp.Request().Timing().StartTime))  FIXME
			Int("bytes", len(b)).
			Msg("got page response")
	})

	resp, err := page.Goto(req.URL.String(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
	})
	if err != nil {
		return nil, fmt.Errorf("page failed to goto url: %w", err)
	}
	html, err := page.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get content from page: %w", err)
	}
	respHeaders := make(http.Header)
	allHeaders, err := resp.AllHeaders()
	if err != nil {
		return nil, fmt.Errorf("failed to get page response headers: %w", err)
	}
	for key, value := range allHeaders {
		respHeaders.Set(key, value)
	}
	return &Page{
		ScrapedAt: time.Now(),
		Request: PageRequest{
			URL: resp.URL(),
			// Unknown
			// RedirectedURL: redirect,
			Method: req.Method,
			Header: respHeaders,
			// XXX
			Body: nil,
		},
		Response: PageResponse{
			Body: []byte(html),
		},
	}, nil
}

func (s *Scraper) do(
	ctx context.Context,
	req *http.Request,
	opts doOptions,
	_ fetchFn,
) (page *Page, err error) {
	start := time.Now()

	bkey, reqBody, err := s.blobKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob key: %w", err)
	}

	if !opts.Replace {
		b, err := s.bucket.Read(ctx, bkey)
		errNoExist := &blob.NotFoundError{}
		if !errors.As(err, &errNoExist) {
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
			return page, nil
		}
	}

	if opts.Limiter != nil {
		rctx := req.Context()
		rctx = context.WithValue(rctx, ctxKeyLimiter{}, ctxValLimiter{opts.Limiter})
		req = req.WithContext(rctx)
	}
	// Retry, as reading the body can fail outside the purview of the
	// retryablehttp api. Or, the read body could indicate that the request
	// should be retried. Adding it to the CheckRetry func would be awkward
	// as it would involve conditionally forwarding an already read body.
	var resp *http.Response
	var body []byte
	attemptsMax := 7
	waitMin := 1 * time.Second
	waitMax := 4 * time.Minute
	waitJitter := 1 * time.Second
	wait := func(attempt int) {
		d := time.Duration(math.Pow(2, float64(attempt))) * waitMin
		d += time.Duration(rand.Intn(int(waitJitter)))
		if d > waitMax {
			d = waitMax
		}
		time.Sleep(d)
	}
	for i := 0; i < attemptsMax; i++ {
		rreq, err := retryablehttp.FromRequest(req)
		if err != nil {
			return nil, err
		}
		resp, err = s.httpClient.Do(rreq)
		if err != nil {
			return nil, fmt.Errorf("failed to perform http get: %w", err)
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		lastAttempt := i >= attemptsMax-1
		if err != nil {
			if lastAttempt {
				return nil, fmt.Errorf("failed to read http resp body: %w", err)
			}
			log.Warn().Err(err).Int("attempt", i).Msg("failed to read http resp body, retrying")
			wait(i)
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
			time.Sleep(10 * time.Second)
			wait(i)
			continue
		}
		break
	}

	redirect := ""
	if resp.Request.URL.String() != req.URL.String() {
		redirect = resp.Request.URL.String()
	}
	page = &Page{
		ScrapedAt: time.Now(),
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
	}
	b, err := json.Marshal(page)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal page: %w", err)
	}
	if err := s.bucket.Write(ctx, bkey, b); err != nil {
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

// func (s *Scraper) doBrowser2(
// 	ctx context.Context,
// 	req *http.Request,
// 	opts doOptions,
// ) (*Page, error) {
// 	if s.browser == nil {
// 		panic("headless browser not running")
// 	}
// 	if req.Method != "GET" {
// 		return nil, fmt.Errorf("browser only supports requests with GET method")
// 	}

// 	context, err := s.browser.NewContext(playwright.BrowserNewContextOptions{})
// 	if err != nil {
// 		return nil, fmt.Errorf("could not create context: %w", err)
// 	}
// 	defer context.Close()
// 	go func() {
// 		<-ctx.Done()
// 		context.Close()
// 	}()

// 	fulfill := func(route playwright.Route, fn func() (*Page, error)) {
// 		page, err := fn()
// 		if err != nil {
// 			s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to fulfill route")
// 			err := route.Fulfill(playwright.RouteFulfillOptions{
// 				Status: playwright.Int(http.StatusInternalServerError),
// 			})
// 			if err != nil {
// 				s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to fulfill route (error)")
// 			}
// 			return
// 		}
// 		headers := make(map[string]string)
// 		for key := range page.Response.Header {
// 			headers[key] = page.Response.Header.Get(key)
// 		}
// 		err = route.Fulfill(playwright.RouteFulfillOptions{
// 			Body: page.Response.Body,
// 			// ContentType: why is this separate?
// 			Headers: headers,
// 			Status:  playwright.Int(page.Response.StatusCode),
// 		})
// 		if err != nil {
// 			s.log.Fieldf("error", "%v", err).Errorf(ctx, "failed to fulfill route (error)")
// 		}
// 	}
// 	err = context.Route("**/*", func(route playwright.Route) {
// 		fulfill(route, func() (*Page, error) {
// 			req := route.Request()
// 			r, err := http.NewRequest(req.Method(), req.URL(), nil)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to make new request: %w", err)
// 			}
// 			// It is currently unsuppored to get all headers with
// 			// ongoing MITM route. Blocks forever.
// 			// https://github.com/playwright-community/playwright-go/blob/2586b38296886f7dcf63b305cb23791a4d84617d/request.go#L145
// 			r.Header = make(http.Header)
// 			for k, v := range req.Headers() {
// 				r.Header.Set(k, v)
// 			}
// 			return s.do(ctx, r, opts, s.fetchPlain)
// 		})
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to set browser context route: %w", err)
// 	}

// 	page, err := context.NewPage()
// 	if err != nil {
// 		return nil, fmt.Errorf("could not create page: %w", err)
// 	}

// 	page.On("request", func(request playwright.Request) {
// 		s.log.Field("url", request.URL()).Debugf(ctx, "making page request")
// 	})
// 	page.On("response", func(request playwright.Response) {
// 		s.log.Field("url", request.URL()).Debugf(ctx, "got page response")
// 	})

// 	resp, err := page.Goto(req.URL.String(), playwright.PageGotoOptions{
// 		WaitUntil: playwright.WaitUntilStateNetworkidle,
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("page failed to goto url: %w", err)
// 	}
// 	html, err := page.Content()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get content from page: %w", err)
// 	}
// 	respHeaders := make(http.Header)
// 	allHeaders, err := resp.AllHeaders()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get page response headers: %w", err)
// 	}
// 	for key, value := range allHeaders {
// 		respHeaders.Set(key, value)
// 	}
// 	return &Page{
// 		ScrapedAt: time.Now(),
// 		Request: PageRequest{
// 			URL: resp.URL(),
// 			// Unknown
// 			// RedirectedURL: redirect,
// 			Method: req.Method,
// 			Header: respHeaders,
// 			// XXX
// 			Body: nil,
// 		},
// 		Response: PageResponse{
// 			Body: []byte(html),
// 		},
// 	}, nil
// }

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

	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return "", nil, err
		}
	}
	if _, err := buf.Write(body); err != nil {
		return "", nil, err
	}
	if _, err := buf.WriteString("."); err != nil {
		return "", nil, err
	}
	req.Body = io.NopCloser(bytes.NewBuffer(body))

	h := sha256.Sum256(buf.Bytes())
	henc := base64.RawURLEncoding.EncodeToString(h[:])
	bkey := filepath.Join(req.URL.Hostname(), henc) + ".json"
	return bkey, body, nil
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
	log := l.log.With()
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

type Page struct {
	ScrapedAt time.Time    `json:"scraped_at"`
	Request   PageRequest  `json:"request"`
	Response  PageResponse `json:"response"`
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
