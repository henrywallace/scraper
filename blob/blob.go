package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

var defaultCacheDir = ".cache/scraper/blob/"

type Bucket struct {
	prefix string
	bucket *blob.Bucket
	cache  *badger.DB
}

func NewBucket(
	ctx context.Context,
	bucketURL string,
	options ...BucketOption,
) (*Bucket, error) {
	log := log.Ctx(ctx)
	var disableCache bool
	for _, opt := range options {
		switch opt := opt.(type) {
		case *OptBucketNoCache:
			disableCache = opt.NoCache
		}
	}
	var cache *badger.DB
	var err error
	if !disableCache {
		cacheDir := defaultCacheDir
		for _, opt := range options {
			switch opt := opt.(type) {
			case *OptBucketCacheDir:
				cacheDir = opt.CacheDir
			}
		}
		cacheOpts := badger.DefaultOptions(cacheDir)
		cacheOpts.Logger = newBadgerLogger(*log)
		cache, err = badger.Open(cacheOpts)
		if err != nil {
			return nil, err
		}
		log.Debug().Str("dir", cacheDir).Msg("opened badger cache")
	}
	bucket, err := newBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		prefix: "",
		bucket: bucket,
		cache:  cache,
	}, nil
}

type BucketOption interface {
	bucketOption()
}

type OptBucketCacheDir struct {
	CacheDir string
}

type OptBucketNoCache struct {
	NoCache bool
}

func (o *OptBucketCacheDir) bucketOption() {}
func (o *OptBucketNoCache) bucketOption()  {}

var reBucketURL = regexp.MustCompile(`^\w+://`)

func newBucket(
	ctx context.Context,
	bucketURL string,
) (*blob.Bucket, error) {
	log := log.Ctx(ctx)
	if !reBucketURL.MatchString(bucketURL) {
		bucketURL = "file://" + bucketURL
	}
	var bucket *blob.Bucket
	var err error
	switch {
	case strings.HasPrefix(bucketURL, "file://"):
		dir := strings.TrimPrefix(bucketURL, "file://")
		bucket, err = fileblob.OpenBucket(dir, &fileblob.Options{
			CreateDir: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open fileblob bucket %s: %w", dir, err)
		}
	case strings.HasPrefix(bucketURL, "s3://"):
		bucketName := strings.TrimRight(strings.TrimPrefix(bucketURL, "s3://"), "/")
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load default config: %w", err)
		}
		client := s3.NewFromConfig(cfg)
		bucket, err = s3blob.OpenBucketV2(ctx, client, bucketName, &s3blob.Options{})
		if err != nil {
			return nil, fmt.Errorf("failed to open s3 bucket %s: %w", bucketName, err)
		}
	default:
		return nil, fmt.Errorf(
			"unsupported bucket-url %s, supported schemes are file, s3",
			bucketURL,
		)
	}
	log.Debug().Str("url", bucketURL).Msg("opened bucket")
	return bucket, nil
}

// WithPrefix returns a new bucket with the given prefix.
func (bu *Bucket) WithPrefix(prefix string) *Bucket {
	prefix = filepath.Join(bu.prefix, prefix)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var bucket *blob.Bucket
	if bu.bucket != nil {
		bucket = blob.PrefixedBucket(bu.bucket, prefix)
	}
	return &Bucket{
		prefix: prefix,
		bucket: bucket,
		cache:  bu.cache,
	}
}

func (bu *Bucket) Close() {
	if bu.cache != nil {
		if err := bu.cache.Close(); err != nil {
			log.Err(err).Msg("failed to close cache")
		}
	}
	if bu.bucket != nil {
		if err := bu.bucket.Close(); err != nil {
			log.Err(err).Msg("failed to close bucket")
		}
	}
}

func (bu *Bucket) Exists(ctx context.Context, key string) (ok bool, err error) {
	start := time.Now()
	source := "remote"
	defer func() {
		if err != nil {
			return
		}
		log.Debug().
			Bool("exists", ok).
			Stringer("dur", time.Since(start).Round(time.Microsecond)).
			Str("source", source).
			Msg("bucket exists")
	}()
	key += ".zst"
	exists := false
	if bu.cache != nil {
		err := bu.cache.View(func(txn *badger.Txn) error {
			_, err := txn.Get(bu.cacheKey(key))
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				exists = true
			}
			return nil
		})
		if err != nil {
			log.Err(err).Msg("failed to check cache for existence")
		}
	}
	if exists {
		source = "cache"
		return exists, nil
	}
	if bu.bucket != nil {
		return bu.bucket.Exists(ctx, key)
	}
	return false, nil
}

func (bu *Bucket) Write(ctx context.Context, key string, data []byte) error {
	key += ".zst"
	if bu.bucket != nil {
		var opts *blob.WriterOptions
		w, err := bu.bucket.NewWriter(ctx, key, opts)
		if err != nil {
			return fmt.Errorf("failed to create bucket writer: %w", err)
		}
		var _ io.Writer
		zw := zstd.NewWriter(w)
		n, err := zw.Write(data)
		if err != nil {
			_ = zw.Close()
			_ = w.Close()
			return err
		}
		if n < len(data) {
			_ = zw.Close()
			_ = w.Close()
			return fmt.Errorf("violation of io.Writer interface")
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("failed to close zstd writer: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close bucket writer: %w", err)
		}
	}
	if bu.cache != nil {
		err := bu.cache.Update(func(txn *badger.Txn) error {
			return txn.Set(bu.cacheKey(key), data)
		})
		if err != nil {
			log.Err(err).Msg("failed to set cache")
		}
	}
	return nil
}

// NotFoundError is returned when a key is not found in the bucket.
type NotFoundError struct {
	Key string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
}

type Blob struct {
	Data   []byte
	Source string
}

func (bu *Bucket) Read(ctx context.Context, key string) (b *Blob, err error) {
	if bu.cache == nil && bu.bucket == nil {
		return nil, errors.New("neither cache nor external bucket is configured")
	}

	start := time.Now()
	defer func() {
		if err != nil {
			return
		}
		log.Debug().
			Int("bytes", len(b.Data)).
			Stringer("dur", time.Since(start).Round(time.Microsecond)).
			Str("source", b.Source).
			Str("key", key).
			Msg("bucket read")
	}()
	key = key + ".zst"

	var cacheData []byte
	if bu.cache != nil {
		err := bu.cache.View(func(txn *badger.Txn) error {
			item, err := txn.Get(bu.cacheKey(key))
			if err == nil {
				cacheData, err = item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return nil
			}
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &NotFoundError{Key: key}
			}
			return nil
		})
		if err != nil && !errors.As(err, lo.ToPtr(&NotFoundError{})) {
			if bu.bucket == nil {
				return nil, fmt.Errorf("failed to read cache: %w", err)
			}
			log.Err(err).Msg("failed to read cache")
		}
	}
	if cacheData != nil {
		return &Blob{Data: cacheData, Source: "cache"}, nil
	}

	if bu.bucket != nil {
		var opts *blob.ReaderOptions
		r, err := bu.bucket.NewReader(ctx, key, opts)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				return nil, &NotFoundError{key}
			}
			return nil, fmt.Errorf("failed to create bucket reader: %w", err)
		}
		zr := zstd.NewReader(r)
		data, err := io.ReadAll(zr)
		if err != nil {
			_ = zr.Close()
			_ = r.Close()
			return nil, err
		}
		if err := zr.Close(); err != nil {
			return nil, fmt.Errorf("failed to close zstd reader: %w", err)
		}
		if err := r.Close(); err != nil {
			return nil, fmt.Errorf("failed to close bucket reader: %w", err)
		}
		if cacheData == nil && bu.cache != nil {
			err := bu.cache.Update(func(txn *badger.Txn) error {
				return txn.Set(bu.cacheKey(key), data)
			})
			if err != nil {
				log.Err(err).Msg("failed to set cache")
			}
		}
		return &Blob{Data: data, Source: "remote"}, nil
	}

	return nil, &NotFoundError{Key: key}
}

func (bu *Bucket) List(options ...ListOption) *ListIterator {
	prefix := mo.None[string]()
	for _, opt := range options {
		switch opt := opt.(type) {
		case *OptListPrefix:
			prefix = mo.Some(opt.Prefix)
		default:
			panic(fmt.Sprintf("invalid option type %T", opt))
		}
	}
	// FIXME need support for cache-only
	it := bu.bucket.List(&blob.ListOptions{
		Prefix: prefix.OrElse(""),
	})
	return &ListIterator{
		b:    bu,
		it:   it,
		obj:  nil,
		err:  nil,
		done: false,
	}
}

type ListOption interface {
	listOption()
}

type OptListPrefix struct {
	Prefix string
}

func (o *OptListPrefix) listOption() {}

type ListIterator struct {
	b    *Bucket
	it   *blob.ListIterator
	obj  *blob.ListObject
	err  error
	done bool
}

func (it *ListIterator) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	obj, err := it.it.Next(ctx)
	if errors.Is(err, io.EOF) {
		it.done = true
		return false
	}
	it.obj = obj
	return true
}

func (it *ListIterator) Err() error {
	return it.err
}

func (it *ListIterator) Key() string {
	return strings.TrimSuffix(it.obj.Key, ".zst")
}

func (it *ListIterator) Value(ctx context.Context) (*Blob, error) {
	return it.b.Read(ctx, it.Key())
}

func (bu *Bucket) cacheKey(key string) []byte {
	return []byte(bu.prefix + key)
}

var _ badger.Logger = (*badgerLogger)(nil)

type badgerLogger struct {
	zerolog.Logger
}

func newBadgerLogger(log zerolog.Logger) badgerLogger {
	log = log.With().
		Str("component", "cache").
		Caller().
		CallerWithSkipFrameCount(5).
		Logger()
	return badgerLogger{log}
}

func (l badgerLogger) Errorf(format string, args ...any) {
	l.log(zerolog.ErrorLevel, format, args)
}

func (l badgerLogger) Warningf(format string, args ...any) {
	l.log(zerolog.WarnLevel, format, args)
}

// lower by 1, as these logs can be noisy
func (l badgerLogger) Infof(format string, args ...any) {
	l.log(zerolog.DebugLevel, format, args)
}

// lower by 1, as these logs can be noisy
func (l badgerLogger) Debugf(format string, args ...any) {
	l.log(zerolog.TraceLevel, format, args)
}

func (l badgerLogger) log(lvl zerolog.Level, format string, args []any) {
	format = strings.TrimSpace(format) // some log messages misbehave
	l.Logger.WithLevel(lvl).Msgf(format, args...)
}
