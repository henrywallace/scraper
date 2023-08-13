package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/badger/v3"
	"github.com/henrywallace/scraper/logger"
	"github.com/samber/mo"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

var defaultCacheDir = ".cache/scraper/blob/"

type Bucket struct {
	log    *logger.Logger
	prefix string
	bucket *blob.Bucket
	cache  *badger.DB
}

func NewBucket(
	ctx context.Context,
	log *logger.Logger,
	options ...BucketOption,
) (*Bucket, error) {
	var u string
	var disableCache bool
	for _, opt := range options {
		switch opt := opt.(type) {
		case *OptBucketURL:
			u = opt.URL
			if opt.DisableCache {
				disableCache = true
			}
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
		cacheOpts.Logger = &badgerLogger{
			ctx: ctx,
			log: log,
		}
		cache, err = badger.Open(cacheOpts)
		if err != nil {
			return nil, err
		}
	}
	var bucket *blob.Bucket
	if u != "" {
		bucket, err = newBucket(ctx, u)
		if err != nil {
			return nil, err
		}
	}
	return &Bucket{
		log:    log,
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

type OptBucketURL struct {
	URL          string
	DisableCache bool
}

func (o *OptBucketCacheDir) bucketOption() {}
func (o *OptBucketURL) bucketOption()      {}

func newBucket(
	ctx context.Context,
	bucketUrl string,
) (*blob.Bucket, error) {
	var bucket *blob.Bucket
	var err error
	switch {
	case strings.HasPrefix(bucketUrl, "file://"):
		dir := strings.TrimPrefix(bucketUrl, "file://")
		bucket, err = fileblob.OpenBucket(dir, &fileblob.Options{
			CreateDir: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open fileblob bucket %s: %w", dir, err)
		}
	case strings.HasPrefix(bucketUrl, "s3://"):
		bucketName := strings.TrimRight(strings.TrimPrefix(bucketUrl, "s3://"), "/")
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
			bucketUrl,
		)
	}
	return bucket, nil
}

func (b *Bucket) WithPrefix(prefix string) *Bucket {
	prefix = filepath.Join(b.prefix, prefix)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var bucket *blob.Bucket
	if b.bucket != nil {
		bucket = blob.PrefixedBucket(b.bucket, prefix)
	}
	return &Bucket{
		log:    b.log,
		prefix: prefix,
		bucket: bucket,
		cache:  b.cache,
	}
}

func (b *Bucket) Close() {
	ctx := context.Background()
	if b.cache != nil {
		if err := b.cache.Close(); err != nil {
			b.log.Errorf(ctx, "failed to close cache: %v", err)
		}
	}
	if b.bucket != nil {
		if err := b.bucket.Close(); err != nil {
			b.log.Errorf(ctx, "failed to close bucket: %v", err)
		}
	}
}

func (b *Bucket) Exists(ctx context.Context, key string) (ok bool, err error) {
	start := time.Now()
	source := "remote"
	defer func() {
		if err != nil {
			return
		}
		b.log.Fieldf("exists", "%t", ok).
			Fieldf("dur", "%v", time.Since(start).Round(time.Microsecond)).
			Field("source", source).
			Debugf(ctx, "bucket exists")
	}()
	key += ".zst"
	exists := false
	if b.cache != nil {
		err := b.cache.View(func(txn *badger.Txn) error {
			_, err := txn.Get(b.cacheKey(key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err != badger.ErrKeyNotFound {
				exists = true
			}
			return nil
		})
		if err != nil {
			b.log.Errorf(ctx, "failed to check cache for existence: %v", err)
		}
	}
	if exists {
		source = "cache"
		return exists, nil
	}
	if b.bucket != nil {
		return b.bucket.Exists(ctx, key)
	}
	return false, nil
}

func (b *Bucket) Write(ctx context.Context, key string, data []byte) error {
	key += ".zst"
	if b.bucket != nil {
		var opts *blob.WriterOptions
		w, err := b.bucket.NewWriter(ctx, key, opts)
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
	if b.cache != nil {
		err := b.cache.Update(func(txn *badger.Txn) error {
			return txn.Set(b.cacheKey(key), data)
		})
		if err != nil {
			b.log.Errorf(ctx, "failed to set cache: %v", err)
		}
	}
	return nil
}

type ErrNotFound struct {
	Key string
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
}

func (b *Bucket) Read(ctx context.Context, key string) (data []byte, err error) {
	if b.cache == nil && b.bucket == nil {
		return nil, errors.New("neither cache nor external bucket is configured")
	}

	start := time.Now()
	source := "remote"
	defer func() {
		if err != nil {
			return
		}
		b.log.Fieldf("bytes", "%d", len(data)).
			Fieldf("dur", "%v", time.Since(start).Round(time.Microsecond)).
			Field("source", source).
			Field("key", key).
			Debugf(ctx, "bucket read")
	}()
	key = key + ".zst"

	var cacheData []byte
	if b.cache != nil {
		err := b.cache.View(func(txn *badger.Txn) error {
			item, err := txn.Get(b.cacheKey(key))
			if err == nil {
				cacheData, err = item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return nil
			}
			if err != badger.ErrKeyNotFound {
				return &ErrNotFound{Key: key}
			}
			return nil
		})
		if err != nil {
			if b.bucket == nil {
				return nil, fmt.Errorf("failed to read cache: %w", err)
			}
			b.log.Errorf(ctx, "failed to read cache: %v", err)
		}
	}
	if cacheData != nil {
		source = "cache"
		return cacheData, nil
	}

	if b.bucket != nil {
		var opts *blob.ReaderOptions
		r, err := b.bucket.NewReader(ctx, key, opts)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				return nil, &ErrNotFound{key}
			}
			return nil, fmt.Errorf("failed to create bucket reader: %w", err)
		}
		zr := zstd.NewReader(r)
		data, err = io.ReadAll(zr)
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
		if cacheData == nil && b.cache != nil {
			err := b.cache.Update(func(txn *badger.Txn) error {
				return txn.Set(b.cacheKey(key), data)
			})
			if err != nil {
				b.log.Errorf(ctx, "failed to set cache: %v", err)
			}
		}
		return data, nil
	}

	return nil, &ErrNotFound{Key: key}
}

func (b *Bucket) List(
	ctx context.Context,
	options ...ListOption,
) *ListIterator {
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
	it := b.bucket.List(&blob.ListOptions{
		Prefix: prefix.OrElse(""),
	})
	return &ListIterator{
		b:    b,
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
	if err == io.EOF {
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

func (it *ListIterator) Value(ctx context.Context) ([]byte, error) {
	return it.b.Read(ctx, it.Key())
}

func (b *Bucket) cacheKey(key string) []byte {
	return []byte(b.prefix + key)
}

var _ badger.Logger = (*badgerLogger)(nil)

type badgerLogger struct {
	ctx context.Context
	log *logger.Logger
}

func (l *badgerLogger) Errorf(format string, args ...any) {
	l.log.Errorf(l.ctx, format, args...)
}
func (l *badgerLogger) Warningf(format string, args ...any) {
	l.log.Warnf(l.ctx, format, args...)
}
func (l *badgerLogger) Infof(format string, args ...any) {
	l.log.Tracef(l.ctx, format, args...)
}
func (l *badgerLogger) Debugf(format string, args ...any) {
	l.log.Tracef(l.ctx, format, args...)
}
