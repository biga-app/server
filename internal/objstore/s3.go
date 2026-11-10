package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config says where an S3-compatible store is and how to sign in.
type S3Config struct {
	// Endpoint is the host and optional port, without a scheme.
	Endpoint string
	Bucket   string
	Region   string
	// AccessKey and SecretKey sign requests. When both are empty, credentials
	// come from the AWS environment variables or the instance's IAM role.
	AccessKey string
	SecretKey string
	// Insecure talks plain HTTP, for a store on a private network.
	Insecure bool
}

// S3 is a [Store] on any S3-compatible service: AWS, MinIO, Cloudflare R2 and
// the like.
type S3 struct {
	client *minio.Client
	bucket string
	region string
}

// NewS3 connects to the service. It does not create the bucket.
func NewS3(cfg S3Config) (*S3, error) {
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.IAM{},
	})
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: creds, Secure: !cfg.Insecure, Region: cfg.Region})
	if err != nil {
		return nil, fmt.Errorf("object storage: %w", err)
	}
	return &S3{client: client, bucket: cfg.Bucket, region: cfg.Region}, nil
}

func (s *S3) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, notFound(err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, notFound(err)
	}
	return data, nil
}

func (s *S3) DeletePrefix(ctx context.Context, prefix string) error {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keys := make(chan minio.ObjectInfo)
	var listErr error
	go func() {
		defer close(keys)
		for obj := range s.client.ListObjects(listCtx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				listErr = obj.Err
				return
			}
			select {
			case keys <- obj:
			case <-listCtx.Done():
				return
			}
		}
	}()
	for rm := range s.client.RemoveObjects(ctx, s.bucket, keys, minio.RemoveObjectsOptions{}) {
		if rm.Err != nil {
			return fmt.Errorf("remove %s: %w", rm.ObjectName, rm.Err)
		}
	}
	return listErr
}

func (s *S3) Delete(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil && !isNotFound(err) {
			return err
		}
	}
	return nil
}

// EnsureBucket creates the bucket unless it is already there.
func (s *S3) EnsureBucket(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil || ok {
		return err
	}
	err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: s.region})
	if code := minio.ToErrorResponse(err).Code; code == "BucketAlreadyOwnedByYou" {
		return nil
	}
	return err
}

// Ping checks that the bucket exists and the credentials can see it.
func (s *S3) Ping(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket %q does not exist", s.bucket)
	}
	return nil
}

func isNotFound(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

func notFound(err error) error {
	if isNotFound(err) {
		return ErrNotFound
	}
	if errors.Is(err, io.EOF) {
		return ErrNotFound
	}
	return err
}
