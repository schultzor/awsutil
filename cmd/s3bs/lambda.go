package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/PaesslerAG/gval"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//  a wrapper for a gzip reader so we retain the original Closer
type gzCloser struct {
	Reader io.Reader
	Closer io.Closer
}

func (g gzCloser) Read(p []byte) (n int, err error) { return g.Reader.Read(p) }
func (g gzCloser) Close() error                     { return g.Closer.Close() }

func scanJsonObjects(ctx context.Context, filter gval.Evaluable, contents io.ReadCloser) ([]string, error) {
	var ret []string
	defer contents.Close()
	decoder := json.NewDecoder(contents)
	for decoder.More() {
		var logEntry map[string]any
		if err := decoder.Decode(&logEntry); err != nil {
			return ret, err
		}
		filterMatch, err := filter.EvalBool(ctx, logEntry)
		if err != nil {
			return ret, err
		}
		if filterMatch {
			if b, err := json.Marshal(logEntry); err == nil {
				ret = append(ret, string(b))
			}
		}
	}
	return ret, nil
}

func getReader(ctx context.Context, client *s3.Client, bucket, key string) (io.ReadCloser, error) {
	res, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	readCloser := res.Body
	if strings.HasSuffix(key, ".gz") {
		reader, err := gzip.NewReader(res.Body)
		if err != nil {
			return nil, err
		}
		readCloser = gzCloser{Reader: reader, Closer: res.Body}
	}
	return readCloser, nil

}

func lambdaEntry(ctx context.Context, input batch) (result, error) {
	log.Println("handling batch", input)
	ret := result{Index: input.Index}
	filter, err := getEvaluable(input.Expr)
	if err != nil {
		log.Fatalf("error evaluating expression '%s': %v", input.Expr, err)
	}
	s3svc := s3.NewFromConfig(getAwsConfig(ctx, input.Region))
	for _, k := range input.Keys {
		log.Println("scanning object at", input.Bucket, k)
		body, err := getReader(ctx, s3svc, input.Bucket, k)
		if err != nil {
			ret.Errors = append(ret.Errors, fmt.Sprint("error reading object", k, err))
			continue
		}
		// TODO: allow for regex/non-json scanning here?
		if matches, err := scanJsonObjects(ctx, filter, body); err == nil {
			ret.Matches = append(ret.Matches, matches...)
		} else {
			ret.Matches = append(ret.Matches, fmt.Sprint("error scanning object", k, err))
		}
	}
	return ret, nil
}
