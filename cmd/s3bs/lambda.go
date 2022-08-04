package main

import (
	"bytes"
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

const ResultLimitBytes = 1024 * 1024 * 5 // keep us under 6MB lambda payload limit

//  a wrapper for a gzip reader so we retain the original Closer
type gzCloser struct {
	Reader io.Reader
	Closer io.Closer
}

func (g gzCloser) Read(p []byte) (n int, err error) { return g.Reader.Read(p) }
func (g gzCloser) Close() error                     { return g.Closer.Close() }

func scanJsonObjects(ctx context.Context, filter gval.Evaluable, contents io.ReadCloser, output io.Writer) error {
	defer contents.Close()
	encoder := json.NewEncoder(output)
	decoder := json.NewDecoder(contents)
	for decoder.More() {
		var logEntry map[string]any
		if err := decoder.Decode(&logEntry); err != nil {
			return err
		}
		filterMatch, err := filter.EvalBool(ctx, logEntry)
		if err != nil {
			return err
		}
		if filterMatch {

			if err := encoder.Encode(logEntry); err != nil {
				return err
			}
		}
	}
	return nil
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
		return ret, fmt.Errorf("error evaluating expression '%s': %v", input.Expr, err)
	}
	var bb bytes.Buffer
	gzOut := gzip.NewWriter(&bb)
	s3svc := s3.NewFromConfig(getAwsConfig(ctx, input.Region))
	for ki, k := range input.Keys {
		if bb.Len() >= ResultLimitBytes {
			skipped := input.Keys[ki:]
			ret.Truncated = fmt.Sprintf("skipping %s", strings.Join(skipped, ","))
			log.Println(ret.Truncated)
			break
		}
		log.Println("scanning object at", input.Bucket, k)
		body, err := getReader(ctx, s3svc, input.Bucket, k)
		if err != nil {
			ret.Errors = append(ret.Errors, fmt.Sprintf("error reading object %s: %v", k, err))
			continue
		}
		// TODO: allow for regex/non-json scanning here?
		if err := scanJsonObjects(ctx, filter, body, gzOut); err != nil {
			ret.Errors = append(ret.Errors, fmt.Sprintf("error scanning object %s: %v", k, err))
		}
	}
	gzOut.Flush()
	ret.GzipMatches = bb.Bytes()
	return ret, nil
}
