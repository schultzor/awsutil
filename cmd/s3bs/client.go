package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func searchWorker(ctx context.Context,
	wg *sync.WaitGroup,
	input chan batch,
	output chan result,
	lambdaName, lambdaRegion string) {

	client := lambda.NewFromConfig(getAwsConfig(ctx, lambdaRegion))
	for i := range input {
		payload, err := json.Marshal(i)
		if err != nil {
			log.Fatal("marshalling error", err)
		}
		start := time.Now()
		res, err := client.Invoke(ctx, &lambda.InvokeInput{
			FunctionName: &lambdaName,
			Payload:      payload})
		if err != nil {
			log.Fatal("error invoking lambda for", i, err)
		}
		if res.FunctionError != nil {
			log.Println("function error on batch", i.Index, *res.FunctionError)
		}
		var rmsg result
		if err := json.Unmarshal(res.Payload, &rmsg); err != nil {
			log.Fatal("unmarshalling error", err)
		}
		rmsg.Took = time.Since(start)
		output <- rmsg
	}
	wg.Done()
}

func stdoutWriter(ctx context.Context, outputs chan result) {
	var accum time.Duration
	var count int
	for r := range outputs {
		for _, e := range r.Errors {
			fmt.Fprintln(os.Stderr, "search error:", e)
		}
		for _, m := range r.Matches {
			fmt.Fprintln(os.Stdout, m)
		}
		if r.Truncated != "" {
			fmt.Fprintln(os.Stderr, "truncated results:", r.Truncated)
		}
		if len(r.GzipMatches) > 0 {
			if gzReader, err := gzip.NewReader(bytes.NewReader(r.GzipMatches)); err == nil {
				io.Copy(os.Stdout, gzReader)
				gzReader.Close()
			}
		}
		accum += r.Took
		count += 1
	}
	log.Println("all done, average batch duration:", accum/time.Duration(count))
}

const defaultRegion = "us-east-1"

func clientEntry() {
	bucket := flag.String("bucket", "", "s3 bucket to search")
	prefix := flag.String("prefix", "", "limit search to a prefix in the bucket")
	expr := flag.String("expr", "true", "gval search expression (which evaluates to a bool)")
	region := flag.String("region", defaultRegion, "AWS region for bucket")
	workerCount := flag.Int("workers", 50, "number of search workers")
	batchSize := flag.Int("batchSize", 50, "number of s3 objects per lambda invocation")
	lambdaName := flag.String("lambdaName", "s3bs-worker", "name of lambda worker function")
	lambdaRegion := flag.String("lambdaRegion", defaultRegion, "AWS region where lambda lives")
	flag.Parse()

	if *bucket == "" {
		log.Fatal("missing -bucket value")
	}
	if _, err := getEvaluable(*expr); err != nil {
		log.Fatalf("error evaluating expression '%s': %v", *expr, err)
	}

	ctx := context.TODO()
	inputs := make(chan batch)
	outputs := make(chan result)
	go stdoutWriter(ctx, outputs)
	var wg sync.WaitGroup

	log.Printf("starting %d search workers", *workerCount)
	for i := 0; i < *workerCount; i++ {
		wg.Add(1)
		go searchWorker(ctx, &wg, inputs, outputs, *lambdaName, *lambdaRegion)
	}

	params := &s3.ListObjectsV2Input{Bucket: bucket}
	if *prefix != "" {
		params.Prefix = prefix
	}
	var batchCount int
	var keyCount int
	var keys []string
	sendBatch := func(threshold int) {
		if len(keys) < threshold {
			return
		}
		inputs <- batch{
			Index:  batchCount,
			Bucket: *bucket,
			Region: *region,
			Keys:   keys,
			Expr:   *expr,
		}
		keyCount += len(keys)
		keys = nil
		batchCount++
	}
	pager := s3.NewListObjectsV2Paginator(s3.NewFromConfig(getAwsConfig(ctx, *region)), params)
	listStart := time.Now()
	for pager.HasMorePages() {
		res, err := pager.NextPage(ctx)
		if err != nil {
			log.Fatal("error listing objects:", err)
		}
		for _, o := range res.Contents {
			keys = append(keys, *o.Key)
			sendBatch(*batchSize)
		}
	}
	log.Println("found", keyCount, "keys in", time.Since(listStart), ", split into", batchCount, "batches")
	sendBatch(1)
	close(inputs)
	wg.Wait()
	close(outputs)
}