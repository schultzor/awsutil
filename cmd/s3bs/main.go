// main entry point that determines if we're executing as the local client or the lambda worker
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type batch struct {
	Index  int      `json:"index"`
	Bucket string   `json:"bucket"`
	Region string   `json:"region"`
	Keys   []string `json:"keys"`
	Expr   string   `json:"expr"`
}

type result struct {
	Index     int      `json:"index"`
	Truncated string   `json:"truncated"`
	Matches   []string `json:"matches"`
	Errors    []string `json:"errors"`
	Took      time.Duration
}

func getAwsConfig(ctx context.Context, region string) aws.Config {
	if cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region)); err == nil {
		return cfg
	} else {
		log.Fatalf("unable to load AWS config, %v", err)
	}
	return aws.Config{}
}

func main() {
	if fn := os.Getenv("AWS_LAMBDA_FUNCTION_NAME"); fn != "" {
		log.Println("invoked as lambda", fn)
		lambda.Start(lambdaEntry)
	} else {
		clientEntry()
	}
}