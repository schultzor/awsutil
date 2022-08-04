package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type batch struct {
	index  int
	bucket string
	region string
	keys   []string
	expr   string
}

type result struct {
	index     int
	truncated bool
	matches   []byte
}

var configForRegion = make(map[string]aws.Config)

func getAwsConfig(ctx context.Context, region string) aws.Config {
	if _, ok := configForRegion[region]; !ok {
		if cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region)); err == nil {
			configForRegion[region] = cfg
		} else {
			log.Fatalf("unable to load AWS config, %v", err)
		}
	}
	return configForRegion[region]
}

func main() {
	if fn := os.Getenv("AWS_LAMBDA_FUNCTION_NAME"); fn != "" {
		log.Println("invoked as lambda", fn)
		lambda.Start(lambdaEntry)
	} else {
		clientEntry()
	}
}