package main

import (
	"context"
	"log"
)

func lambdaEntry(ctx context.Context, input batch) (result, error) {
	log.Println("handling batch", input)
	ret := result{index: input.index}
	return ret, nil
}
