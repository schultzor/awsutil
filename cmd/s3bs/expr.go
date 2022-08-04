package main

import (
	"errors"
	"strings"

	"github.com/PaesslerAG/gval"
)

var errBadCall = errors.New("expression argument error")

func containsFunc(args ...any) (any, error) {
	if len(args) < 2 {
		return nil, errBadCall
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, errBadCall
	}
	substr, ok := args[1].(string)
	if !ok {
		return nil, errBadCall
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr)), nil
}

// get an evaluable expression function from our expression string
// in addition to the stock "Full" grammer, includes these functions:
// - contains(s, substr): performs a case-insensitive substring search
func getEvaluable(expr string) (gval.Evaluable, error) {
	return gval.Full(gval.Function("contains", containsFunc)).NewEvaluable(expr)
}