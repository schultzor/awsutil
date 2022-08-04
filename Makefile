.PHONY: all clean

all: bin/s3bs-linux install lambda.zip

install:
	go install ./cmd/s3bs

bin/s3bs-linux: ./cmd/s3bs/*.go
	env GOOS=linux GOARCH=amd64 go build -ldflags '-w -s' -o $@ ./cmd/s3bs/*.go

lambda.zip: bin/s3bs-linux
	cd bin && zip ../lambda.zip s3bs-linux

clean:
	rm -rf ./bin lambda.zip
