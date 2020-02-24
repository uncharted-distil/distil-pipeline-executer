#!/bin/bash

HIGHLIGHT='\033[0;34m'
NC='\033[0m'

# get distil service executable
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go get -a -v github.com/uncharted-distil/distil-pipeline-executor
mv $GOPATH/bin/distil-pipeline-executor .

docker build --squash --no-cache --network=host --tag docker.uncharted.software/distil_service:latest .
