#!/bin/bash

HIGHLIGHT='\033[0;34m'
NC='\033[0m'

DISTIL_VERSION=3d62704146cec826c06bdf8c3addf383bb97ba32

# get distil service executable
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go get -a -v github.com/uncharted-distil/distil-pipeline-executer@$DISTIL_VERSION
mv $GOPATH/bin/distil-pipeline-executer .

docker build --squash --no-cache --network=host --tag docker.uncharted.software/distil_service:latest .
