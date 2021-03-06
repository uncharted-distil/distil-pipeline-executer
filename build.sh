#!/bin/bash

HIGHLIGHT='\033[0;34m'
NC='\033[0m'

DISTIL_VERSION=e6daa843ec74b25f8f46f9081dd9df3fe638ca9c
D3MSTATICDIR=/data/static_resources
DOCKER_REPO=docker.uncharted.software
DOCKER_IMAGE_NAME=distil_service
DOCKER_IMAGE_VERSION=0.1.1

# get distil service executable
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go get -a -v github.com/uncharted-distil/distil-pipeline-executer@$DISTIL_VERSION
mv $GOPATH/bin/distil-pipeline-executer .

# get the static file
cp $D3MSTATICDIR/5c106cde386e87d4033832f2996f5493238eda96ccf559d1d62760c4de0613f8 5c106cde386e87d4033832f2996f5493238eda96ccf559d1d62760c4de0613f8

docker build --squash --no-cache --network=host \
  --tag $DOCKER_REPO/$DOCKER_IMAGE_NAME:${DOCKER_IMAGE_VERSION} \
  --tag $DOCKER_REPO/$DOCKER_IMAGE_NAME:latest .
