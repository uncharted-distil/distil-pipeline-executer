#!/bin/sh

docker run \
  --name distil-service \
  --rm \
  -p 8080:8080 \
  docker.uncharted.software/distil_service:latest
