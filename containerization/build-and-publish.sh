#!/bin/bash
set -e

IMAGE="zanfranceschi/rinha-de-backend-2026-zan-dotnet"
TAG=$(date +%Y%m%d%H%M)

docker build \
    --platform linux/amd64 \
    -t "$IMAGE:$TAG" \
    -t "$IMAGE:latest" \
    -t "rinha-de-backend-2026-zan-dotnet:latest" \
    -f Dockerfile ..

# docker push --all-tags $IMAGE

echo "Published $IMAGE:$TAG"

echo $TAG
