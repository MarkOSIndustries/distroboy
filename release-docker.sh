#!/bin/bash

die() { echo "$*" 1>&2 ; exit 1; }

echo $1 | grep -E -q '^([0-9]+).([0-9]+).([0-9]+)$' || die "Version must be of the form N.N.N, $1 provided"
VERSION=$1

docker build -f coordinator/Dockerfile --target runtime \
  --build-arg VERSION="${VERSION}" \
  -t markosindustries/distroboy-coordinator:latest \
  -t markosindustries/distroboy-coordinator:${VERSION} \
  .

if [[ $2 == "push" ]]; then
  docker push markosindustries/distroboy-coordinator:latest
  docker push markosindustries/distroboy-coordinator:$VERSION
fi