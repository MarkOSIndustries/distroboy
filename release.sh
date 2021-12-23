#!/bin/bash

die() { echo "$*" 1>&2 ; exit 1; }

echo $1 | grep -E -q '^([0-9]+).([0-9]+).([0-9]+)$' || die "Version must be of the form N.N.N, $1 provided"
VERSION=$1

COMMIT_SHA=$(git rev-parse $VERSION) || die "You haven't set up the git release tag yet."
[[ "${COMMIT_SHA}" == "$(git rev-parse HEAD)" ]] || die "Check out the tagged commit first."
[[ -z $(git status --short) ]] || die "Working directory is dirty"

docker build -f Dockerfile --target build_env \
  --build-arg VERSION="${VERSION}" \
  -t distroboy-build-env:${VERSION} \
  .

docker build -f Dockerfile --target coordinator_runtime \
  --build-arg VERSION="${VERSION}" \
  -t markosindustries/distroboy-coordinator:latest \
  -t markosindustries/distroboy-coordinator:${VERSION} \
  .

docker build -f Dockerfile --target example_runtime \
  --build-arg VERSION="${VERSION}" \
  -t markosindustries/distroboy-example:latest \
  -t markosindustries/distroboy-example:${VERSION} \
  .

if [[ $@ =~ "--push-maven" ]]; then
  [[ -v OSSRH_USERNAME ]] || die "OSSRH_USERNAME is not set"
  [[ -v OSSRH_PASSWORD ]] || die "OSSRH_PASSWORD is not set"
  [[ -v GPG_KEYID ]] || die "GPG_KEYID is not set"
  [[ -v GPG_PASSWORD ]] || die "GPG_PASSWORD is not set"

  docker run \
    -v ~/.gnupg/secring.gpg:/secrets/.gnupg/secring.gpg \
    -e OSSRH_USERNAME \
    -e OSSRH_PASSWORD \
    -e GPG_KEYID \
    -e GPG_PASSWORD \
    -e VERSION=${VERSION} \
    -e COMMIT_SHA=${COMMIT_SHA} \
    --entrypoint bash \
    distroboy-build-env:${VERSION} \
    -c /home/distroboy/push-to-ossrh.sh
fi

if [[ $@ =~ "--push-docker" ]]; then
  docker push markosindustries/distroboy-coordinator:latest
  docker push markosindustries/distroboy-coordinator:$VERSION
  docker push markosindustries/distroboy-example:latest
  docker push markosindustries/distroboy-example:$VERSION
fi