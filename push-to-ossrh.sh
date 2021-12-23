#!/bin/bash

#TODO remove this
echo "Using VERSION string ${VERSION}"

# This is intended to run within docker from the build_env stage
./gradlew \
  -Pversion_string="${VERSION}" \
  -Pcommit_sha="${COMMIT_SHA}" \
  -Psigning.secretKeyRingFile=/secrets/.gnupg/secring.gpg \
  -Psigning.keyId="${GPG_KEYID}" \
  -Psigning.password="${GPG_PASSWORD}" \
  publishAllPublicationsToOSSRHRepository
