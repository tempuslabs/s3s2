#!/bin/bash

# this script can run against PRs. dont publish PRs!
if [[ ${GIT_BRANCH} != "master" ]]; then
  echo "Non-master build detected. Skipping publish."
  exit 0
fi

NEXUS_PATH='https://nexus.opstempus.com/repository/tempus-n'
PUBLIC_S3_BUCKET='tdo-n-message-gateway-s3s2-use1'
GIT_COMMIT=$(git rev-parse HEAD)

echo "Publishing S3S2 Version: ${GIT_COMMIT}"

# publish to nexus
curl --fail --user "${NEXUS_USERNAME}:${NEXUS_PASSWORD}" --upload-file ./linux/s3s2-linux-amd64 "${NEXUS_PATH}/${GIT_COMMIT}/s3s2-linux-amd64"
curl --fail --user "${NEXUS_USERNAME}:${NEXUS_PASSWORD}" --upload-file ./darwin/s3s2-darwin-amd64 "${NEXUS_PATH}/${GIT_COMMIT}/s3s2-darwin-amd64"
curl --fail --user "${NEXUS_USERNAME}:${NEXUS_PASSWORD}" --upload-file ./windows/s3s2-windows-amd64.exe "${NEXUS_PATH}/${GIT_COMMIT}/s3s2-windows-amd64.exe"
curl --fail --user "${NEXUS_USERNAME}:${NEXUS_PASSWORD}" --upload-file ./so/s3s2.so "${NEXUS_PATH}/${GIT_COMMIT}/s3s2.so"

# publish to s3
apt -qq update && apt -qq install -y python3-pip && pip --no-cache-dir install awscli
aws s3api put-object --bucket "${PUBLIC_S3_BUCKET}" --key "${GIT_COMMIT}/s3s2-linux-amd64" --body ./linux/s3s2-linux-amd64 --acl public-read
aws s3api put-object --bucket "${PUBLIC_S3_BUCKET}" --key "${GIT_COMMIT}/s3s2-darwin-amd64" --body ./darwin/s3s2-darwin-amd64 --acl public-read
aws s3api put-object --bucket "${PUBLIC_S3_BUCKET}" --key "${GIT_COMMIT}/s3s2-windows-amd64.exe" --body ./windows/s3s2-windows-amd64.exe --acl public-read
