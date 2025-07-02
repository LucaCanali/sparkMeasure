#!/bin/bash
# Build image containing integration tests for sparkMeasure over Kubernetes
# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)
spark_image_tag="3.5.6-scala2.12-java17-python3-ubuntu"

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h        this message
    -i        spark image tag to use as base image (default: 3.5.6-scala2.12-java17-python3-ubuntu)

Build image containing integration tests for sparkMeasure over Kubernetes
EOD
}

# get the options
while getopts hi: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    i) spark_image_tag="$OPTARG" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

ciux ignite --selector build $PROJECT_DIR
. $PROJECT_DIR/.ciux.d/ciux_build.sh

scala_version=$(echo "$spark_image_tag" | grep -oP 'scala\K[0-9]+\.[0-9]+')

echo "Building Docker image for sparkMeasure integration tests"
docker image build  \
  --build-arg "spark_image_tag=$spark_image_tag" \
  --build-arg "scala_version=$scala_version" \
  --tag "$CIUX_IMAGE_URL" "$PROJECT_DIR"

echo "Build successful"

