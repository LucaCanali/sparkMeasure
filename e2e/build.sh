#!/bin/bash


# Build image containing integration tests for sparkMeasure over Kubernetes

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message

Build image containing integration tests for sparkMeasure over Kubernetes
EOD
}

# get the options
while getopts h c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

ciux ignite --selector build $PROJECT_DIR
. $PROJECT_DIR/.ciux.d/ciux_build.sh

echo "Building Docker image for sparkMeasure integration tests"
docker image build --tag "$CIUX_IMAGE_URL" "$PROJECT_DIR"

echo "Build successful"

