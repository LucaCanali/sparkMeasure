#!/bin/bash

# Run the entire end-to-end test workflow

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message

Run the entire end-to-end test workflow for sparkMeasure over Kubernetes
EOD
}

# get the options
while getopts h c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    \?) usage ; exit 2 ;;
    esac
done

# Build spark container image with latest sparkMeasure code
$DIR/build.sh

# Push the image to the registry (docker registry credentials must be set in the environment)
$DIR/push-image.sh

# Create and configure the Kubernetes cluster
$DIR/prereq.sh

# Run continuous deployment with ArgoCD for e2e tests
$DIR/argocd.sh

# Check sparkMeasure metrics avalaibility through prometheus exporter
$DIR/check-metrics.sh

