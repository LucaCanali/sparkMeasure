#!/usr/bin/env bash

# Push image to Docker Hub or load it inside kind

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)


usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -k          development mode: load image in kind
    -d          do not push image to remote registry

Push image to remote registry and/or load it inside kind
EOD
}

kind=false
registry=true

# get the options
while getopts dhk c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    k) kind=true ;;
	    d) registry=false ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

export CIUXCONFIG=$DIR/../.ciux.d/ciux_build.sh
$(ciux get image --check $PROJECT_DIR --env)

if [ $kind = true ]; then
  cluster_name=$(ciux get clustername $PROJECT_DIR)
  kind load docker-image "$CIUX_IMAGE_URL" --name "$cluster_name"
fi
if [ $registry = true ]; then
  docker push "$CIUX_IMAGE_URL"
fi
