#!/bin/bash

# Run integration tests for sparkMeasure over Kubernetes

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h        this message

Run integration tests for sparkMeasure over Kubernetes

EOD
}

# get the options
while getopts hi: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

ciux ignite --selector itest "$PROJECT_DIR"

# Run the CD pipeline
export CIUXCONFIG=$PROJECT_DIR/.ciux.d/ciux_itest.sh
. $CIUXCONFIG
app_name="$CIUX_IMAGE_NAME"

NS=argocd

argocd login --core
kubectl config set-context --current --namespace="$NS"

# Add support for Github PR, which do not have a branch name in the git repository
if [ "${GITHUB_EVENT_NAME:-}" = "pull_request" ]; then
  revision="${GITHUB_HEAD_REF}"
  # During a PR, the git repository containing the CI code is the forked one
  git_repo=$(jq -r '.pull_request.head.repo.full_name' "$GITHUB_EVENT_PATH")
else
  revision="${SPARKMEASURE_WORKBRANCH}"
  git_repo="${GITHUB_REPOSITORY}"
fi
git_repo="https://github.com/${git_repo}"

argocd app create "$app_name" --dest-server https://kubernetes.default.svc \
    --dest-namespace "$app_name" \
    --repo "$git_repo" \
    --path e2e/charts/apps --revision "$revision" \
    -p spec.source.targetRevision.default="$revision"

argocd app sync $app_name

argocd app set spark-jobs -p image.tag="$CIUX_IMAGE_TAG"

argocd app sync -l app.kubernetes.io/part-of=$app_name,app.kubernetes.io/component=operator
argocd app wait -l app.kubernetes.io/part-of=$app_name,app.kubernetes.io/component=operator

argocd app sync -l app.kubernetes.io/part-of=$app_name
