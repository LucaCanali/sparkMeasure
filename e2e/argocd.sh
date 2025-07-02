#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)

ciux ignite --selector itest "$PROJECT_DIR"

# Run the CD pipeline
export CIUXCONFIG=$PROJECT_DIR/.ciux.d/ciux_itest.sh
. $CIUXCONFIG
app_name="$CIUX_IMAGE_NAME"

NS=argocd

argocd login --core
kubectl config set-context --current --namespace="$NS"

argocd app create $app_name --dest-server https://kubernetes.default.svc \
    --dest-namespace "$app_name" \
    --repo https://github.com/k8s-school/$app_name \
    --path e2e/charts/apps --revision "$SPARKMEASURE_WORKBRANCH" \
    -p spec.source.targetRevision.default="$SPARKMEASURE_WORKBRANCH"

argocd app sync $app_name

argocd app set spark-jobs -p image.tag="$CIUX_IMAGE_TAG"

argocd app sync -l app.kubernetes.io/part-of=$app_name,app.kubernetes.io/component=operator
argocd app wait -l app.kubernetes.io/part-of=$app_name,app.kubernetes.io/component=operator

argocd app sync -l app.kubernetes.io/part-of=$app_name