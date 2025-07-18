#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)
export CIUXCONFIG=$PROJECT_DIR/.ciux.d/ciux_itest.sh
. $CIUXCONFIG
app_name="$CIUX_IMAGE_NAME"

timeout="480s"

pod="spark-sql-driver"
namespace="spark-jobs"

while ! kubectl get pods -n $namespace | grep -q $pod; do
    echo "Waiting for Spark SQL driver pod to be created..."
    sleep 5
done

echo "Spark SQL driver pod is created:"
kubectl get pods -n $namespace

echo "Waiting for Spark SQL driver to be ready..."
kubectl wait -n $namespace --for=condition=Ready pod/$pod --timeout="$timeout"

while ! kubectl exec -n $namespace $pod -- curl -s http://localhost:8090/metrics; do
    echo "Waiting for Spark SQL driver metrics to be available..."
    sleep 5
done

argocd app wait -l app.kubernetes.io/part-of="$app_name"