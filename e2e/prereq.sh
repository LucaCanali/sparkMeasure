#!/bin/bash

# Install pre-requisite for fink ci

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
PROJECT_DIR=$(cd "$DIR/.."; pwd -P)

ciux_version="v0.0.5-rc4"
go install github.com/k8s-school/ciux@"$ciux_version"

echo "Ignite the project using ciux"
ciux ignite --selector itest $PROJECT_DIR

export CIUXCONFIG=$PROJECT_DIR/.ciux.d/ciux_itest.sh
cluster_name=$(ciux get clustername $PROJECT_DIR)
monitoring=false

# Get kind version from option -k
while getopts mk: flag
do
    case "${flag}" in
        k) kind_version_opt=--kind-version=${OPTARG};;
        m) monitoring=true;;
    esac
done

ktbx install kind
ktbx install kubectl
ktbx install helm
ink "Create kind cluster"
ktbx create -s --name $cluster_name
ink "Install OLM"
ktbx install olm
ink "Install ArgoCD operator"
ktbx install argocd
