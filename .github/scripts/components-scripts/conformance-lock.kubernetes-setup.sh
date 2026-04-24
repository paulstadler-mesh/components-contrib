#!/bin/sh

set -e

# The lock.kubernetes component uses the coordination.k8s.io/v1 Lease API,
# which ships with every vanilla Kubernetes cluster (including KinD), so no
# prerequisite resources need to be created. The test uses randomized UUIDs
# as lock keys, so there is no shared state to seed either.
#
# NAMESPACE is still exported for consistency with the other kubernetes
# conformance setup scripts; the test component config references the
# 'default' namespace explicitly.
echo "NAMESPACE=default" >> $GITHUB_ENV
