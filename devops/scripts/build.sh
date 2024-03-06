#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_ksql() {
  mvn ${KAFKA_MAVEN_ARGS}
  tgz_name="./ksqldb-package/target/ksqldb-package-*-package.tgz"
  mkdir -p "${BUILD_ROOT}/build"
  tar xvf ${tgz_name} -C "${BUILD_ROOT}/build"
}

main() {
  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"

  echo "Building project..."
  build_ksql

  echo "Preparing directory structure..."
  setup_role "mapr-ksql"

  setup_package "mapr-ksql-internal"

  echo "Building packages..."
  build_package "mapr-ksql-internal"
  build_package "mapr-ksql"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main
