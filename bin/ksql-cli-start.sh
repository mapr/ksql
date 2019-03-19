#!/bin/bash

MAPR_HOME=${MAPR_HOME:-/opt/mapr}
KSQL_VERSION="5.1.2"
KSQL_HOME="$MAPR_HOME"/ksql/ksql-"$KSQL_VERSION"
KSQL_BIN="$KSQL_HOME"/bin

exec "$KSQL_BIN"/ksql "$@"
