#!/bin/bash

MAPR_HOME=${MAPR_HOME:-/opt/mapr}
KSQL_VERSION="5.1.2"
KSQL_HOME="$MAPR_HOME"/ksql/ksql-"$KSQL_VERSION"
KSQL_BIN="$KSQL_HOME"/bin
KSQL_TEMPLATE_CONF_DIR="$KSQL_HOME/conf.new/"
KSQL_CONF_DIR="$KSQL_HOME/etc/ksql/"
MAPR_CONF_DIR="${MAPR_HOME}/conf/"
MAPR_WARDEN_CONF_DIR="${MAPR_HOME}/conf/conf.d"
DAEMON_CONF="$MAPR_HOME/conf/daemon.conf"
WARDEN_KSQL_CONF="$KSQL_HOME"/warden/warden.ksql.conf
WARDEN_KSQL_DEST="$MAPR_WARDEN_CONF_DIR/warden.ksql.conf"
HADOOP_VER=$(cat "$MAPR_HOME/hadoop/hadoopversion")
secureCluster=0
MAPR_USER=""
MAPR_GROUP=""

if [ -f "$DAEMON_CONF" ]; then
  MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' "$DAEMON_CONF")
  MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' "$DAEMON_CONF")
else
  MAPR_USER=`logname`
  MAPR_GROUP="$MAPR_USER"
fi

# isSecure is set in server/configure.sh
if [ -n "$isSecure" ]; then
  if [ "$isSecure" == "true" ]; then
    secureCluster=1
  fi
fi

changeKSQLPermission() {
  #
  # change permissions
  #
  if [ ! -z "$MAPR_USER" ]; then
    chown -R "$MAPR_USER" "$MAPR_HOME/ksql"
  fi
  if [ ! -z "$MAPR_GROUP" ]; then
    chgrp -R "$MAPR_GROUP" "$MAPR_HOME/ksql"
  fi
}

createRestartFile(){
  if ! [ -d ${MAPR_CONF_DIR}/restart ]; then
    mkdir -p ${MAPR_CONF_DIR}/restart
  fi

cat > "${MAPR_CONF_DIR}/restart/ksql-${KSQL_VERSION}.restart" <<'EOF'
  #!/bin/bash
  isSecured="false"
  if [ -f "${MAPR_HOME}/conf/mapr-clusters.conf" ]; then
    isSecured=$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
  fi
  if [ "${isSecured}" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
    export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
    fi
  maprcli node services -action restart -name ksql -nodes $(hostname)
EOF

  chmod +x "${MAPR_CONF_DIR}/restart/ksql-$KSQL_VERSION.restart"
  chown -R $MAPR_USER:$MAPR_GROUP "${MAPR_CONF_DIR}/restart/ksql-$KSQL_VERSION.restart"
}

#
# Copying the warden service config file
#
setupWardenConfFile() {
  if ! [ -d ${MAPR_WARDEN_CONF_DIR} ]; then
    mkdir -p ${MAPR_WARDEN_CONF_DIR} > /dev/null 2>&1
  fi

  # Install warden file
  cp ${WARDEN_KSQL_CONF} ${MAPR_WARDEN_CONF_DIR}
  chown $MAPR_USER:$MAPR_GROUP $WARDEN_KSQL_DEST
}

function getProperty() {
   PROPERTY_FILE=$1
   PROP_KEY=$2
   PROP_VALUE=`cat $PROPERTY_FILE | grep "$PROP_KEY" | cut -d'=' -f2`
   echo $PROP_VALUE
}

copyFilesToTargetConfigDir() {
    mkdir -p $KSQL_CONF_DIR
    cp -n $KSQL_TEMPLATE_CONF_DIR/*.properties $KSQL_CONF_DIR
    if [ $secureCluster == 1 ]; then
        cp -n $KSQL_TEMPLATE_CONF_DIR/secure/ksql-server-secure.properties $KSQL_CONF_DIR/ksql-server.properties
    else
        cp -n $KSQL_TEMPLATE_CONF_DIR/unsecure/ksql-server.properties $KSQL_CONF_DIR/ksql-server.properties
    fi
}

createSymlink() {
    rm -f  /usr/bin/ksql
    ln -sf "$KSQL_BIN"/ksql-cli-start.sh /usr/bin/ksql
}

#
# main
#
# typically called from core configure.sh
#

USAGE="usage: $0 [--secure|--customSecure|--unsecure|-EC|-R|--help"
if [ ${#} -gt 1 ]; then
  for i in "$@" ; do
    case "$i" in
      --secure)
        secureCluster=1
        shift
        ;;
      --customSecure|-cs)
        secureCluster=1
        shift
        ;;
      --unsecure)
        secureCluster=0
        shift
        ;;
      --help)
        echo "$USAGE"
        return 0 2>/dev/null || exit 0
        ;;
      -EC|--EC)
         shift
         ;;
       -R|--R)
         shift
         ;;
       --)
        echo "$USAGE"
        return 1 2>/dev/null || exit 1
        ;;
    esac
  done
else
    echo "$USAGE"
    return 1 2>/dev/null || exit 1
fi

if [ ! -f "$KSQL_CONF_DIR/.not_configured_yet" ]; then
    createRestartFile
fi
copyFilesToTargetConfigDir
changeKSQLPermission
setupWardenConfFile
createSymlink

# remove state file and start files
if [ -f "$KSQL_CONF_DIR/.not_configured_yet" ]; then
    rm -f "$KSQL_CONF_DIR/.not_configured_yet"
fi

true
