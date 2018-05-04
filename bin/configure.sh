#!/bin/bash
#######################################################################
# Copyright (c) 2018 & onwards. MapR Tech, Inc., All rights reserved
#######################################################################
#
# Configure script for KSQL
#
# This script is normally run by the core configure.sh to setup KSQL
# during install. If it is run standalone, need to correctly
# initialize the variables that it normally inherits from the master
# configure.sh
#######################################################################


RETURN_SUCCESS=0
RETURN_ERR_MAPR_HOME=1
RETURN_ERR_ARGS=2
RETURN_ERR_MAPRCLUSTER=3

# Initialize API and globals

MAPR_HOME=${MAPR_HOME:-/opt/mapr}

. ${MAPR_HOME}/server/common-ecosystem.sh 2>/dev/null
{ set +x; } 2>/dev/null

initCfgEnv

if [ $? -ne 0 ] ; then
  echo '[ERROR] MAPR_HOME seems to not be set correctly or mapr-core not installed.'
  exit $RETURN_ERR_MAPR_HOME
fi

MAPR_CONF_DIR=${MAPR_CONF_DIR:-"${MAPR_HOME}/conf"}

KSQL_VERSION=$(cat "${MAPR_HOME}/ksql/ksqlversion")
KSQL_HOME="${MAPR_HOME}/ksql/ksql-${KSQL_VERSION}"

WARDEN_KSQL_SRC="${KSQL_HOME}/config/warden.ksql.conf"
WARDEN_KSQL_CONF="${MAPR_HOME}/conf/conf.d/warden.ksql.conf"

KSQL_FILE_SECURE="${KSQL_HOME}/conf/.isSecure"
KSQL_FILE_NOT_CONFIGURED="${KSQL_HOME}/conf/.not_configured_yet"

# Set MAPR_USER and MAPR_GROUP
DAEMON_CONF="${MAPR_HOME}/conf/daemon.conf"

MAPR_USER=${MAPR_USER:-$([ -f "$DAEMON_CONF" ] && grep "mapr.daemon.user" "$DAEMON_CONF" | cut -d '=' -f 2)}
MAPR_USER=${MAPR_USER:-$(logname)} # Edge node
MAPR_USER=${MAPR_USER:-"mapr"}

MAPR_GROUP=${MAPR_GROUP:-$([ -f "$DAEMON_CONF" ] && grep "mapr.daemon.group" "$DAEMON_CONF" | cut -d '=' -f 2)}
MAPR_GROUP=${MAPR_GROUP:-$MAPR_USER}

read_secure() {
  [ -e "$KSQL_FILE_SECURE" ] && cat "$KSQL_FILE_SECURE"
}

write_secure() {
  echo "$1" > "$KSQL_FILE_SECURE"
}

chown_component() {
  chown -R $MAPR_USER:$MAPR_GROUP "$KSQL_HOME"
}

setup_warden_conf() {
  cp -v "$WARDEN_KSQL_SRC" "$WARDEN_KSQL_CONF"
  chown $MAPR_USER:$MAPR_GROUP "$WARDEN_KSQL_CONF"
}

create_restart_file() {
  mkdir -p "${MAPR_CONF_DIR}/restart"
  cat > "${MAPR_CONF_DIR}/restart/ksql-${KSQL_VERSION}.restart" <<'EOF'
#!/bin/bash
MAPR_HOME=${MAPR_HOME:-/opt/mapr}
MAPR_USER=${MAPR_USER:-mapr}
if [ -z "$MAPR_TICKETFILE_LOCATION" ] && [ -e "${MAPR_HOME}/conf/mapruserticket" ]; then
    export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
fi
sudo -u $MAPR_USER -E maprcli node services -action restart -name ksql -nodes $(hostname)
EOF
  chmod +x "${MAPR_CONF_DIR}/restart/ksql-${KSQL_VERSION}.restart"
  chown $MAPR_USER:$MAPR_GROUP "${MAPR_CONF_DIR}/restart/ksql-${KSQL_VERSION}.restart"
}

conf_uncomment() {
  local conf_file="$1"
  local property_name="$2"
  local delim="="
  sed -i "s|#*\s*${property_name}\s*${delim}|${property_name} ${delim}|" "${conf_file}"
}

conf_comment() {
  local conf_file="$1"
  local property_name="$2"
  local delim="="
  sed -i "s|#*\s*${property_name}\s*${delim}|# ${property_name} ${delim}|" "${conf_file}"
}

conf_get_property() {
    local conf_file="$1"
    local property_name="$2"
    local delim="="
    grep "^\s*${property_name}" "${conf_file}" | sed "s|^\s*${property_name}\s*${delim}\s*||"
}

conf_set_property() {
    local conf_file="$1"
    local property_name="$2"
    local property_value="$3"
    local delim="="
    if grep -q "^\s*${property_name}\s*${delim}" "${conf_file}"; then
        # modify property
        sed -i -r "s|^\s*${property_name}\s*${delim}.*$|${property_name} ${delim} ${property_value}|" "${conf_file}"
    else
        echo "${property_name} ${delim} ${property_value}" >> "${conf_file}"
    fi
}

create_internal_stream_if_needed() {
  sudo -u $MAPR_USER -E maprcli stream create -path /var/mapr/kafka-internal-stream -produceperm p -consumeperm p -topicperm p | 
}


# Initialize arguments
isOnlyRoles=${isOnlyRoles:-0}

# Parse options
USAGE="usage: $0 [-h] [-R] [--secure|--unsecure|--customSecure] [-EC <options>]"

while [ ${#} -gt 0 ]; do
  case "$1" in
    --secure)
      isSecure="true";
      shift 1;;
    --unsecure)
      isSecure="false";
      shift 1;;
    --customSecure)
      isSecure="custom";
      shift 1;;
    -R)
      isOnlyRoles=1;
      shift 1;;
    -EC)
      for i in $2 ; do
        case $i in
          -R) isOnlyRoles=1 ;;
          *) : ;; # unused in KSQL
        esac
      done
      shift 2;;
    -h)
      echo "${USAGE}"
      exit $RETURN_SUCCESS
      ;;
    *)
      # Invalid arguments passed
      echo "${USAGE}"
      exit $RETURN_ERR_ARGS
  esac
done


if [ "$isOnlyRoles" = "1" ]; then
  oldSecure=$(read_secure)
  updSecure="false"
  if [ -n "$isSecure" ] && [ "$isSecure" != "$oldSecure" ]; then
    updSecure="true"
  fi

  #init_ksql_confs

  if [ "$updSecure" = "true" ]; then
    write_secure "$isSecure"
  fi

  chown_component

  create_internal_stream_if_needed

  setup_warden_conf

  if [ -f "$KSQL_FILE_NOT_CONFIGURED" ]; then
    rm -f "$KSQL_FILE_NOT_CONFIGURED"
  elif [ "$updSecure" = "true" ]; then
    create_restart_file
  fi
fi
