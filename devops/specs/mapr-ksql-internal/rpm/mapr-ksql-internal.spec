%undefine __check_files

summary:     HPE DataFabric Ecosystem Pack: Confluent KSQL
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-ksql-internal
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-kafka >= 3.6.1
conflicts:   mapr-core < 7.2.0, mapr-kafka < 3.6.1
AutoReqProv: no


%description
Confluent KSQL distribution included in HPE DataFabric Software Ecosystem Pack
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/ksql

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "2" ]; then
   if [ -f  __PREFIX__/ksql/ksqlversion ]; then
      bash __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/bin/ksql-server-stop
      bash __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/bin/ksql-stop
   fi

   if [ -d __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/logs ]; then
      rm -rf __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/logs
   fi
   #Saving of old configurations
   OLD_TIMESTAMP=$(rpm --queryformat='%%{VERSION}' -q mapr-ksql)
   OLD_VERSION=$(echo "$OLD_TIMESTAMP" | grep -o '^[0-9]*\.[0-9]*\.[0-9]*')

   OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-ksql-old-timestamp"
   OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-ksql-old-version"

   STATE_DIR="$(dirname $OLD_TIMESTAMP_FILE)"
   if [ ! -d "$STATE_DIR" ]; then
       mkdir -p "$STATE_DIR"
   fi

   echo "$OLD_TIMESTAMP" > "$OLD_TIMESTAMP_FILE"
   echo "$OLD_VERSION" > "$OLD_VERSION_FILE"

   mkdir -p __PREFIX__/ksql/ksql-${OLD_TIMESTAMP}/etc/ksql
   cp __PREFIX__/ksql/ksql-${OLD_VERSION}/etc/ksql/* __PREFIX__/ksql/ksql-${OLD_TIMESTAMP}/etc/ksql

   DAEMON_CONF=__PREFIX__/conf/daemon.conf
   if [ -f "$DAEMON_CONF" ]; then
       MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
       MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
       if [ ! -z "$MAPR_USER" ]; then
           chown -R ${MAPR_USER}:${MAPR_GROUP} __PREFIX__/ksql/ksql-${OLD_TIMESTAMP}
       fi
   fi
fi


%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

#
# change permissions
#
DAEMON_CONF=__PREFIX__/conf/daemon.conf
if [ -f "$DAEMON_CONF" ]; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
    if [ ! -z "$MAPR_USER" ]; then
        chown -R ${MAPR_USER} __PREFIX__/ksql/
    fi
    if [ ! -z "$MAPR_GROUP" ]; then
        chgrp -R ${MAPR_GROUP} __PREFIX__/ksql/
    fi
fi

mkdir -p "__INSTALL_3DIGIT__"/logs
chmod 1777 "__INSTALL_3DIGIT__"/logs


%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ -f  __PREFIX__/ksql/ksqlversion ]; then
    bash __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/bin/ksql-server-stop
    bash __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/bin/ksql-stop
    if [ -d __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/logs ]; then
        rm -rf __PREFIX__/ksql/ksql-__VERSION_3DIGIT__/logs
    fi
fi

%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" = "0" ]; then
    rm -rf __PREFIX__/ksql
fi

%posttrans
# $1 -eq 0 install
# $1 -eq 0 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-ksql-old-timestamp"
OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-ksql-old-version"

# This files will exist only on upgrade
if [ -e "$OLD_TIMESTAMP_FILE" ] && [ -e "$OLD_VERSION_FILE" ]; then
    OLD_TIMESTAMP=$(cat "$OLD_TIMESTAMP_FILE")
    OLD_VERSION=$(cat "$OLD_VERSION_FILE")

    rm "$OLD_TIMESTAMP_FILE" "$OLD_VERSION_FILE"

    # Remove directory with old version
    NEW_VERSION=$(cat __PREFIX__/ksql/ksqlversion)

    if [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
        rm -rf "__PREFIX__/ksql/ksql-${OLD_VERSION}"
    fi
fi
