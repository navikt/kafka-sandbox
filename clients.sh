#!/bin/sh
set -e

if ! test -f clients/target/clients-*.jar; then
    mvn install
fi

exec java -jar clients/target/clients-*.jar "$@"
