#!/bin/sh
set -e

cd "$(dirname "$0")"

if ! test -f clients/target/clients-*.jar; then
    mvn install
fi

exec java -jar clients/target/clients-*.jar "$@"
