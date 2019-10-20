#!/bin/sh
set -e

if ! test -f target/kafka-sandbox*.jar; then
    mvn package
fi

exec java -jar target/kafka-sandbox*.jar "$@"
