#!/bin/sh
set -e

cd "$(dirname "$0")"

if ! [ -f messages/target/messages-*.jar ]; then
    mvn install
fi

cd clients-spring
exec mvn spring-boot:run -Dspring-boot.run.arguments="$*"
