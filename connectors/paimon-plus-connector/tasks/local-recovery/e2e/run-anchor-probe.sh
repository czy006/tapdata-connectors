#!/usr/bin/env bash
# Execute against the engine's dependency graph. No TapData engine or connector source is edited.
set -euo pipefail
: "${TAPDATA_REPO:?Set TAPDATA_REPO to the local TapData checkout}"
: "${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
maven="${MAVEN:-mvn}"
fixture_dir="$(cd "$(dirname "$0")" && pwd)"
engine="$TAPDATA_REPO/iengine/iengine-app"
pin=65add935fdee7fa6bccb5abfb85555b973d69697
actual="$(git -C "$TAPDATA_REPO" rev-parse HEAD)"
if [[ "$actual" != "$pin" ]]; then
    echo "Expected TapData $pin, got $actual" >&2
    exit 2
fi
if [[ ! -f "$engine/target/classes/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastTargetPdkBaseNode.class" ]]; then
    echo "Build iengine-app at the pinned commit first" >&2
    exit 2
fi
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
"$maven" -o -q -f "$engine/pom.xml" org.apache.maven.plugins:maven-dependency-plugin:3.7.0:build-classpath \
    "-Dmdep.outputFile=$scratch/dependencies"
deps="$(cat "$scratch/dependencies")"
agent="$(tr ':' '\n' < "$scratch/dependencies" | awk '/\/byte-buddy-agent-[0-9.]+\.jar$/ {print; exit}')"
if [[ -z "$agent" ]]; then
    echo "No Byte Buddy agent found in the engine test classpath" >&2
    exit 2
fi
cp="$engine/target/classes:$TAPDATA_REPO/iengine/iengine-common/target/classes:$deps"
"$JAVA_HOME/bin/javac" --release 17 -proc:none -cp "$cp" -d "$scratch" "$fixture_dir/EngineAnchorProbe.java"
"$JAVA_HOME/bin/java" --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
    "-javaagent:$agent" -cp "$scratch:$cp" io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.EngineAnchorProbe
