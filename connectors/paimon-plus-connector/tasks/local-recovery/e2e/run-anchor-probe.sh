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
package=io.tapdata.flow.engine.V2.node.hazelcast.data.pdk
sources=("$fixture_dir/EngineAnchorProbe.java")
mode="${1:---anchor}"
case "$mode" in
  --anchor) ;;
  --concurrent|--coupled)
    module="$(cd "$fixture_dir/../../.." && pwd)"
    report="$module/target/surefire-reports/TEST-io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest.xml"
    if [[ ! -f "$report" ]]; then
        echo "Run PaimonStartupRecoveryIntegrationTest before this joint probe" >&2
        exit 2
    fi
    connector_cp="$(python3 - "$report" <<'CP_PY'
import sys
from xml.etree import ElementTree
props = ElementTree.parse(sys.argv[1]).findall(".//property")
print(next(p.get("value") for p in props if p.get("name") == "surefire.test.class.path"))
CP_PY
)"
    cp="$cp:$connector_cp"
    if [[ "$mode" == --concurrent ]]; then
        sources+=("$fixture_dir/EngineConcurrentAnchorProbe.java")
    else
        sources+=("$fixture_dir/EnginePaimonReplayProbe.java")
    fi
    ;;
  *) echo "Usage: bash run-anchor-probe.sh [--anchor|--concurrent|--coupled]" >&2; exit 2 ;;
esac
"$JAVA_HOME/bin/javac" --release 17 -proc:none -cp "$cp" -d "$scratch" "${sources[@]}"
run_java() {
    "$JAVA_HOME/bin/java" -Xmx768m --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
        "-javaagent:$agent" -cp "$scratch:$cp" "$package.$1" "${@:2}"
}
run_coupled_case() (
    local first="$1" restart="$2" first_exit="$3"
    local run_id state phase expected rc
    run_id="$(python3 -c 'import secrets; print(secrets.token_hex(12))')"
    state="$scratch/$first"
    mkdir "$state"
    if [[ -n "${PROBE_MONGO_URI:-}" ]]; then
        cleanup_probe() {
            local result=$?
            if ! run_java EnginePaimonReplayProbe "$state" cleanup "$run_id" >"$state/cleanup.log" 2>&1; then
                echo "Could not verify cleanup of test document engine_paimon_probe/$run_id" >&2
                result=1
            fi
            exit "$result"
        }
        trap cleanup_probe EXIT
    fi
    for phase in "$first" "$restart"; do
        expected=0
        if [[ "$phase" == "$first" ]]; then expected=$first_exit; fi
        rc=0
        run_java EnginePaimonReplayProbe "$state" "$phase" "$run_id" >"$state/$phase.log" 2>&1 || rc=$?
        if [[ "$rc" != "$expected" ]]; then cat "$state/$phase.log" >&2; exit 1; fi
        sed -n '/^PASS /p' "$state/$phase.log"
    done
)
case "$mode" in
    --anchor) run_java EngineAnchorProbe ;;
    --concurrent) run_java EngineConcurrentAnchorProbe "$scratch" ;;
    --coupled)
        run_coupled_case initial guard-initial 42
        run_coupled_case continuous guard-cdc 43
        ;;
esac
