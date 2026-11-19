#!/usr/bin/env bash
set -euo pipefail
: "${JAVA_HOME:?Set JAVA_HOME to JDK 17}"
fixture_dir="$(cd "$(dirname "$0")" && pwd)"
module="$(cd "$fixture_dir/../../.." && pwd)"
report="$module/target/surefire-reports/TEST-io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest.xml"
if [[ ! -f "$report" ]]; then
    echo "Run PaimonStartupRecoveryIntegrationTest for this checkout first (the fixture uses that test classpath)." >&2
    exit 2
fi
batches="${BATCHES:-16}"
batch_size="${BATCH_SIZE:-512}"
runs="${RUNS:-3}"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
cp="$(python3 - "$report" <<'PY'
import sys
import xml.etree.ElementTree as ET
properties = {p.get("name"): p.get("value") for p in ET.parse(sys.argv[1]).findall(".//property")}
print(properties["surefire.test.class.path"])
PY
)"
mkdir "$scratch/connector-classes"
cp -R "$module/target/classes/." "$scratch/connector-classes"
cp="$scratch/connector-classes:$cp"
agent="$(awk -v RS=: '/\/byte-buddy-agent-[0-9.]+\.jar$/ { print; exit }' <<< "$cp")"
if [[ ! -f "$agent" ]]; then echo "Missing Byte Buddy test agent" >&2; exit 2; fi
"$JAVA_HOME/bin/javac" --release 17 -proc:none -cp "$cp" -d "$scratch" \
    "$module/src/test/java/io/tapdata/connector/paimon/service/PaimonStartupRecoveryIntegrationTest.java" \
    "$fixture_dir/ServiceRecoveryBenchmark.java"
run_phase() {
    local dir="$1" mode="$2" phase="$3" expected="$4" rc=0 log
    mkdir -p "$dir"
    log="$dir/$phase.log"
    "$JAVA_HOME/bin/java" -Xms256m -Xmx512m --add-opens java.base/java.lang=ALL-UNNAMED \
        "-javaagent:$agent" -cp "$scratch:$cp" io.tapdata.connector.paimon.service.ServiceRecoveryBenchmark \
        "$dir" "$mode" "$phase" "$batches" "$batch_size" > "$log" 2>&1 || rc=$?
    if [[ "$rc" != "$expected" ]]; then tail -80 "$log" >&2; return 1; fi
    if [[ "$phase" != crash ]]; then sed -n '/^SAMPLE /p' "$log"; fi
}
for ((i=1; i<=runs; i++)); do
    echo "run=$i batches=$batches batch_size=$batch_size"
    if (( i % 2 == 0 )); then modes=(strong legacy); else modes=(legacy strong); fi
    for mode in "${modes[@]}"; do run_phase "$scratch/$i/$mode" "$mode" normal 0; done
    run_phase "$scratch/$i/restart" strong crash 42
    run_phase "$scratch/$i/restart" strong recover 0
done
