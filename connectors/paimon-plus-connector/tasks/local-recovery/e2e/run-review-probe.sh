#!/usr/bin/env bash
# Example: JAVA_HOME=/path/to/jdk-17 bash run-review-probe.sh LocalRecoveryImageAudit
set -euo pipefail
: "${JAVA_HOME:?Set JAVA_HOME to JDK 17}"
probe="${1:?Pass the simple Java class name, followed by any probe-specific arguments}"
shift
fixture_dir="$(cd "$(dirname "$0")" && pwd)"
module="$(cd "$fixture_dir/../../.." && pwd)"
[[ "$probe" =~ ^[A-Za-z][A-Za-z0-9_]*$ && -f "$fixture_dir/$probe.java" ]] || { echo "Unknown probe $probe" >&2; exit 2; }
report="$module/target/surefire-reports/TEST-io.tapdata.connector.paimon.service.PaimonStartupRecoveryIntegrationTest.xml"
[[ -f "$report" ]] || { echo "Run PaimonStartupRecoveryIntegrationTest in this checkout first" >&2; exit 2; }
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/work"
python3 - "$report" "$scratch" <<'PY'
from pathlib import Path
import os, sys, xml.etree.ElementTree as ET
report, dest = sys.argv[1], Path(sys.argv[2])
cp = next(p.get('value') for p in ET.parse(report).findall('.//property') if p.get('name') == 'surefire.test.class.path')
agent = next(path for path in cp.split(os.pathsep) if Path(path).name.startswith('byte-buddy-agent-') and path.endswith('.jar'))
def arg(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'
(dest/'compile.args').write_text('-cp\n' + arg(cp) + '\n')
(dest/'run.args').write_text(arg('-javaagent:' + agent) + '\n-cp\n' + arg(str(dest) + os.pathsep + cp) + '\n')
PY
"$JAVA_HOME/bin/javac" -proc:none --release 17 "@$scratch/compile.args" -d "$scratch" "$fixture_dir/$probe.java"
"$JAVA_HOME/bin/java" --add-opens=java.base/java.lang=ALL-UNNAMED "@$scratch/run.args" \
    "io.tapdata.connector.paimon.service.$probe" "$scratch/work" "$@"
