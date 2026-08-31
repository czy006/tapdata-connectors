# Constraints

Last reviewed: 2026-09-01 for the Paimon spill and asynchronous resource-lifecycle root fix.

## Scope and verified baseline

- Connector repository branch point: `develop@3b8e6d982266e430d825b1309038e84f4645d3ef`.
- Connector implementation branch: `codex/paimon-spill-lifecycle-spec-v5`.
- Paimon fork baseline: Apache `release-1.3.2` dereferenced to `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`.
- Paimon fork implementation branch: `codex/spill-lifecycle-root-fix`.
- The Connector module's effective compiler configuration is Java source/target 11. The earlier Java 8 properties are overridden by the module's later duplicate compiler-plugin declaration. The Paimon 1.3.2 fork remains source/target 1.8.
- The local evidence environment is Maven 3.9.12 on JDK 17. It is an evidence environment, not a claim that production runs JDK 17.

Verify the immutable source baselines with:

```bash
git merge-base develop HEAD
git -C ../paimon merge-base c05f7d1f1b1e5d37e64edab0f2978124d90b64f7 HEAD
git ls-remote https://github.com/apache/paimon.git \
  'refs/tags/release-1.3.2^{}'
```

The two commit results must equal the values above. A mismatch blocks implementation because source-level conclusions would no longer apply.

## Floor (always blocking)

- No new suppression comments or annotations used to silence a checker.
- No unimplemented stubs, empty catch blocks, swallowed lifecycle failures, or warning-only handling for ownership/close failures.
- No skipped, disabled, weakened, or deleted tests without an explicit reviewed exception in the commit message.
- No credentials, credential-bearing URIs, raw access keys, or secret values in source, tests, logs, metrics, or evidence.
- No production fallback to reflection, version guessing, timing sleeps, thread-name polling, broad registry clearing, or unproven force-close.
- No dependency resource is closed or deleted unless its owner and all asynchronous borrowers have positively terminated.
- `FAILED_DRAINED` proves runnable termination only. It blocks downstream FileIO/spill/lease release because the operation still failed.
- A read owner must close and drain every returned structured operation before releasing batch, reader, table, Catalog, or FileIO resources.
- This file must not be weakened in the same change that needs the weaker rule to pass.

Review the diff from the Connector branch point with:

```bash
git diff --check develop...HEAD
git diff --unified=0 develop...HEAD -- \
  'connectors/paimon-plus-connector/**' 'tasks/**' 'AGENTS.md' 'CONSTRAINTS.md'
```

The review is blocking on any newly added suppression, stub, skipped test, removed assertion, secret, or weakened threshold. Report only the rule and location for suspected secrets; never print the value.

## Enforced now

| Dimension | Blocking rule | Checked by | Why |
|---|---|---|---|
| Connector compilation | Zero compiler errors against the module's effective Java 11 configuration | `mvn -pl connectors/paimon-plus-connector -am -DskipTests install` | A lifecycle patch that is not consumable by the real Connector reactor cannot ship. |
| Connector behavior | Zero failures/errors in every focused lifecycle suite and in the full module suite; no new skipped tests | `mvn -pl connectors/paimon-plus-connector -am test` plus focused `-Dtest=...` RED/GREEN commands recorded per commit | Data consistency and exact-close behavior require deterministic regression tests, not log inspection. |
| Paimon compilation | Zero compiler/checkstyle/Spotless errors for every changed Paimon module | From the Paimon fork root: `mvn -pl paimon-api,paimon-common,paimon-core -am -DskipTests install` | The patched API/Common/Core artifacts form one ABI-compatible stack. |
| Paimon behavior | Zero failures/errors in every focused structured-lifecycle suite and in changed-module suites; no new skipped tests | From the Paimon fork root: focused `mvn -pl <module> -Dtest=<test> test`, then `mvn -pl paimon-api,paimon-common,paimon-core -am test` | Early-abandon, interruption, multi-failure, and ownership races must be executable facts. |
| Dependency closure | Zero mixed original/patched Paimon stack artifacts and zero unresolved patched ecosystem coordinates | `mvn -pl connectors/paimon-plus-connector dependency:tree` and clean-repository resolution of the release manifest | A partial patch can pass compilation yet load incompatible classes at runtime. |
| Release immutability | Zero SNAPSHOT Paimon patch artifacts; SHA-256 recorded for every binary, sources JAR, and POM | `mvn ... deploy` output plus `shasum -a 256 <artifact>` in the release evidence | Production diagnosis must map a running JAR to one immutable source delta. |

## Release gates awaiting their executable harness

The numerical W1-W4 performance and lifecycle budgets in the approved Spec remain blocking. They are not duplicated here until Tasks 27, 37, and 38 install commands that run in this repository; a number without an executable checker is not a constraint. Publication is blocked until those tasks add the command, measured baseline, reason, and result to this file.

Changed lifecycle behavior must have a deterministic RED test before its production change and a GREEN result afterward. The target for changed behavioral branches is at least 80% line coverage once the task-end JaCoCo changed-line check is installed; 80% is the default forcing meaningful tests while allowing generated/configuration lines. Until that checker exists, code review must treat missing branch/error-path tests as Required rather than claiming the percentage passed.

## Exceptions

There are no approved exceptions. Any future exception requires a rule, exact path, reason, owner, and expiry no later than 90 days; 90 days is long enough to schedule repair without creating a permanent bypass.
