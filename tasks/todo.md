# Paimon Plus Writer Spill Governance TODO

> Source of truth: [`tasks/plan.md`](plan.md) and [`paimon-writer-spill-options-governance-spec.md`](../connectors/paimon-plus-connector/src/doc/paimon-writer-spill-options-governance-spec.md).
>
> Planning only: no implementation task is complete. Every Java command must use JDK 17 and `-Dmaven.compiler.release=11`. S3/S3A buffer directory work is out of scope.

## Phase 1: Configuration Foundation

- [ ] Task 1 — Publish native UI fields and protect legacy open-save-run compatibility.
  - [ ] New tasks do not emit legacy keys; hidden/no-default fallback remains until target UI evidence exists.
  - [ ] `PaimonSpecTest` locks the fallback; target UI/Engine round-trip remains a Task 13 release gate.
- [ ] Task 2 — Implement Paimon-native typed normalization, raw-shape errors, and recursive snapshot isolation.
  - [ ] `PaimonWriterOptionsNormalizerTest` covers native/legacy/default precedence and invalid runtime shapes.
  - [ ] No duplicated parser, key, default, unit conversion, or shallow-copy alias remains.
- [ ] Task 3 — Capture deep startup input and delay four-key/temp-directory parsing to first target intent.
  - [ ] Source/connection-test/schema contexts do not parse unused Writer or directory inputs.
  - [ ] Directory comparison preserves order/duplicates and uses Paimon split plus JDK lexical normalization.

### Checkpoint A

- [ ] Tasks 1–3 acceptance criteria pass under JDK 17 with `release=11`.
- [ ] Input immutability and legacy round-trip strategy have review evidence.
- [ ] Human review approves configuration foundation.

## Phase 2: Preflight and Catalog Reconciliation

- [ ] Task 4 — Move existing semantic/HASH_DYNAMIC preflights before Catalog mutation.
  - [ ] Resolver and DynamicBucketPreflight tests cover normal 3-page, KEY_DYNAMIC 6-page and marker-aware HASH_DYNAMIC paths.
  - [ ] No semantic or dynamic-bucket logic is duplicated in a new framework/helper.
- [ ] Task 5 — Produce a Catalog-zero per-table plan and immutable full Paimon Schema summary.
  - [ ] `PaimonTableOptionsReconcilerTest` proves plan only schedules HASH_DYNAMIC preflight and makes no marker/IOManager/CREATE/ALTER side effect.
- [ ] Task 6 — Apply missing-key ALTER, reload-classify uncertain results, and conditionally compensate.
  - [ ] Tests cover applied-before-exception, not-applied, partial/diverged, unknown and concurrent-value skip.
  - [ ] Final status is explicitly `RESTORED`, `RESIDUAL`, or `UNKNOWN`.

### Checkpoint B

- [ ] Tasks 4–6 acceptance criteria pass.
- [ ] Only pre-mutation deterministic failures claim Catalog zero change.
- [ ] Human review approves Paimon reuse and Catalog side effects.

## Phase 3: Gate and Production Paths

- [ ] Task 7 — Implement task/per-Identifier single-flight on the existing Service Lifecycle.
  - [ ] Tests prove exactly-once plan/cleanup/IOManager-smoke/HASH-preflight/mutation ordering, one sticky failure owner and safe stop/DDL lock order.
- [ ] Task 8 — Wire new-table CREATE with full summary comparison and post-exception reload classification.
  - [ ] Non-equivalent/unknown tables create no Writer and are not automatically dropped.
- [ ] Task 9 — Wire existing/runtime tables and invalidate governance registry on DDL/UUID changes.
  - [ ] INITIAL/CDC/rebuild share one result; drop/recreate and UUID-unavailable cases re-govern.

### Checkpoint C

- [ ] Tasks 7–9 acceptance criteria pass.
- [ ] New, existing and runtime tables share one lifecycle, registry and failure result.
- [ ] Human review approves state machine and production-path wiring.

## Phase 4: Runtime Resource and Observability

- [ ] Task 10 — Use the final task directory for protected stale cleanup before smoke/preflight/mutation.
  - [ ] Only final task roots are scanned; ignored legacy per-table roots are WARN-only, and source/connection/schema paths do not run cleanup.
  - [ ] Existing live-dir, owner-lock, grace and lockless rolling-upgrade protection is reused; cleanup failure is best-effort WARN.
- [ ] Task 11 — Preserve unique Writer/Committer/IOManager ownership and failure-safe close semantics.
  - [ ] Normal close order, idempotence, primary/suppressed ordering and final live-dir/owner-lock release are fault-injection tested.
  - [ ] Every construction boundary releases acquired resources in ownership-reverse order without raw-Writer double-close or resource leakage.
- [ ] Task 12 — Complete stable errors and remove full DataMap/options/finalSchema logging.
  - [ ] Log tests cover safe summaries, mutation classifications and residual states.

### Checkpoint D

- [ ] Tasks 10–12 acceptance criteria pass.
- [ ] stale cleanup, Writer/Committer/IOManager ownership, safe logs and public-observable option verification pass review.
- [ ] Human review approves release-gate execution.

## Phase 5: Release and Documentation Closure

- [ ] Task 13 — Run full Spec matrix, JDK 17/Java 11 build, Engine enumeration and UI round-trip gates.
  - [ ] Active compiler source/target is 11 and `release=11` package/full test pass.
  - [ ] Target `PdkTableMap` enumeration and old-task open-save-run pass; unavailable environments remain blockers.
  - [ ] JSON and tracked/untracked whitespace checks pass.
- [ ] Task 14 — Replace plan assumptions with actual Paimon reuse audit and synchronize Spec/Plan/Todo.
  - [ ] No unexplained new tool/domain class, reflection, copied Paimon logic or stale 1.3.1 source link remains.
  - [ ] Actual test names, results, blockers and document status are truthful and synchronized.

### Checkpoint E

- [ ] Every task satisfies acceptance criteria and the project-wide Definition of Done.
- [ ] Full Plus test/build and Engine/UI external gates pass.
- [ ] Human review explicitly approves merge readiness.

## External Gates

- [ ] Target Engine `PdkTableMap` runtime integration is available or an equivalent fixture is approved.
- [ ] Target UI/Engine old-task open-save-run evidence is recorded; until then hidden/deprecated aliases remain.
- [ ] `max-disk-size` verification is limited to persisted raw option and public `CoreOptions` unless a stable public behavior seam is proven.
