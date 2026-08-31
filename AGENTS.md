# Repository Agent Instructions

Read `CONSTRAINTS.md` before writing code. Do not weaken it to make a change pass.

For `paimon-plus-connector`, data accuracy, commit-state consistency, resource ownership, and provable asynchronous termination are release-blocking requirements. Preserve unrelated working-tree changes and keep each lifecycle change independently testable and revertible.
