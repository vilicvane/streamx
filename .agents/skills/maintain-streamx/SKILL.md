---
name: maintain-streamx
description: Maintain, debug, review, and evolve the StreamX Rust stream-operator library. Use when changing StreamX operators, shared runtime infrastructure, public APIs, tests, Rust documentation, user guidance, or behavioral contracts, especially work involving polling, buffering, back pressure, task lifetime, completion, replay, fairness, or reload/restart behavior.
---

# Maintain StreamX

Preserve StreamX's documented behavior while allowing its implementation and
maintenance process to evolve. Establish direct causal evidence for bug fixes,
keep code, tests, and documentation aligned, and improve this skill whenever it
could guide the next maintenance task better.

## Resolve the repository

Treat the directory three levels above this file as the repository root. Do not
assume the current working directory is the repository root.

Before editing:

1. Inspect the worktree and preserve unrelated or user-owned changes.
2. Locate definitions and tests with `rg`; do not infer an API from call sites
   alone.
3. Identify whether the task changes observable behavior, only implementation,
   only documentation, or some combination.

## Load authoritative context

Do not duplicate operator semantics in this skill. Read them from the repository
sources that own them:

| Question | Source of truth |
| --- | --- |
| How users select and operate the library | [README](../../../README.md) |
| Observable behavior and implementation invariants | [operator semantics and maintainer guide](../../../OPERATOR_SEMANTICS.md) |
| Exact public signatures and item-level Rust documentation | Relevant files under [`src/`](../../../src) |
| Current executable behavior | Focused unit tests beside the relevant implementation |
| Runtime features and dependency constraints | [`Cargo.toml`](../../../Cargo.toml) |

Read the README before changing user-visible API, requirements, selection
guidance, examples, capacity, lifetime, or completion behavior. Read the full
operator semantics guide before changing or reviewing observable behavior or
shared runtime infrastructure. For a narrow implementation-only task, read at
least its global invariants, semantic matrix, relevant operator contract, and
relevant implementation-invariant section.

If documentation, tests, and implementation disagree, stop and make the
conflict explicit. Determine the intended contract from the task and available
evidence; do not silently choose whichever source is easiest to change.

## Establish causality before fixing a bug

For a reported runtime symptom:

1. State the exact observable failure and the event or lifecycle transition
   correlated with it.
2. Trace construction, upstream ownership, polling, buffering, wakeups,
   completion, and drop only as relevant to that path.
3. Separate possible defects, intentional idempotent behavior, and proven
   causes. Do not present a possible defect as the cause without connecting it
   to the symptom.
4. Prefer a deterministic regression test that fails for the reported reason.
   Avoid relying on timing luck or long sleeps.
5. Verify the fix through that causal path, then check adjacent shared behavior
   for regressions.

Do not attribute a failure to an external source, scheduler, or coincidental
stall unless evidence rules out the local lifecycle and state-transition path.

## Make a coherent change

Before implementation, write down the affected contract in task-local notes or
the working plan. Use the repository documents for the contract itself rather
than copying their content into the plan.

Then:

1. Make the smallest coherent implementation change that satisfies the
   contract; internal structure is not sacred.
2. Add or update focused tests for every changed observable boundary.
3. Re-check shared infrastructure consumers rather than assuming a local test
   covers them.
4. Keep public type bounds and signatures no stronger than ownership and
   implementation actually require.
5. Preserve fairness and release behavior when adding internal loops or
   background work; verify them with tests described by the maintainer guide.

## Synchronize documentation

Update documentation in the same change when its owned facts change:

- Update the README for user-visible selection, usage, requirements, examples,
  capacity, replay, lifetime, or completion changes.
- Update the operator semantics guide for observable contracts, public shapes,
  cross-cutting implementation invariants, or required test coverage.
- Update item-level Rust documentation for public API details and constraints.
- Keep README examples compilable through the crate-level doctest integration.

Avoid historical discussion logs and temporary decision status in normative
documents. Describe the resulting contract and rationale needed to maintain it.
If no documentation changes are needed, verify that consciously and state why
in the handoff.

## Validate proportionally

Run focused tests while iterating. Before handing off a cross-cutting or public
change, normally run from the repository root:

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
git diff --check
```

Use explicit timeouts when the execution environment requires protection from
hanging commands. If a check cannot run, report exactly which evidence is
missing and why.

Review the final diff for unintended changes and confirm that new files are
tracked by status even if they do not appear in a normal unstaged diff.

## Evolve this skill

Actively evaluate this skill after every StreamX maintenance task. Update it in
the same change when any of the following is true:

- a source-of-truth file, validation command, architectural boundary, or
  maintenance workflow moved or changed;
- the task exposed a recurring failure mode or review step that this workflow
  did not catch;
- following the skill caused ambiguity, unnecessary work, or missed evidence;
- the trigger description no longer covers the work for which the skill should
  load;
- UI metadata under `agents/openai.yaml` no longer describes the skill.

Keep skill updates procedural. Link to repository-owned facts instead of
copying semantic details here. Remove stale instructions rather than preserving
history. After changing the skill, run the `skill-creator` validator when it is
available and re-read the skill as if arriving without prior conversation.

In the final handoff, state which user documentation, maintainer contract,
tests, and skill guidance changed—or why each did not need to change.
