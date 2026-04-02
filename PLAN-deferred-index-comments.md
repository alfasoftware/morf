# Deferred Index Creation — Comments-Based Model

See integration guide at: `~/deferred-index-comments-integration-guide.md`
See dev description at: `~/deferred-index-comments-dev.txt`

## Summary

Replace the tracking-table approach (`DeferredIndexOperation` table + DAO + state machine)
with a comments-based declarative model (~3600 net lines removed):

- `Index.isDeferred()` — deferred is a property on the index itself
- Table comments store permanent `DEFERRED:[name|cols|unique]` segments
- MetaDataProvider merges comments with physical catalog: declared indexes always have `isDeferred()=true`
- Executor scans for indexes that are `isDeferred()=true` but not physically present
- `getMissingDeferredIndexStatements()` exposes raw SQL for custom execution
- IF EXISTS DDL for safe operations on deferred indexes (built or not)
- "Change the plan" — unbuilt deferred indexes modified in later upgrades without force-build

## Branch

`experimental/deferred-index-comments` (branched from `experimental/deferred-index-creation`)

## Key Design Rule

DEFERRED comment segments are **permanent** — never removed when the index is physically built.
They are only modified during upgrade steps (removeIndex, changeIndex, renameIndex).
The executor never writes comments.
