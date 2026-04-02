# Deferred Index Creation — Comments-Based Model

See full plan at: `~/.claude/plans/abstract-coalescing-pillow.md`
See integration guide at: `~/deferred-index-comments-integration-guide.md`
See dev description at: `~/deferred-index-comments-dev.txt`

## Summary

Replace the tracking-table approach (`DeferredIndexOperation` table + DAO + state machine)
with a comments-based declarative model:

- `Index.isDeferred()` — deferred is a property on the index itself
- Table comments store `DEFERRED:[name|cols|unique]` segments
- Comments are permanent (never removed after build)
- Executor scans comments vs physical catalog to find what needs building
- `getMissingDeferredIndexStatements()` exposes raw SQL for custom execution
- IF EXISTS DDL for safe operations on potentially-unbuilt indexes

## Branch

`experimental/deferred-index-comments` (branched from `experimental/deferred-index-creation`)
