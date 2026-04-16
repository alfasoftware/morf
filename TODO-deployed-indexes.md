# TODO — experimental/deployed-indexes branch

## Must fix
- **Column rename does not propagate to deferred index in schema model.** `ChangeColumn.apply()` renames the column in the table's column list but does NOT rewrite `Index.columnNames()` strings. After `applyToSchema()`, the deferred index still references the old column name. `getDeferredIndexStatements()` generates SQL with the old column name, which fails at execution time. The DeployedIndexes table IS correctly updated via `DeployedIndexesChangeService.updateColumnName()`. Fix: after `ChangeColumn.apply()`, rebuild deferred indexes in the schema model with the new column name using `TableOverrideSchema` (same approach as the comments branch fix in `AbstractSchemaChangeVisitor.updateDeferredIndexColumnName()`).

## Should fix
- **Package rename for integration test upgrade step fixtures.** The fixture classes in `upgrade/v1_0_0/` and `upgrade/v2_0_0/` still have names like `AddDeferredIndex`, `AddDeferredUniqueIndex`, etc. which reference the old "deferred" terminology. Consider renaming to reflect the new architecture (e.g. `AddIndexDeferred` or keeping as-is since "deferred" accurately describes what the index is).
- **Prepopulation integration test.** No test verifies that the `CreateDeployedIndexes` upgrade step correctly populates existing physical indexes into the DeployedIndexes table. Needs a test with a pre-existing index before the step runs.
- **ChangeColumn on non-deferred index.** No integration test verifies that renaming a column on a non-deferred (COMPLETED) index also updates the `indexColumns` field in DeployedIndexes.
