package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema     currentSchema;
  protected SqlDialect sqlDialect;
  protected final UpgradeConfigAndContext upgradeConfigAndContext;

  protected abstract void writeStatements(Collection<String> statements);


  public AbstractSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect) {
    this.currentSchema = currentSchema;
    this.upgradeConfigAndContext = upgradeConfigAndContext;
    this.sqlDialect = sqlDialect;
  }


  @Override
  public void visit(AddIndex addIndex) {
    currentSchema = addIndex.apply(currentSchema);
    Index foundIndex = null;
    List<Index> ignoredIndexes = upgradeConfigAndContext.getIgnoredIndexesForTable(addIndex.getTableName());
    for (Index index : ignoredIndexes) {
      if (index.columnNames().equals(addIndex.getNewIndex().columnNames())) {
        foundIndex = index;
        break;
      }
    }

    if (foundIndex != null) {
      writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(addIndex.getTableName()), foundIndex.getName(), addIndex.getNewIndex().getName()));
    } else {
      writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(addIndex.getTableName()), addIndex.getNewIndex()));
    }
  }

}
