package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema sourceSchema;
  protected SqlDialect sqlDialect;
  protected final SchemaResource schemaResource;

  protected abstract void writeStatements(Collection<String> statements);


  public AbstractSchemaChangeVisitor(Schema sourceSchema, SchemaResource schemaResource, SqlDialect sqlDialect) {
    this.sourceSchema = sourceSchema;
    this.schemaResource = schemaResource;
    this.sqlDialect = sqlDialect;
  }


  @Override
  public void visit(AddIndex addIndex) {
    sourceSchema = addIndex.apply(sourceSchema);
    Index foundIndex = null;
    Table table = schemaResource.getTable(addIndex.getTableName());
    if (!table.ignoredIndexes().isEmpty()) {
        List<Index> tableIgnoredIndexes = table.ignoredIndexes();
        for (Index index : tableIgnoredIndexes) {
          if (index.columnNames().equals(addIndex.getNewIndex().columnNames())) {
            foundIndex = index;
            break;
          }
        }
    }

    if (foundIndex != null) {
      writeStatements(sqlDialect.renameIndexStatements(sourceSchema.getTable(addIndex.getTableName()), foundIndex.getName(), addIndex.getNewIndex().getName()));
    } else {
      writeStatements(sqlDialect.addIndexStatements(sourceSchema.getTable(addIndex.getTableName()), addIndex.getNewIndex()));
    }
  }

}
