package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class SchemaChangeVisitorBase implements SchemaChangeVisitor {

  protected Schema sourceSchema;
  protected SchemaResource schemaResource;
  protected SqlDialect sqlDialect;

  protected abstract void writeStatements(Collection<String> statements);


  public SchemaChangeVisitorBase(Schema sourceSchema, SchemaResource schemaResource, SqlDialect sqlDialect) {
    this.sourceSchema = sourceSchema;
    this.schemaResource = schemaResource;
    this.sqlDialect = sqlDialect;
  }


  @Override
  public void visit(AddIndex addIndex) {
    sourceSchema = addIndex.apply(sourceSchema);
    Index foundIndex = null;
    if (schemaResource != null && schemaResource.getAdditionalMetadata().isPresent()) {
      AdditionalMetadata additionalMetadata = schemaResource.getAdditionalMetadata().get();
      String tableName = addIndex.getTableName().toUpperCase(Locale.ROOT);
      if (additionalMetadata.ignoredIndexes().containsKey(tableName)) {
        List<Index> tableIgnoredIndexes = additionalMetadata.ignoredIndexes().get(tableName);
        for (Index index : tableIgnoredIndexes) {
          if (index.columnNames().equals(addIndex.getNewIndex().columnNames())) {
            foundIndex = index;
            break;
          }
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
