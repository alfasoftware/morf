package org.alfasoftware.morf.upgrade;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.field;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.CtasDuringUpgrade;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**
 * Implementation of {@link SchemaChangeToSchemaAdaptor}, which translates
 * {@link CtasDuringUpgrade}-hinted UPDATE statements to CTAS constructs.
 *
 * <p>
 * Caveat: Throws on hinted UPDATE statements with WHERE clauses,
 * because an UPDATE with a WHERE clause should not be optimised into a CTAS,
 * and therefore should not have the hint attached in the first place.
 *
 * <p>
 * Caveat: Does not currently handle sequences properly.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public class UpdateToCtasAdaptor implements SchemaChangeToSchemaAdaptor {

  private static final Log log = LogFactory.getLog(UpdateToCtasAdaptor.class);

  private final String databaseType;

  @VisibleForTesting
  public UpdateToCtasAdaptor() {
    this.databaseType = "?";
  }

  public UpdateToCtasAdaptor(DatabaseType databaseType) {
    this.databaseType = databaseType.identifier();
  }


  @Override
  public List<SchemaChange> adapt(SchemaChange change, Schema schema) {
    // Can we convert this change?
    if (change instanceof ExecuteStatement) {
      ExecuteStatement executeStatement = (ExecuteStatement) change;
      Statement statement = executeStatement.getStatement();
      if (statement instanceof UpdateStatement) {
        UpdateStatement updateStatement = (UpdateStatement) statement;
        Optional<CtasDuringUpgrade> hint = getRelevantUseCtasDuringUpgrade(updateStatement);
        if (hint.isPresent()) {
          return convertUpdateToCtas(updateStatement, schema, hint.get());
        }
      }
    }
    // Fallback to the usual
    return SchemaChangeToSchemaAdaptor.super.adapt(change, schema);
  }


  private Optional<CtasDuringUpgrade> getRelevantUseCtasDuringUpgrade(UpdateStatement updateStatement) {
    return updateStatement.getHints().stream()
      .filter(hint -> hint instanceof CtasDuringUpgrade)
      .map(hint -> (CtasDuringUpgrade)hint)
      .filter(hint -> hint.getUseCtasDuringUpgrade(databaseType))
      .findAny();
  }


  private List<SchemaChange> convertUpdateToCtas(UpdateStatement updateStatement, Schema schema, CtasDuringUpgrade hint) {
    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas(" + hint + "): " + updateStatement);

    if (updateStatement.getWhereCriterion() != null) {
      throw new RuntimeException("Cannot translate an UPDATE to CTAS; WHERE clause found: " + updateStatement);
    }

    final TableReference tableRef = updateStatement.getTable();
    final Table table = schema.getTable(tableRef.getName());

    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: tableRef " + tableRef);
    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: table " + table);

    if (table == null) {
      throw new RuntimeException("Cannot translate an UPDATE to CTAS; table [" + tableRef.getName() + "] not found: " + updateStatement);
    }

    final Map<String, Column> tableColumns = table.columns().stream()
        .collect(toMap(Column::getUpperCaseName, c -> c));

    final Map<String, AliasedField> updatedFields = updateStatement.getFields().stream()
        .collect(toMap(f -> f.getAlias().toUpperCase(), f -> extractFieldFromUpdate(f, tableColumns)));

    final Iterable<AliasedFieldBuilder> selectFields = table.columns().stream()
        .map(c -> updatedFields.getOrDefault(c.getUpperCaseName(), field(c.getName())))
        .collect(toList());

    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: tableColumns " + tableColumns);
    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: updatedFields " + updatedFields);
    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: selectFields " + selectFields);

    final SelectStatement selectStatement =
        SelectStatement.select(selectFields)
          .from(table.getName())
          .build();

    final TableBuilder newTable = table(getTempTableName(table.getName()))
        .columns(table.columns());
    if (table.isTemporary()) newTable.temporary();

    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: selectStatement " + selectStatement);
    if (log.isDebugEnabled()) log.debug("convertUpdateToCtas: newTable " + newTable);

    final ImmutableList.Builder<SchemaChange> adaptedChanges = ImmutableList.builder();

    // --
    // 1. Create temporary table with the new structure and the new data
    adaptedChanges.add(
        new AddTableFrom(newTable, selectStatement));

    // --
    // 2. Remove the original table
    adaptedChanges.add(
        new RemoveTable(table));

    // --
    // 3. Rename the temporary table to replace the original table
    adaptedChanges.add(
        new RenameTable(newTable.getName(), table.getName()));

    // --
    // 4. Add back all the old indexes, which we lost along the way
    table.indexes().stream()
      .forEach(index ->
        adaptedChanges.add(
            new AddIndex(table.getName(), index)));

    return adaptedChanges.build();
  }

  private String getTempTableName(String tableName) {
    return "CTAS_" + adjustTempTableName(tableName);
  }

  private String adjustTempTableName(String name) {
    if (name.length() > 25) {
      return name.substring(0, 25);
    }
    return name;
  }

  private AliasedField extractFieldFromUpdate(AliasedField field, Map<String, Column> tableColumns) {
    String columnName = field.getAlias().toUpperCase();
    Column column = tableColumns.get(columnName);
    if (column == null) {
      throw new RuntimeException("Cannot translate an UPDATE to CTAS; Cannot find " + columnName + " amongst " + tableColumns);
    }
    return cast(field).asType(column.getType(), column.getWidth(), column.getScale());
  }
}
