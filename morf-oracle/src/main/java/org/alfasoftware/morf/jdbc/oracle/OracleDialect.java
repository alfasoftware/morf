/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.jdbc.oracle;

import static com.google.common.base.Predicates.instanceOf;
import static org.alfasoftware.morf.metadata.DataType.DECIMAL;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.element.Direction.ASCENDING;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DialectSpecificHint;
import org.alfasoftware.morf.sql.DirectPathQueryHint;
import org.alfasoftware.morf.sql.ExceptSetOperator;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.NoDirectPathQueryHint;
import org.alfasoftware.morf.sql.OptimiseForRowCount;
import org.alfasoftware.morf.sql.OracleCustomHint;
import org.alfasoftware.morf.sql.ParallelQueryHint;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SqlUtils;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.UseIndex;
import org.alfasoftware.morf.sql.UseParallelDml;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AllowParallelDmlHint;
import org.alfasoftware.morf.sql.element.BlobFieldLiteral;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ClobFieldLiteral;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.PortableSqlFunction;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements Oracle specific statement generation.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class OracleDialect extends SqlDialect {

  private static final Log log = LogFactory.getLog(OracleDialect.class);

  /**
   * Database platforms may order nulls first or last. This null first.
   */
  public static final String NULLS_FIRST = "NULLS FIRST";

  /**
   * Database platforms may order nulls first or last. This is null last.
   */
  public static final String NULLS_LAST = "NULLS LAST";

  /**
   * Database platforms may order nulls first or last. My SQL always orders nulls first, Oracle defaults to ordering nulls last.
   * Fortunately on Oracle it is possible to specify that nulls should be ordered first.
   */
  public static final String DEFAULT_NULL_ORDER = NULLS_FIRST;

  public static final int MAX_LEGACY_NAME_LENGTH = 30;


  /**
   * Creates an instance of the Oracle dialect.
   *
   * @param schemaName The database schema name.
   */
  public OracleDialect(String schemaName) {
    super(schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#truncateTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> truncateTableStatements(Table table) {
    String mainTruncate = "TRUNCATE TABLE " + schemaNamePrefix() + table.getName();
    if (table.isTemporary()) {
      return Arrays.asList(mainTruncate);
    } else {
      return Arrays.asList(mainTruncate + " REUSE STORAGE");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> internalTableDeploymentStatements(Table table) {
    return tableDeploymentStatements(table, false);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> internalSequenceDeploymentStatements(Sequence sequence) {
    return ImmutableList.<String>builder()
      .add(createSequenceStatement(sequence))
      .build();
  }


  @Override
  public List<String> getSchemaConsistencyStatements(SchemaResource schemaResource) {
    return getOracleMetaDataProvider(schemaResource)
        .map(this::getSchemaConsistencyStatements)
        .orElseGet(() -> super.getSchemaConsistencyStatements(schemaResource));
  }


  private Optional<OracleMetaDataProvider> getOracleMetaDataProvider(SchemaResource schemaResource) {
    return schemaResource.getAdditionalMetadata()
        .filter(instanceOf(OracleMetaDataProvider.class))
        .map(OracleMetaDataProvider.class::cast);
  }


  private List<String> getSchemaConsistencyStatements(OracleMetaDataProvider metaDataProvider) {
    return FluentIterable.from(metaDataProvider.tables())
        .transformAndConcat(table -> healTable(table, metaDataProvider))
        .toList(); // turn all the concatenated fluent iterables into a firm immutable list
  }


  private Iterable<String> healTable(Table table, AdditionalMetadata metaDataProvider) {
    Map<String, String> primaryKeyIndexNames = metaDataProvider.primaryKeyIndexNames();
    Iterable<String> statements = healTruncatedIndexesSequencesAndTriggers(table, primaryKeyIndexNames);

    if (statements.iterator().hasNext()) {
      List<String> intro = ImmutableList.of(convertCommentToSQL("Auto-Healing table: " + table.getName()));
      return Iterables.concat(intro, statements);
    }
    return ImmutableList.of();
  }

  private Iterable<String> healTruncatedIndexesSequencesAndTriggers(Table table, Map<String, String> primaryKeyIndexNames) {
    List<String> statements = Lists.newArrayList();
    // Truncation of indexes will only have occurred for tables with names that are over 27 characters.
    if (table.getName().length() < 28) return statements;

    // Check if a truncated PK index exists.
    String truncatedIndexName = truncatedTableNameWithSuffixLegacy(table.getName(), "_PK");
    if (!truncatedIndexName.equalsIgnoreCase(primaryKeyIndexNames.get(table.getName().toUpperCase()))) return statements;

    // Triggers always get rebuilt during upgrade so drop all triggers with legacy names.
    String legacyTriggerName = schemaNamePrefix() + truncatedTableNameWithSuffixLegacy(table.getName(), "_TG").toUpperCase();

    statements.add(dropTrigger(legacyTriggerName));

    // Similarly, sequences get rebuilt so drop legacy versions of these too.
    if (getAutoIncrementColumnForTable(table) != null) {
      String legacySequenceName = schemaNamePrefix() + truncatedTableNameWithSuffixLegacy(table.getName(), "_SQ").toUpperCase();
      statements.add(dropSequence(legacySequenceName));
    }

    // Finally, rename the legacy PK
    if (!primaryKeysForTable(table).isEmpty()) {
      String legacyConstraintName = truncatedTableNameWithSuffixLegacy(table.getName(), "_PK").toUpperCase();
      String constraintName = primaryKeyConstraintName(table.getName());
      statements.add(renameConstraintIfExists(table.getName(), legacyConstraintName, constraintName));
      statements.add(renameIndexIfExists(legacyConstraintName, constraintName));
    }

    return statements;
  }


  private String renameIndexIfExists(String oldIndexName, String newIndexName) {
    String qualifiedIndexName = schemaNamePrefix() + oldIndexName.toUpperCase();
    return new StringBuilder()
        .append("DECLARE \n")
        .append("  e exception; \n")
        .append("  pragma exception_init(e,-1418); \n")
        .append("BEGIN \n")
        .append("  EXECUTE IMMEDIATE 'ALTER INDEX ").append(qualifiedIndexName).append(" RENAME TO ").append(newIndexName).append("'; \n")
        .append("EXCEPTION \n")
        .append("  WHEN e THEN \n")
        .append("    null; \n")
        .append("END;")
        .toString();
  }


  private String renameConstraintIfExists(String tableName, String oldConstraintName, String newConstraintName) {
    String qualifiedTableName = schemaNamePrefix() + tableName.toUpperCase();
    return new StringBuilder()
        .append("DECLARE \n")
        .append("  e exception; \n")
        .append("  pragma exception_init(e,-2448); \n")
        .append("BEGIN \n")
        .append("  EXECUTE IMMEDIATE 'ALTER TABLE ").append(qualifiedTableName).append(" RENAME CONSTRAINT ").append(oldConstraintName).append(" TO ").append(newConstraintName).append("'; \n")
        .append("EXCEPTION \n")
        .append("  WHEN e THEN \n")
        .append("    null; \n")
        .append("END;")
        .toString();
  }


  private Collection<String> tableDeploymentStatements(Table table, boolean asSelect) {
    ImmutableList.Builder<String> builder =   ImmutableList.<String>builder()
            .add(createTableStatement(table, asSelect));
    if (!primaryKeysForTable(table).isEmpty() && !table.isTemporary()) {
      builder.add(disableParallelAndEnableLoggingForPrimaryKey(table));
    }
    builder.addAll(buildRemainingStatementsAndComments(table));
    return builder.build();
  }


  /**
   * Private method to form the SQL statement required to create a sequence in the schema.
   *
   * @param sequence The {@link Sequence} for which a create sequence SQL statement should be created.
   * @return A create sequence SQL statement
   */
  private String createSequenceStatement(Sequence sequence) {
    StringBuilder createSequenceStatement = new StringBuilder();

    createSequenceStatement.append("CREATE ");

    createSequenceStatement.append("SEQUENCE ");

    String sequenceName = sequence.getName();

    createSequenceStatement.append(schemaNamePrefix());
    createSequenceStatement.append(sequenceName);

    createSequenceStatement.append(" CACHE 100000");

    if (sequence.isTemporary()) {
      createSequenceStatement.append(" SESSION");
    }

    if (sequence.getStartsWith() != null) {
      createSequenceStatement.append(" START WITH ");
      createSequenceStatement.append(sequence.getStartsWith());
    }

    return createSequenceStatement.toString();
  }


  private String createTableStatement(Table table, boolean asSelect) {
    // Create the table deployment statement
    StringBuilder createTableStatement = new StringBuilder();
    createTableStatement.append("CREATE ");

    if (table.isTemporary()) {
      createTableStatement.append("GLOBAL TEMPORARY ");
    }

    createTableStatement.append("TABLE ");

    String tableName = table.getName();

    createTableStatement.append(schemaNamePrefix());
    createTableStatement.append(tableName);
    createTableStatement.append(" (");

    boolean first = true;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }

      createTableStatement.append(column.getName());
      if (asSelect) {
        createTableStatement.append(" ").append(sqlRepresentationOfColumnType(column, true, true, false));
      } else {
        createTableStatement.append(" ").append(sqlRepresentationOfColumnType(column));
      }

      first = false;
    }

    // Put on the primary key constraint
    if (!primaryKeysForTable(table).isEmpty() && !asSelect) {
      createTableStatement.append(", ");
      createTableStatement.append(primaryKeyConstraint(table));
    }

    createTableStatement.append(")");

    if (table.isTemporary()) {
      createTableStatement.append(" ON COMMIT PRESERVE ROWS");
    }

    if (asSelect) {
      createTableStatement.append(" PARALLEL NOLOGGING");
    }

    return createTableStatement.toString();
  }

  private String addTableAlterForPrimaryKeyStatement(Table table) {
    StringBuilder updateTableStatement =new StringBuilder();
    updateTableStatement.append("ALTER TABLE " + schemaNamePrefix() + table.getName()  + " ADD " + primaryKeyConstraint(table));
    return updateTableStatement.toString();
  }

  private Collection<String> createColumnComments(Table table) {

    List<String> columnComments = Lists.newArrayList();

    for (Column column : table.columns()) {
      columnComments.add(columnComment(column, table.getName()));
    }

    return columnComments;
  }


  private Column findAutonumberedColumn(Table table) {
    Column sequence = null;
    for (Column column : table.columns()) {

      if (column.isAutoNumbered()) {
        sequence = column;
        break;
      }
    }

    return sequence;
  }

  /**
   * Adds the table name into comments.
   */
  private String commentOnTable(String truncatedTableName) {
    return "COMMENT ON TABLE " + schemaNamePrefix() + truncatedTableName + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + truncatedTableName + "]'";
  }

  private String disableParallelAndEnableLoggingForPrimaryKey(Table table) {
    return "ALTER INDEX " + schemaNamePrefix() + primaryKeyConstraintName(table.getName()) + " NOPARALLEL LOGGING";
  }

  /**
   * CONSTRAINT DEF_PK PRIMARY KEY (X, Y, Z) USING INDEX (CREATE UNIQUE INDEX ... NOLOGGING PARALLEL)
   */
  private String primaryKeyConstraint(String tableName, List<String> newPrimaryKeyColumns, boolean isTempTable) {
    // truncate down to 27, since we add _PK to the end
    String constraintClause = "CONSTRAINT " + primaryKeyConstraintName(tableName)
            + " PRIMARY KEY (" + Joiner.on(", ").join(newPrimaryKeyColumns) + ")"
            + " USING INDEX (CREATE UNIQUE INDEX " + schemaNamePrefix() + primaryKeyConstraintName(tableName)
            + " ON "
            + schemaNamePrefix() + tableName
            + " (" + Joiner.on(", ").join(newPrimaryKeyColumns) + ")";
    if (!isTempTable) {
      constraintClause += " NOLOGGING PARALLEL)";
    } else {
      constraintClause += ")";
    }
    return constraintClause;
  }

  /**
   * CONSTRAINT DEF_PK PRIMARY KEY (X, Y, Z)
   */
  private String primaryKeyConstraint(Table table) {
    return primaryKeyConstraint(table.getName(), namesOfColumns(primaryKeysForTable(table)), table.isTemporary());
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    for (Column column : table.columns()) {
      if (column.isAutoNumbered()) {
        return Arrays.asList(
          dropTrigger(table),
          "DROP TABLE " + schemaNamePrefix() + table.getName(),
          dropSequence(table));
      }
    }
    return Arrays.asList("DROP TABLE " + schemaNamePrefix() + table.getName());
  }

  @Override
  public Collection<String> dropTables(List<Table> tables, boolean ifExists, boolean cascade) {
    StringBuilder sb = new StringBuilder();
    if (tables.size() == 1 && !ifExists) {
      sb.append("DROP TABLE ").append(schemaNamePrefix()).append(tables.get(0).getName());
      if (cascade) {
        sb.append(" CASCADE CONSTRAINTS");
      }
    } else {
      String tablesString = tables.stream().map(s -> "'" + s.getName().toUpperCase() + "'").collect(Collectors.joining(", "));
      sb.append("BEGIN\n");
      sb.append("  FOR T IN (\n");
      sb.append("    SELECT '").append(schemaNamePrefix()).append("' || TABLE_NAME AS TABLE_NAME\n");
      if (ifExists) {
        sb.append("    FROM ALL_TABLES\n");
        sb.append("   WHERE TABLE_NAME  IN (").append(tablesString).append(")\n");
      } else {
        sb.append("    FROM (SELECT COLUMN_VALUE AS TABLE_NAME from TABLE(SYS.dbms_debug_vc2coll(").append(tablesString).append(")))\n");
      }
      sb.append("  )\n");
      sb.append("  LOOP\n");
      sb.append("    EXECUTE IMMEDIATE 'DROP TABLE ' || T.TABLE_NAME ").append(cascade ? "|| ' CASCADE CONSTRAINTS ' " : "").append(";\n");
      sb.append("  END LOOP;\n");
      sb.append("END;");
    }
    return Arrays.asList(sb.toString());
  }

  /**
   * Returns a SQL statement to safely drop a sequence, if it exists.
   *
   * @param table Table for which the sequence should be dropped.
   * @return SQL string.
   */
  private String dropSequence(Table table) {
    String sequenceName = sequenceName(table.getName());
    return dropSequence(sequenceName);
  }

  private String dropSequence(String sequenceName) {
    return new StringBuilder("DECLARE \n")
        .append("  query CHAR(255); \n")
        .append("BEGIN \n")
        .append("  select queryField into query from SYS.DUAL D left outer join (\n")
        .append("    select concat('drop sequence ").append(schemaNamePrefix()).append("', sequence_name) as queryField \n")
        .append("    from ALL_SEQUENCES S \n")
        .append("    where S.sequence_owner='").append(getSchemaName().toUpperCase()).append("' AND S.sequence_name = '").append(sequenceName.toUpperCase()).append("' \n")
        .append("  ) on 1 = 1; \n")
        .append("  IF query is not null THEN \n")
        .append("    execute immediate query; \n")
        .append("  END IF; \n")
        .append("END;")
        .toString();
  }


  /**
   * Returns a SQL statement to create a sequence for a table's autonumber column
   *
   * @param table Table for which the sequence should be created.
   * @param onColumn The autonumber column.
   * @return SQL string.
   */
  private String createNewSequence(Table table, Column onColumn) {
    int autoNumberStart = onColumn.getAutoNumberStart() == -1 ? 1 : onColumn.getAutoNumberStart();
    return new StringBuilder("CREATE SEQUENCE ")
      .append(schemaNamePrefix())
      .append(sequenceName(table.getName()))
      .append(" START WITH ")
      .append(autoNumberStart)
      .append(" CACHE 2000")
      .toString();
  }


  /**
   * Returns a SQL statement to create a sequence for a table's autonumber column, where
   * the sequence should start from the greater of either the autonumber column's start value
   * or the maximum value for that column existing in the table.
   *
   * @param table Table for which the sequence should be created.
   * @param onColumn The autonumber column.
   * @return  SQL string.
   */
  private String createSequenceStartingFromExistingData(Table table, Column onColumn) {
    String tableName = schemaNamePrefix() + table.getName();
    String sequenceName = schemaNamePrefix() + sequenceName(table.getName());
    return new StringBuilder("DECLARE query CHAR(255); \n")
      .append("BEGIN \n")
      .append("  SELECT 'CREATE SEQUENCE ").append(sequenceName).append(" START WITH ' || TO_CHAR(GREATEST(").append(onColumn.getAutoNumberStart()).append(", MAX(id)+1)) || ' CACHE 2000' INTO QUERY FROM \n")
      .append("    (SELECT MAX(").append(onColumn.getName()).append(") AS id FROM ").append(tableName).append(" UNION SELECT 0 AS id FROM SYS.DUAL); \n")
      .append("  EXECUTE IMMEDIATE query; \n")
      .append("END;")
      .toString();
  }


  /**
   * Returns a list of SQL statement to create a trigger to populate a table's autonumber column
   * from a sequence.
   *
   * @param table Table for which the trigger should be created.
   * @param onColumn The autonumber column.
   * @return SQL string list.
   */
  private List<String> createTrigger(Table table, Column onColumn) {
    List<String> createTriggerStatements = new ArrayList<>();
    createTriggerStatements.add(String.format("ALTER SESSION SET CURRENT_SCHEMA = %s", getSchemaName()));
    String tableName = table.getName();
    String sequenceName = sequenceName(table.getName());
    String triggerName = schemaNamePrefix() + triggerName(table.getName());
    createTriggerStatements.add(new StringBuilder("CREATE TRIGGER ").append(triggerName).append(" \n")
      .append("BEFORE INSERT ON ").append(tableName).append(" FOR EACH ROW \n")
      .append("BEGIN \n")
      .append("  IF (:new.").append(onColumn.getName()).append(" IS NULL) THEN \n")
      .append("    SELECT ").append(sequenceName).append(".nextval \n")
      .append("    INTO :new.").append(onColumn.getName()).append(" \n")
      .append("    FROM DUAL; \n")
      .append("  END IF; \n")
      .append("END;")
      .toString());
    return createTriggerStatements;
  }


  /**
   * Create statements to safely drop a trigger, if it exists.
   *
   * @param table Table for which the trigger should be dropped.
   * @return Query string.
   */
  private String dropTrigger(Table table) {
    String triggerName =  schemaNamePrefix() + triggerName(table.getName());
    return dropTrigger(triggerName);
  }

  private static String dropTrigger(String triggerName) {
    return new StringBuilder()
        .append("DECLARE \n")
        .append("  e exception; \n")
        .append("  pragma exception_init(e,-4080); \n")
        .append("BEGIN \n")
        .append("  EXECUTE IMMEDIATE 'DROP TRIGGER ").append(triggerName).append("'; \n")
        .append("EXCEPTION \n")
        .append("  WHEN e THEN \n")
        .append("    null; \n")
        .append("END;")
        .toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.View)
   */
  @Override
  public Collection<String> dropStatements(View view) {
    return Arrays.asList("BEGIN FOR i IN (SELECT null FROM all_views WHERE OWNER='" + getSchemaName().toUpperCase() + "' AND VIEW_NAME='" + view.getName().toUpperCase() + "') LOOP EXECUTE IMMEDIATE 'DROP VIEW " + schemaNamePrefix() + view.getName() + "'; END LOOP; END;");
  }


  @Override
  public Collection<String> dropStatements(Sequence sequence) {
    return ImmutableList.of("DROP SEQUENCE " + schemaNamePrefix() + sequence.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.ClobFieldLiteral)
   */
  @Override
  protected String getSqlFrom(ClobFieldLiteral field) {
    final int chunkSize = 512;
    if (field.getValue().length() <= chunkSize) {
      return super.getSqlFrom(field);
    }
    Iterable<String> scriptChunks = Splitter.fixedLength(chunkSize).split(field.getValue());
    FluentIterable<Cast> concatLiterals = FluentIterable.from(scriptChunks)
            .transform(chunk -> SqlUtils.cast(SqlUtils.clobLiteral(chunk)).asType(DataType.CLOB));

    return getSqlFrom(SqlUtils.concat(concatLiterals));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.Cast)
   */
  @Override
  protected String getSqlFrom(Cast cast) {
    // CAST does not directly support any of the LOB datatypes
    if (DataType.CLOB.equals(cast.getDataType())) {
      return String.format("TO_CLOB(%s)", getSqlFrom(cast.getExpression()));
    }
    if (DataType.BLOB.equals(cast.getDataType())) {
      return String.format("TO_BLOB(%s)", getSqlFrom(cast.getExpression()));
    }
    return super.getSqlFrom(cast);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#postInsertWithPresetAutonumStatements(org.alfasoftware.morf.metadata.Table, SqlScriptExecutor, Connection, boolean)
   */
  @Override
  public void postInsertWithPresetAutonumStatements(Table table, SqlScriptExecutor executor,Connection connection, boolean insertingUnderAutonumLimit) {
    // When we know we're definitely under the current next sequence value, there's no need to do anything.
    if (insertingUnderAutonumLimit) {
      return;
    }
    executor.execute(rebuildSequenceAndTrigger(table,getAutoIncrementColumnForTable(table)),connection);
  }


  /**
   * If the table has an auto-numbered column, rebuild its sequence and trigger.
   *
   * @param table The {@link Table}.
   * @return The SQL statements to run.
   */
  private Collection<String> rebuildSequenceAndTrigger(Table table,Column sequence) {
    // This requires drop/create trigger/sequence privileges so we avoid where we can.
    if(sequence == null) {
      return Lists.newArrayList(dropTrigger(table));
    }

    List<String> statements = new ArrayList<>();
    statements.add(dropTrigger(table));
    statements.add(dropSequence(table));
    statements.add(createSequenceStartingFromExistingData(table, sequence));
    statements.addAll(createTrigger(table, sequence));
    return statements;
  }


  /**
   * Form the standard name for a table's primary key constraint.
   *
   * @param tableName Name of the table for which the primary key constraint name is required.
   * @return Name of constraint.
   */
  private String primaryKeyConstraintName(String tableName) {
    return tableName + "_PK";
  }


  /**
   * Form the standard name for a table's autonumber sequence.
   *
   * @param tableName Name of the table for which the sequence name is required.
   * @return Name of sequence.
   */
  private String sequenceName(String tableName) {
    return tableName.toUpperCase() + "_SQ";
  }


  /**
   * Form the standard name for a table's autonumber trigger.
   *
   * @param tableName Name of the table for which the trigger name is required.
   * @return Name of trigger.
   */
  private String triggerName(String tableName) {
    return tableName.toUpperCase() + "_TG";
  }


  /**
   * Truncate table names 3 shorter than the maximum name length supported by Oracle, then add a 3 character suffix.
   */
  private String truncatedTableNameWithSuffixLegacy(String tableName, String suffix) {
    return StringUtils.substring(tableName, 0, MAX_LEGACY_NAME_LENGTH-3) + StringUtils.substring(suffix, 0, 3);
  }


  /**
   * Turn a string value into an SQL string literal which has that value.
   * <p>
   * We use {@linkplain StringUtils#isEmpty(CharSequence)} because we want to
   * differentiate between a single space and an empty string.
   * </p>
   * <p>
   * This is necessary because char types cannot be null and must contain
   * a single space.
   * <p>
   *
   * @param literalValue the literal value of the string.
   * @return SQL String Literal
   */
  @Override
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }

    return String.format("N'%s'", super.escapeSql(literalValue));
  }

  /**
   * https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html
   *  - ANSI data type DECIMAL(p,s) is equivalent to NUMBER(p,s)
   *  - BIG_INTEGER is therefore comparable to DECIMAL
   */
  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        // the null suffix here is potentially controversial, since oracle does
        // not distinguish between null and blank.
        // obey the metadata for now, since this makes the process reversible.
        return String.format("NVARCHAR2(%d)", width);

      case DECIMAL:
        return String.format("DECIMAL(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        return "DECIMAL(1,0)";

      case INTEGER:
        return "INTEGER";

      case BIG_INTEGER:
        return "NUMBER(19)";

      case BLOB:
        return "BLOB";

      case CLOB:
        return  "NCLOB";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#prepareBooleanParameter(org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement, java.lang.Boolean, org.alfasoftware.morf.sql.element.SqlParameter)
   */
  @Override
  protected void prepareBooleanParameter(NamedParameterPreparedStatement statement, Boolean boolVal, SqlParameter parameter) throws SQLException {
    statement.setBigDecimal(
      parameter(parameter.getImpliedName()).type(DECIMAL).width(1),
      boolVal == null ? null : boolVal ? BigDecimal.ONE : BigDecimal.ZERO
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#connectionTestStatement()
   */
  @Override
  public String connectionTestStatement() {
    return "select 1 from dual";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getDatabaseType()
   */
  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(Oracle.IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(ConcatenatedField)
   */
  @Override
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      sql.add(getSqlFrom(field));
    }
    return StringUtils.join(sql, " || ");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForIsNull(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForIsNull(Function function) {
    return "nvl(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ") ";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#buildSQLToStartTracing(java.lang.String)
   */
  @Override
  public List<String> buildSQLToStartTracing(String identifier) {
    return Arrays.asList("ALTER SESSION SET tracefile_identifier = '" + identifier + "'","ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT FOREVER, LEVEL 8'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#buildSQLToStopTracing()
   */
  @Override
  public List<String> buildSQLToStopTracing() {
    return Arrays.asList("ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT OFF'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForOrderByFieldNullValueHandling(org.alfasoftware.morf.sql.element.FieldReference)
   */
  @Override
  protected String getSqlForOrderByFieldNullValueHandling(FieldReference orderByField) {
    if (orderByField.getNullValueHandling().isPresent()) {
      switch (orderByField.getNullValueHandling().get()) {
        case FIRST:
          return " " + NULLS_FIRST;
        case LAST:
          return " " + NULLS_LAST;
        case NONE:
        default:
          return "";
      }
    } else if (orderByField.getDirection() != null) {
      return ASCENDING.equals(orderByField.getDirection()) ? " " + NULLS_FIRST : " " + NULLS_LAST;
    } else {
      return " " + defaultNullOrder();
    }
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlDialect#defaultNullOrder()
   */
  @Override
  protected String defaultNullOrder() {
    return DEFAULT_NULL_ORDER;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#addIndexStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> addIndexStatements(Table table, Index index) {
    return ImmutableList.of(
      // when adding indexes to existing tables, use PARALLEL NOLOGGING to efficiently build the index
      Iterables.getOnlyElement(indexDeploymentStatements(table, index)) + " PARALLEL NOLOGGING",
      indexPostDeploymentStatements(index)
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder createIndexStatement = new StringBuilder();

    // Specify the preamble
    createIndexStatement.append("CREATE ");
    if (index.isUnique()) {
      createIndexStatement.append("UNIQUE ");
    }

    // Name the index
    createIndexStatement
      .append("INDEX ")
      .append(schemaNamePrefix())
      .append(index.getName())

      // Specify which table the index is over
      .append(" ON ")
      .append(schemaNamePrefix())
      .append(table.getName())

      // Specify the fields that are used in the index
      .append(" (")
      .append(Joiner.on(", ").join(index.columnNames()))
      .append(")");

    return Collections.singletonList(createIndexStatement.toString());
  }


  /**
   * Generate the SQL to alter the index back to NOPARALLEL LOGGING.
   *
   * @param index The index we want to alter.
   * @return The SQL to alter the index.
   */
  private String indexPostDeploymentStatements(Index index) {
    return new StringBuilder()
      .append("ALTER INDEX ")
      .append(schemaNamePrefix())
      .append(index.getName())
      .append(" NOPARALLEL LOGGING")
      .toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    String tableName = table.getName();

    result.add(String.format("ALTER TABLE %s%s ADD (%s %s)",
      schemaNamePrefix(),
      tableName,
      column.getName(),
      sqlRepresentationOfColumnType(column, true)
    ));

    result.add(columnComment(column, tableName));

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(Table, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> result = new ArrayList<>();
    String tableName = table.getName();
    // Drop existing primary key and make columns not null
    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add(dropPrimaryKeyConstraint(tableName));
      for (String columnName : oldPrimaryKeyColumns) {
        result.add(makeColumnNotNull(tableName, columnName));
      }
    }

    //Create new primary key constraint
    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(generatePrimaryKeyStatement(newPrimaryKeyColumns, table.getName(), table.isTemporary()));
    }

    return result;
  }


  /**
   * It returns the SQL statement to make the column not null. The function
   * catches the exception with SQL error code ORA-01442: column to be modified
   * to NOT NULL is already NOT NULL.
   *
   * <p>Example of the generated SQL statement:</p>
   * <pre>
   * DECLARE
   *   e EXCEPTION;
   *   pragma exception_init(e,-1442);
   * BEGIN
   *   EXECUTE immediate 'alter table sandbox.genericglposting modify (version not null)';
   * EXCEPTION
   * WHEN e THEN
   *   NULL;
   * END;
   * </pre>
   *
   * @param tableName Table name to be altered
   * @param columnName Column name to make it not null
   * @return The SQL statement to make the column not null
   */
  private String makeColumnNotNull(String tableName, String columnName) {
    StringBuilder statement = new StringBuilder();
    statement.append("DECLARE \n").append("  e EXCEPTION; \n").append("  pragma exception_init(e,-1442); \n").append("BEGIN \n")
        .append("  EXECUTE immediate 'ALTER TABLE ").append(schemaNamePrefix()).append(tableName).append(" MODIFY (")
        .append(columnName).append(" NOT NULL)'; \n").append("EXCEPTION \n").append("WHEN e THEN \n").append("  NULL; \n")
        .append("END;");
    if (log.isDebugEnabled()) {
      log.debug(statement.toString());
    }
    return statement.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();

    Table oldTable = oldTableForChangeColumn(table, oldColumn, newColumn);

    String tableName = oldTable.getName();

    boolean recreatePrimaryKey = oldColumn.isPrimaryKey() || newColumn.isPrimaryKey();
    boolean alterNullable = oldColumn.isNullable() != newColumn.isNullable();
    boolean alterType = oldColumn.getType() != newColumn.getType() || oldColumn.getScale() != newColumn.getScale() || oldColumn.getWidth() != newColumn.getWidth();
    boolean alterDefaultValue = !Objects.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue());

    if (recreatePrimaryKey && !primaryKeysForTable(oldTable).isEmpty()) {
      result.add(dropPrimaryKeyConstraint(tableName));
    }

    if (alterNullable || alterType || alterDefaultValue) {
      for (Index index : oldTable.indexes()) {
        for (String column : index.columnNames()) {
          if (column.equalsIgnoreCase(oldColumn.getName())) {
            result.addAll(indexDropStatements(oldTable, index));
          }
        }
      }
    }

    if (!newColumn.getName().equalsIgnoreCase(oldColumn.getName())) {
      result.add("ALTER TABLE " + schemaNamePrefix() + tableName + " RENAME COLUMN " + oldColumn.getName() + " TO " + newColumn.getName());
    }

    boolean includeNullability = newColumn.isNullable() != oldColumn.isNullable();
    boolean includeColumnType = newColumn.getType() != oldColumn.getType() || newColumn.getWidth() != oldColumn.getWidth() || newColumn.getScale() != oldColumn.getScale();
    String sqlRepresentationOfColumnType = sqlRepresentationOfColumnType(newColumn, includeNullability, true, includeColumnType);

    if (!StringUtils.isBlank(sqlRepresentationOfColumnType)) {
      StringBuilder statement = new StringBuilder()
              .append("ALTER TABLE ")
              .append(schemaNamePrefix())
              .append(tableName)
              .append(" MODIFY (")
              .append(newColumn.getName())
              .append(' ')
              .append(sqlRepresentationOfColumnType)
              .append(")");

      result.add(statement.toString());
    }

    if (!StringUtils.isBlank(oldColumn.getDefaultValue()) && StringUtils.isBlank(newColumn.getDefaultValue())) {
      StringBuilder statement = new StringBuilder()
              .append("ALTER TABLE ")
              .append(schemaNamePrefix())
              .append(tableName)
              .append(" MODIFY (")
              .append(newColumn.getName())
              .append(" DEFAULT NULL")
              .append(")");

      result.add(statement.toString());
    }

    if (recreatePrimaryKey && !primaryKeysForTable(table).isEmpty()) {
      result.add(generatePrimaryKeyStatement(namesOfColumns(SchemaUtils.primaryKeysForTable(table)), tableName, table.isTemporary()));
      if (!table.isTemporary()) {
        result.add(disableParallelAndEnableLoggingForPrimaryKey(table));
      }
    }

    if (alterNullable || alterType || alterDefaultValue) {
      for (Index index : table.indexes()) {
        for (String column : index.columnNames()) {
          if (column.equalsIgnoreCase(newColumn.getName())) {
            result.addAll(addIndexStatements(table, index));
          }
        }
      }
    }
    result.add(columnComment(newColumn, tableName));

    return result;
  }


  private String generatePrimaryKeyStatement(List<String> columnNames, String tableName, boolean isTempTable) {
    StringBuilder primaryKeyStatement = new StringBuilder();
    primaryKeyStatement.append("ALTER TABLE ")
    .append(schemaNamePrefix())
    .append(tableName)
    .append(" ADD ")
    .append(primaryKeyConstraint(tableName, columnNames, isTempTable));
    return primaryKeyStatement.toString();
  }


  /**
   * ALTER TABLE ABC.DEF DROP PRIMARY KEY DROP INDEX
   */
  private String dropPrimaryKeyConstraint(String tableName) {
    // Drop the associated unique index at the same time
    return "ALTER TABLE " + schemaNamePrefix() + tableName + " DROP PRIMARY KEY DROP INDEX";
  }


  /**
   * Build the comment comment that allows the metadata reader to determine the correct lower case table names and types.
   */
  private String columnComment(Column column, String tableName) {
    StringBuilder comment = new StringBuilder ("COMMENT ON COLUMN " + schemaNamePrefix() + tableName + "." + column.getName() + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + column.getName() + "]/TYPE:[" + column.getType().toString() + "]");

    if (column.isAutoNumbered()) {
      int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
      comment.append("/AUTONUMSTART:[").append(autoNumberStart).append("]");
    }

    comment.append("'");

    return comment.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    String tableName = table.getName();

    StringBuilder statement = new StringBuilder()
    .append("ALTER TABLE ")
    .append(schemaNamePrefix())
    .append(tableName)
    .append(" SET UNUSED") // perform a logical (rather than physical) delete of the row for performance reasons.
    .append(" (").append(column.getName()).append(")");

    result.add(statement.toString());

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    StringBuilder statement = new StringBuilder();

    statement.append("DROP INDEX ")
    .append(schemaNamePrefix())
    .append(indexToBeRemoved.getName());

    return Arrays.asList(statement.toString());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    return "TO_DATE(" + getSqlFrom(function.getArguments().get(0)) + ", 'yyyymmdd')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    return String.format("TO_NUMBER(TO_CHAR(%s, 'yyyymmdd'))",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    return String.format("TO_NUMBER(TO_CHAR(%s, 'yyyymmddHH24MISS'))",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "SYSTIMESTAMP AT TIME ZONE 'UTC'";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return String.format("(%s) - (%s)", getSqlFrom(toDate), getSqlFrom(fromDate));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForMonthsBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate) {
    String toDateStr = getSqlFrom(toDate);
    String fromDateStr = getSqlFrom(fromDate);
    return
       "(EXTRACT(YEAR FROM "+toDateStr+") - EXTRACT(YEAR FROM "+fromDateStr+")) * 12"
       + "+ (EXTRACT(MONTH FROM "+toDateStr+") - EXTRACT(MONTH FROM "+fromDateStr+"))"
       + "+ CASE WHEN "+toDateStr+" > "+fromDateStr
             + " THEN CASE WHEN EXTRACT(DAY FROM "+toDateStr+") >= EXTRACT(DAY FROM "+fromDateStr+") THEN 0"
                       + " WHEN EXTRACT(MONTH FROM "+toDateStr+") <> EXTRACT(MONTH FROM "+toDateStr+" + 1) THEN 0"
                       + " ELSE -1 END"
             + " ELSE CASE WHEN EXTRACT(MONTH FROM "+fromDateStr+") <> EXTRACT(MONTH FROM "+fromDateStr+" + 1) THEN 0"
                       + " WHEN EXTRACT(DAY FROM "+fromDateStr+") >= EXTRACT(DAY FROM "+toDateStr+") THEN 0"
                       + " ELSE 1 END"
             + " END"
       + "\n"
    ;
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSubstringFunctionName()
   */
  @Override
  protected String getSubstringFunctionName() {
    return "SUBSTR";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddDays(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddDays(Function function) {
    return String.format(
      "(%s) + (%s)",
      getSqlFrom(function.getArguments().get(0)),
      getSqlFrom(function.getArguments().get(1))
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddMonths(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddMonths(Function function) {
    return "ADD_MONTHS(" +
      getSqlFrom(function.getArguments().get(0)) + ", " +
      getSqlFrom(function.getArguments().get(1)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameTableStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> renameTableStatements(Table fromTable, Table toTable) {
    String from = fromTable.getName();
    String fromConstraint = primaryKeyConstraintName(fromTable.getName());

    String to = toTable.getName();
    String toConstraint = primaryKeyConstraintName(toTable.getName());

    ArrayList<String> statements = new ArrayList<>();

    if (!primaryKeysForTable(fromTable).isEmpty()) {
      // Rename the PK constraint
      statements.add("ALTER TABLE " + schemaNamePrefix() + from + " RENAME CONSTRAINT " + fromConstraint + " TO " + toConstraint);
      // Rename the index for the PK constraint the Oracle uses to manage the PK
      statements.add("ALTER INDEX " + schemaNamePrefix() + fromConstraint + " RENAME TO " + toConstraint);
    }

    // Rename the table itself
    statements.add("ALTER TABLE " + schemaNamePrefix() + from + " RENAME TO " + to);
    statements.add(commentOnTable(to));

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#addTableFromStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.sql.SelectStatement)
   */
  @Override
  public Collection<String> addTableFromStatements(Table table, SelectStatement selectStatement) {
    return internalAddTableFromStatements(table, selectStatement, true);
  }


  @Override
  public Collection<String> addTableFromStatementsWithCasting(Table table, SelectStatement selectStatement) {
    return internalAddTableFromStatements(table, selectStatement, true);
  }


  private Collection<String> internalAddTableFromStatements(Table table, SelectStatement selectStatement, boolean withCasting) {
    Builder<String> result = ImmutableList.<String>builder();
    result.add(new StringBuilder()
            .append(createTableStatement(table, true))
            .append(" AS ")
            .append(withCasting ? convertStatementToSQL(addCastsToSelect(table, selectStatement)) : convertStatementToSQL(selectStatement))
            .toString()
    );

    if (!primaryKeysForTable(table).isEmpty()) {
      result.add(addTableAlterForPrimaryKeyStatement(table));
    }

    result.add("ALTER TABLE " + schemaNamePrefix() + table.getName()  + " NOPARALLEL LOGGING");

    if (!primaryKeysForTable(table).isEmpty()) {
      result.add(disableParallelAndEnableLoggingForPrimaryKey(table));
    }

    result.addAll(buildRemainingStatementsAndComments(table));

    return result.build();
  }


  /**
   * Builds the remaining statements (triggers, sequences and comments).
   *
   * @param table The table to create the statements.
   * @return the collection of statements.
   */
  private Collection<String> buildRemainingStatementsAndComments(Table table) {

    List<String> statements = Lists.newArrayList();

    Column sequence = findAutonumberedColumn(table);
    if (sequence != null) {
      statements.add(dropTrigger(table));
      statements.add(dropSequence(table));
      statements.add(createNewSequence(table, sequence));
      statements.addAll(createTrigger(table, sequence));
    }

    String tableName = table.getName();
    statements.add(commentOnTable(tableName));

    statements.addAll(createColumnComments(table));

    return statements;
  }


  /**
   * SqlPlus requires sql statement lines to be less than 2500 characters in length.
   * Additionally PL\SQL statements must be ended with "/".
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#formatSqlStatement(java.lang.String)
   */
  @Override
  public String formatSqlStatement(String sqlStatement) {
    // format statement ending
    StringBuilder builder = new StringBuilder(sqlStatement);
    if (sqlStatement.endsWith("END;")) {
      builder.append(System.getProperty("line.separator"));
      builder.append("/");
    } else {
      builder.append(";");
    }

    return splitSqlStatement(builder.toString());
  }


  /**
   * If the SQL statement line is greater than 2499 characters then split
   * it into multiple lines where each line is less than 2500 characters in
   * length. The split is done on a space character; if a space character
   * cannot be found then a warning will be logged but the statement line
   * will still be returned exceeding 2499 characters in length.
   *
   * @param sqlStatement the statement to split
   * @return the correctly formatted statement
   */
  private String splitSqlStatement(String sqlStatement) {
    StringBuilder sql = new StringBuilder();
    if (sqlStatement.length() >= 2500) {
      int splitAt = sqlStatement.lastIndexOf(' ', 2498);
      if (splitAt == -1) {
        log.warn("SQL statement greater than 2499 characters in length but unable to find white space (\" \") to split on.");
        sql.append(sqlStatement);
      } else {
        sql.append(sqlStatement, 0, splitAt);
        sql.append(System.getProperty("line.separator"));
        sql.append(splitSqlStatement(sqlStatement.substring(splitAt + 1)));
      }
    } else {
      sql.append(sqlStatement);
    }

    return sql.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandom()
   */
  @Override
  protected String getSqlForRandom() {
    return "dbms_random.value";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return String.format("dbms_random.string('A', %s)",getSqlFrom(function.getArguments().get(0)));
  }


  @Override
  protected String getSqlforBlobLength(Function function) {
    return String.format("dbms_lob.getlength(%s)", getSqlFrom(function.getArguments().get(0)));
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.SelectFirstStatement)
   */
  @Override
  protected String getSqlFrom(SelectFirstStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT MIN(");

    // Start by adding the field
    result.append(getSqlFrom(stmt.getFields().get(0))).append(") KEEP (DENSE_RANK FIRST");

    appendOrderBy(result, stmt);
    result.append(")");

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);

    return result.toString().trim();
  }


  /**
   * @see SqlDialect#getSqlFrom(SequenceReference)
   */
  @Override
  protected String getSqlFrom(SequenceReference sequenceReference) {
    StringBuilder result = new StringBuilder();

    if (getSchemaName() != null || !getSchemaName().isBlank()) {
      result.append(getSchemaName());
      result.append(".");
    }

    result.append(sequenceReference.getName().toUpperCase());

    switch (sequenceReference.getTypeOfOperation()) {
      case NEXT_VALUE:
        result.append(".NEXTVAL");
        break;
      case CURRENT_VALUE:
        result.append(".CURRVAL");
        break;
    }

    return result.toString();
  }

  /**
   * @param field the BLOB field literal
   * @return The SQL got a BLOB field
   */
  @Override
  protected String getSqlFrom(BlobFieldLiteral field) {
    return String.format("HEXTORAW('%s')", field.getValue());
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#selectStatementPreFieldDirectives(org.alfasoftware.morf.sql.SelectStatement)
   */
  @Override
  protected String selectStatementPreFieldDirectives(SelectStatement selectStatement) {
    StringBuilder builder = new StringBuilder();

    for (Hint hint : selectStatement.getHints()) {
      if (hint instanceof OptimiseForRowCount) {
        builder.append(" FIRST_ROWS(")
          .append(((OptimiseForRowCount)hint).getRowCount())
          .append(")");
      }
      else if (hint instanceof UseIndex) {
        UseIndex useIndex = (UseIndex)hint;
        builder.append(" INDEX(")
          // No schema name - see http://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements006.htm#BABIEJEB
          .append(StringUtils.isEmpty(useIndex.getTable().getAlias()) ? useIndex.getTable().getName() : useIndex.getTable().getAlias())
          .append(" ")
          .append(useIndex.getIndexName())
          .append(")");
      }
      else if (hint instanceof UseImplicitJoinOrder) {
        builder.append(" ORDERED");
      }
      else if (hint instanceof ParallelQueryHint) {
        builder.append(" PARALLEL");
        ParallelQueryHint parallelQueryHint = (ParallelQueryHint) hint;
        builder.append(parallelQueryHint.getDegreeOfParallelism().map(d -> "(" + d + ")").orElse(""));
      }
      else if (hint instanceof AllowParallelDmlHint) {
        builder.append(" ENABLE_PARALLEL_DML");
      }
      else if (hint instanceof OracleCustomHint) {
        builder.append(" ")
        .append(((OracleCustomHint)hint).getCustomHint());
      }
      else if ( hint instanceof DialectSpecificHint && ((DialectSpecificHint)hint).isSameDatabaseType(Oracle.IDENTIFIER) ) {
        builder.append(" ")
        .append(((DialectSpecificHint)hint).getHintContents());
      }
    }

    if (builder.length() == 0) {
      return super.selectStatementPreFieldDirectives(selectStatement);
    }

    return "/*+" + builder.append(" */ ").toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#updateStatementPreTableDirectives(org.alfasoftware.morf.sql.UpdateStatement)
   */
  @Override
  protected String updateStatementPreTableDirectives(UpdateStatement updateStatement) {
    if(updateStatement.getHints().isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder("/*+");
    for (Hint hint : updateStatement.getHints()) {
      if (hint instanceof UseParallelDml) {
        builder.append(" ENABLE_PARALLEL_DML PARALLEL");
        builder.append(((UseParallelDml) hint).getDegreeOfParallelism()
                .map(d -> "(" + d + ")")
                .orElse(""));
      }
    }
    return builder.append(" */ ").toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#rebuildTriggers(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> rebuildTriggers(Table table) {
    return rebuildSequenceAndTrigger(table,getAutoIncrementColumnForTable(table));
  }


  /**
   * @deprecated this method returns the legacy fetch size value for Oracle and is primarily for backwards compatibility.
   * Please use {@link SqlDialect#fetchSizeForBulkSelects()} for the new recommended default value.
   * @see SqlDialect#fetchSizeForBulkSelects()
   */
  @Override
  @Deprecated
  public int legacyFetchSizeForBulkSelects() {
    return 200;
  }


  /**
   * We do use NVARCHAR for strings on Oracle.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#usesNVARCHARforStrings()
   */
  @Override
  public boolean usesNVARCHARforStrings() {
    return true;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "LAST_DAY(" + getSqlFrom(date) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAnalyseTable(Table)
   */
  @Override
  public Collection<String> getSqlForAnalyseTable(Table table) {
    return ImmutableList.of(
                     "BEGIN \n" +
                       "DBMS_STATS.GATHER_TABLE_STATS(ownname=> '" + getSchemaName() + "', "
                          + "tabname=>'" + table.getName() + "', "
                          + "cascade=>true, degree=>DBMS_STATS.AUTO_DEGREE, no_invalidate=>false); \n"
                   + "END;");
  }


  /**
   * @see SqlDialect#getDeleteLimitWhereClause(int)
   */
  @Override
  protected Optional<String> getDeleteLimitWhereClause(int limit) {
    return Optional.of("ROWNUM <= " + limit);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForInsertInto(org.alfasoftware.morf.sql.InsertStatement)
   */
  @Override
  protected String getSqlForInsertInto(InsertStatement insertStatement) {
    return "INSERT " + insertStatementPreIntoDirectives(insertStatement) + "INTO ";
  }


  private String insertStatementPreIntoDirectives(InsertStatement insertStatement) {

    if (insertStatement.getHints().isEmpty()) {
      return "";
    }

    StringBuilder builder = new StringBuilder().append("/*+");

    for (Hint hint : insertStatement.getHints()) {
      if (hint instanceof DirectPathQueryHint) {
        builder.append(" APPEND");
      }
      else if(hint instanceof NoDirectPathQueryHint) {
        builder.append(" NOAPPEND");
      }
      if(hint instanceof UseParallelDml) {
        builder.append(" ENABLE_PARALLEL_DML PARALLEL");
        builder.append(((UseParallelDml) hint).getDegreeOfParallelism()
                .map(d -> "(" + d + ")")
                .orElse(""));
      }
    }

    return builder.append(" */ ").toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableNameWithSchemaName(org.alfasoftware.morf.sql.element.TableReference)
   */
  @Override
  protected String tableNameWithSchemaName(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getDblink())) {
      return super.tableNameWithSchemaName(tableRef);
    } else {
      return super.tableNameWithSchemaName(tableRef) + "@" + tableRef.getDblink();
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.ExceptSetOperator)
   */
  @Override
  protected String getSqlFrom(ExceptSetOperator operator) {
    return String.format(" MINUS %s", // MINUS has been supported by Oracle for a long time and the EXCEPT support was added in 21c
        getSqlFrom(operator.getSelectStatement()));
  }


  @Override
  protected String getSqlFrom(PortableSqlFunction function) {
    return super.getSqlForPortableFunction(function.getFunctionForDatabaseType(Oracle.IDENTIFIER));
  }


  @Override
  public boolean useForcedSerialImport() {
    return false;
  }
}