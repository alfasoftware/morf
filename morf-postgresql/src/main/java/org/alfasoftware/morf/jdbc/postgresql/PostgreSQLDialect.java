package org.alfasoftware.morf.jdbc.postgresql;

import static com.google.common.base.Predicates.instanceOf;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.field;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.DeleteStatementBuilder;
import org.alfasoftware.morf.sql.DialectSpecificHint;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.OptimiseForRowCount;
import org.alfasoftware.morf.sql.ParallelQueryHint;
import org.alfasoftware.morf.sql.PostgreSQLCustomHint;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SelectStatementBuilder;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.UseIndex;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.BlobFieldLiteral;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.FunctionType;
import org.alfasoftware.morf.sql.element.PortableSqlFunction;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class PostgreSQLDialect extends SqlDialect {

  public PostgreSQLDialect(String schemaName) {
   super(schemaName);
  }


  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(PostgreSQL.IDENTIFIER);
  }


  @Override
  public String schemaNamePrefix() {
    String schemaName = getSchemaName();

    if (StringUtils.isEmpty(schemaName)) {
      return "";
    }

    return schemaName + ".";
  }


  private String schemaNamePrefix(TableReference tableRef) {
    if(tableRef.isTemporary()) {
      return "";
    }
    if (StringUtils.isEmpty(tableRef.getSchemaName())) {
      return schemaNamePrefix();
    } else {
      return tableRef.getSchemaName() + ".";
    }
  }


  @Override
  protected String schemaNamePrefix(Table table) {
    if (table.isTemporary()) {
      return "";
    }
    return schemaNamePrefix();
  }


  @Override
  protected String schemaNamePrefix(Sequence sequence) {
    if (sequence.isTemporary()) {
      return "";
    }
    return schemaNamePrefix();
  }



  @Override
  protected String getSqlForRowNumber(){
    return "ROW_NUMBER() OVER()";
  }


  @Override
  protected String getSqlForWindowFunction(Function function) {
    FunctionType functionType = function.getType();
    switch (functionType) {
      case ROW_NUMBER:
        return "ROW_NUMBER()";

      default:
        return super.getSqlForWindowFunction(function);
    }
  }


  @Override
  protected String getDataTypeRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        return String.format("VARCHAR(%d)", width);

      case DECIMAL:
        return String.format("DECIMAL(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        return "BOOLEAN";

      case BIG_INTEGER:
        return "NUMERIC(19)";

      case INTEGER:
        return "INTEGER";

      case BLOB:
        return "BYTEA";

      case CLOB:
        return "TEXT";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    if (dataType == DataType.STRING) {
      return getDataTypeRepresentation(dataType, width, scale) + " COLLATE \"POSIX\"";
    }

    return getDataTypeRepresentation(dataType, width, scale);
  }


  @Override
  protected String getSqlFrom(Cast cast) {
    if (cast.getDataType() == DataType.STRING) {
      return super.getSqlFrom(cast) + " COLLATE \"POSIX\"";
    }

    return super.getSqlFrom(cast);
  }


  @Override
  protected Collection<String> internalTableDeploymentStatements(Table table) {
    return ImmutableList.<String>builder()
        .addAll(createTableStatement(table))
        .addAll(createCommentStatements(table))
        .build();
  }


  /**
   * @see SqlDialect#internalSequenceDeploymentStatements(Sequence)
   */
  @Override
  protected Collection<String> internalSequenceDeploymentStatements(Sequence sequence) {
    return ImmutableList.<String>builder()
        .add(createSequenceStatement(sequence))
        .build();
  }


  @Override
  public Collection<String> addTableFromStatementsWithCasting(Table table, SelectStatement selectStatement) {
    return internalAddTableFromStatements(table, selectStatement, true);
  }


  private Collection<String> internalAddTableFromStatements(Table table, SelectStatement selectStatement, boolean withCasting) {
    return ImmutableList.<String>builder()
            .addAll(createTableStatement(table, selectStatement, withCasting))
            .addAll(createFieldStatements(table))
            .addAll(createCommentStatements(table))
            .addAll(createAllIndexStatements(table))
            .build();
  }


  private List<String> createTableStatement(Table table) {
    List<String> preStatements = new ArrayList<>();
    List<String> postStatements = new ArrayList<>();

    StringBuilder createTableStatement = new StringBuilder();
    beginTableStatement(table, createTableStatement);

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;

    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }

      createTableStatement.append(column.getName());
      createTableStatement.append(" ").append(sqlRepresentationOfColumnType(column));
      handleAutoNumberedColumn(table, preStatements, postStatements, createTableStatement, column, false);

      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      first = false;
    }

    if (!primaryKeys.isEmpty()) {
      createTableStatement
              .append(", CONSTRAINT ")
              .append(table.getName())
              .append("_PK PRIMARY KEY(")
              .append(Joiner.on(", ").join(primaryKeys))
              .append(")");
    }

    createTableStatement.append(")");

    ImmutableList.Builder<String> statements = ImmutableList.<String>builder()
            .addAll(preStatements)
            .add(createTableStatement.toString());

    statements.addAll(postStatements);

    return statements.build();
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

    if (sequence.isTemporary()) {
      createSequenceStatement.append("TEMPORARY ");
    }

    createSequenceStatement.append("SEQUENCE ")
        .append(schemaNamePrefix(sequence))
        .append(sequence.getName());

    if (sequence.getStartsWith() != null) {
      createSequenceStatement.append(" START WITH ");
      createSequenceStatement.append(sequence.getStartsWith());
    }

    return createSequenceStatement.toString();

  }


  private List<String> createTableStatement(Table table, SelectStatement asSelect, boolean withCasting) {
    List<String> preStatements = new ArrayList<>();
    List<String> postStatements = new ArrayList<>();

    StringBuilder createTableStatement = new StringBuilder();
    beginTableStatement(table, createTableStatement);

    boolean first = true;

    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }

      createTableStatement.append(column.getName());
      createTableStatement.append(sqlRepresentationOfColumnType(column, false, false, false));
      handleAutoNumberedColumn(table, preStatements, postStatements, createTableStatement, column, true);

      first = false;
    }

    createTableStatement.append(")");

    String selectStatement = withCasting ? convertStatementToSQL(addCastsToSelect(table, asSelect)) : convertStatementToSQL(asSelect);
    createTableStatement.append(" AS ").append(selectStatement);

    ImmutableList.Builder<String> statements = ImmutableList.<String>builder()
            .addAll(preStatements)
            .add(createTableStatement.toString());

    statements.addAll(postStatements);

    return statements.build();
  }


  private void beginTableStatement(Table table, StringBuilder createTableStatement) {
    createTableStatement.append("CREATE ");

    if (table.isTemporary()) {
      createTableStatement.append("TEMP ");
    }

    createTableStatement.append("TABLE ")
            .append(schemaNamePrefix(table))
            .append(table.getName())
            .append(" (");
  }


  private void handleAutoNumberedColumn(Table table, List<String> preStatements, List<String> postStatements, StringBuilder createTableStatement, Column column, boolean asSelect) {
    if(column.isAutoNumbered()) {
      int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
      String autoNumberSequenceName = schemaNamePrefix() + table.getName() + "_" + column.getName() + "_seq";
      preStatements.add("DROP SEQUENCE IF EXISTS " + autoNumberSequenceName);
      preStatements.add("CREATE SEQUENCE " + autoNumberSequenceName + " START " + autoNumberStart);

      if (asSelect) {
        postStatements.add("ALTER TABLE " + table.getName() + " ALTER " + column.getName() + " SET DEFAULT nextval('" + autoNumberSequenceName + "')");
      } else {
        createTableStatement.append(" DEFAULT nextval('").append(autoNumberSequenceName).append("')");
      }

      postStatements.add("ALTER SEQUENCE " + autoNumberSequenceName + " OWNED BY " + schemaNamePrefix() + table.getName() + "." + column.getName());
    }
  }


  private List<String> createFieldStatements(Table table) {
    List<String> fieldStatements = new ArrayList<>();
    List<String> primaryKeys = new ArrayList<>();

    StringJoiner joiner = new StringJoiner(",", "ALTER TABLE " + table.getName(), "");

    for (Column column : table.columns()) {
      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      if (!column.isNullable()) {
        joiner.add(" ALTER " + column.getName() + " SET NOT NULL");
      }

      if (StringUtils.isNotEmpty(column.getDefaultValue()) && !column.isAutoNumbered()) {
        joiner.add(" ALTER " + column.getName() + " SET DEFAULT " + column.getDefaultValue());
      }
    }

    if (!primaryKeys.isEmpty()) {
      joiner.add(" ADD CONSTRAINT " + table.getName() + "_PK PRIMARY KEY(" + Joiner.on(", ").join(primaryKeys) + ")");
    }

    fieldStatements.add(joiner.toString());
    return fieldStatements;
  }


  private Collection<String> createCommentStatements(Table table) {
    List<String> commentStatements = Lists.newArrayList();

    commentStatements.add(addTableComment(table));
    for (Column column : table.columns()) {
      commentStatements.add(addColumnComment(table, column));
    }

    return commentStatements;
  }


  private String addTableComment(Table table) {
    return "COMMENT ON TABLE " + schemaNamePrefix(table) + table.getName() + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + table.getName() + "]'";
  }


  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {
    Iterable<String> renameTable = ImmutableList.of("ALTER TABLE " + schemaNamePrefix(from) + from.getName() + " RENAME TO " + to.getName());

    Iterable<String> renamePk = SchemaUtils.primaryKeysForTable(from).isEmpty()
        ? ImmutableList.of()
        : renameIndexStatements(from, from.getName() + "_pk", to.getName() + "_pk");

    Iterable<String> renameSeq = SchemaUtils.autoNumbersForTable(from).isEmpty()
        ? ImmutableList.of()
        : renameSequenceStatements(from.getName() + "_seq", to.getName() + "_seq");

    return ImmutableList.<String>builder()
        .addAll(renameTable)
        .addAll(renamePk)
        .addAll(renameSeq)
        .add(addTableComment(to))
        .build();
  }


  @Override
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.<String>builder()
        .addAll(super.renameIndexStatements(table, fromIndexName, toIndexName))
        .add(addIndexComment(toIndexName))
        .build();
  }


  private Collection<String> renameSequenceStatements(String fromSeqName, String toSeqName) {
    return ImmutableList.of(String.format("ALTER SEQUENCE %s RENAME TO %s", fromSeqName, toSeqName));
  }


  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> result = new ArrayList<>();

    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add(dropPrimaryKeyConstraint(table));
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(addPrimaryKeyConstraint(table));
    }

    return result;
  }


  @Override
  public Collection<String> viewDeploymentStatements(View view) {
    return ImmutableList.<String>builder()
        .addAll(super.viewDeploymentStatements(view))
        .add(addViewComment(view.getName()))
        .build();
  }


  private String addViewComment(String viewName) {
    return "COMMENT ON VIEW " + viewName + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + viewName + "]'";
  }


  @Override
  public String connectionTestStatement() {
    return "SELECT 1";
  }


  @Override
  public Collection<String> dropStatements(View view) {
    return ImmutableList.of("DROP VIEW IF EXISTS " + schemaNamePrefix() + view.getName() + " CASCADE");
  }


  @Override
  protected String getFromDummyTable() {
    return "";
  }


  @Override
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      sql.add(getSqlFrom(field));
    }
    return "CONCAT(" + StringUtils.join(sql, ", ") + ")";
  }

  @Override
  protected String getSqlFrom(BlobFieldLiteral field) {
    return String.format("E'\\x%s'", field.getValue());
  }

  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return "(" + getSqlFrom(toDate) + ") - (" + getSqlFrom(fromDate) + ")";
  }


  @Override
  protected String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate) {
    String toDateStr = getSqlFrom(toDate);
    String fromDateStr = getSqlFrom(fromDate);
    return "("
         + "(EXTRACT(YEAR FROM "+toDateStr+") - EXTRACT(YEAR FROM "+fromDateStr+")) * 12"
         + " + (EXTRACT(MONTH FROM "+toDateStr+") - EXTRACT(MONTH FROM "+fromDateStr+"))"
         + " + CASE WHEN "+toDateStr+" > "+fromDateStr
                + " THEN CASE WHEN EXTRACT(DAY FROM "+toDateStr+") >= EXTRACT(DAY FROM "+fromDateStr+") THEN 0"
                          + " WHEN EXTRACT(MONTH FROM "+toDateStr+") <> EXTRACT(MONTH FROM "+toDateStr+" + 1) THEN 0"
                          + " ELSE -1 END"
                + " ELSE CASE WHEN EXTRACT(MONTH FROM "+fromDateStr+") <> EXTRACT(MONTH FROM "+fromDateStr+" + 1) THEN 0"
                          + " WHEN EXTRACT(DAY FROM "+fromDateStr+") >= EXTRACT(DAY FROM "+toDateStr+") THEN 0"
                          + " ELSE 1 END"
           + " END"
         + ")";
  }


  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return String.format(
        "(DATE_TRUNC('MONTH', (%s)) + INTERVAL '1 MONTH' - INTERVAL '1 DAY') :: DATE",
        getSqlFrom(date));
  }


  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_CHAR("+ getSqlFrom(field) + ",'YYYYMMDD') :: NUMERIC";
  }


  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_CHAR("+ getSqlFrom(field) + ",'YYYYMMDDHH24MISS') :: NUMERIC";
  }


  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_DATE(("+ getSqlFrom(field) + ") :: TEXT,'YYYYMMDD')";
  }


  @Override
  protected String getSqlForNow(Function function) {
    return "NOW()";
  }


  @Override
  protected String getSqlForAddDays(Function function) {
    AliasedField date = function.getArguments().get(0);
    AliasedField days = function.getArguments().get(1);
    return String.format(
      "(((%s) + (%s) * INTERVAL '1 DAY') :: DATE)",
      getSqlFrom(date), getSqlFrom(days));
  }


  @Override
  protected String getSqlForAddMonths(Function function) {
    AliasedField date = function.getArguments().get(0);
    AliasedField months = function.getArguments().get(1);
    return String.format(
      "(((%s) + (%s) * INTERVAL '1 MONTH') :: DATE)",
      getSqlFrom(date), getSqlFrom(months));
  }


  @Override
  protected String getSqlForRandomString(Function function) {
    String lengthSql = getSqlFrom(function.getArguments().get(0));
    String randomString = "MD5(RANDOM() :: TEXT)";
    return "UPPER(SUBSTRING(" + randomString + ", 1, (" + lengthSql + ") :: INT))";
  }


  @Override
  protected String getSqlForRandom() {
    return "RANDOM()";
  }


  @Override
  protected String getSqlForRound(Function function) {
    return "ROUND((" + getSqlFrom(function.getArguments().get(0)) + ") :: NUMERIC, " + getSqlFrom(function.getArguments().get(1)) + ")";
  }


  @Override
  protected String getSqlFrom(MergeStatement statement) {
    if (StringUtils.isBlank(statement.getTable().getName())) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    checkSelectStatementHasNoHints(statement.getSelectStatement(), "MERGE may not be used with SELECT statement hints");

    Iterable<String> destinationFields = Iterables.transform(
        statement.getSelectStatement().getFields(),
        AliasedField::getImpliedName);

    Iterable<AliasedField> updateExpressions = getMergeStatementUpdateExpressions(statement);
    String updateExpressionsSql = getMergeStatementAssignmentsSql(updateExpressions);

    Iterable<String> keyFields = Iterables.transform(
        statement.getTableUniqueKey(),
        AliasedField::getImpliedName);

    StringBuilder sqlBuilder = new StringBuilder();

    sqlBuilder.append("INSERT INTO ")
              .append(tableNameWithSchemaName(statement.getTable()))
              .append(" (")
              .append(Joiner.on(", ").join(destinationFields))
              .append(") ")
              .append(getSqlFrom(statement.getSelectStatement()))
              .append(" ON CONFLICT (")
              .append(Joiner.on(",").join(keyFields))
              .append(")");

    if (getNonKeyFieldsFromMergeStatement(statement).iterator().hasNext()) {
      sqlBuilder.append(" DO UPDATE SET ")
                .append(updateExpressionsSql);
    } else {
      sqlBuilder.append(" DO NOTHING");
    }

    return sqlBuilder.toString();
  }


  /**
   * @see SqlDialect#getSqlFrom(SequenceReference)
   */
  @Override
  protected String getSqlFrom(SequenceReference sequenceReference) {
    StringBuilder result = new StringBuilder();

    switch (sequenceReference.getTypeOfOperation()) {
      case NEXT_VALUE:
        result.append("nextval('");
        break;
      case CURRENT_VALUE:
        result.append("currval('");
        break;
    }

    result.append(sequenceReference.getName());

    result.append("')");

    return result.toString();

  }

  @Override
  protected String getSqlFrom(MergeStatement.InputField field) {
    return "EXCLUDED." + field.getName();
  }


  @Override
  protected String getSqlFrom(SelectFirstStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT ");
    // Start by adding the field
    result.append(getSqlFrom(stmt.getFields().get(0)));

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);
    appendOrderBy(result, stmt);

    result.append(" LIMIT 1 OFFSET 0");

    return result.toString().trim();
  }


  @Override
  protected String selectStatementPreFieldDirectives(SelectStatement selectStatement) {
    StringBuilder builder = new StringBuilder();

    for (Hint hint : selectStatement.getHints()) {
      if (hint instanceof OptimiseForRowCount) {
        // not available in pg_hint_plan
      }
      else if (hint instanceof UseIndex) {
        UseIndex useIndex = (UseIndex)hint;
        builder.append(" IndexScan(")
          .append(StringUtils.isEmpty(useIndex.getTable().getAlias()) ? useIndex.getTable().getName() : useIndex.getTable().getAlias())
          .append(" ")
          .append(useIndex.getIndexName().toLowerCase())
          .append(")");
      }
      else if (hint instanceof UseImplicitJoinOrder) {
        // not available in pg_hint_plan
        // actually, there is Leading hint, which we could abuse
      }
      else if (hint instanceof ParallelQueryHint) {
        // not available in pg_hint_plan
      }
      else if (hint instanceof PostgreSQLCustomHint) {
        builder.append(" ")
        .append(((PostgreSQLCustomHint)hint).getCustomHint());
      }
      else if ( hint instanceof DialectSpecificHint && ((DialectSpecificHint)hint).isSameDatabaseType(PostgreSQL.IDENTIFIER) ) {
        builder.append(" ")
        .append(((DialectSpecificHint)hint).getHintContents());
      }
    }

    if (builder.length() == 0) {
      return super.selectStatementPreFieldDirectives(selectStatement);
    }

    return "/*+" + builder.append(" */ ");
  }


  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    return ImmutableList.of("ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " ADD COLUMN " + column.getName() + " " + sqlRepresentationOfColumnType(column, true),
        addColumnComment(table, column));
  }


  @Override
  public Collection<String> getSqlForAnalyseTable(Table table) {
    return ImmutableList.of("ANALYZE " + schemaNamePrefix(table) + table.getName());
  }


  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> statements = new ArrayList<>();

    Table oldTable = oldTableForChangeColumn(table, oldColumn, newColumn);

    boolean recreatePrimaryKey = oldColumn.isPrimaryKey() || newColumn.isPrimaryKey();

    if (recreatePrimaryKey && !primaryKeysForTable(oldTable).isEmpty()) {
      statements.add(dropPrimaryKeyConstraint(table));
    }

    if (oldColumn.isAutoNumbered() && !newColumn.isAutoNumbered()) {
      String autoNumberSequenceName = schemaNamePrefix() + table.getName() + "_" + oldColumn.getName() + "_seq";
      statements.add("DROP SEQUENCE IF EXISTS " + autoNumberSequenceName + " CASCADE");
    }

    if(!oldColumn.getName().equalsIgnoreCase(newColumn.getName())) {
      statements.add("ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " RENAME " + oldColumn.getName() + " TO " + newColumn.getName());
    }

    boolean alterNullable = oldColumn.isNullable() != newColumn.isNullable();
    boolean alterType = oldColumn.getType() != newColumn.getType() || oldColumn.getScale() != newColumn.getScale() || oldColumn.getWidth() != newColumn.getWidth();
    boolean alterDefaultValue = !Objects.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue());

    if(alterNullable || alterType || alterDefaultValue) {
      statements.add(addAlterTableConstraint(table, newColumn, alterNullable, alterType, alterDefaultValue));
    }

    if (recreatePrimaryKey && !primaryKeysForTable(table).isEmpty()) {
      statements.add(addPrimaryKeyConstraint(table));
    }

    statements.add(addColumnComment(table, newColumn));

    return statements;
  }


  private String addAlterTableConstraint(Table table, Column newColumn, boolean alterNullable, boolean alterType,
      boolean alterDefaultValue) {

    return "ALTER TABLE " + schemaNamePrefix(table) + table.getName()
            + (alterNullable ? " ALTER COLUMN " + newColumn.getName() + (newColumn.isNullable() ? " DROP NOT NULL" : " SET NOT NULL") : "")
            + (alterNullable && alterType ? "," : "")
            + (alterType ? " ALTER COLUMN " + newColumn.getName() + " TYPE " + sqlRepresentationOfColumnType(newColumn, false, false, true) : "")
            + (alterDefaultValue && (alterNullable || alterType) ? "," : "")
            + (alterDefaultValue ? " ALTER COLUMN " + newColumn.getName() + (!newColumn.getDefaultValue().isEmpty() ? " SET DEFAULT " + sqlForDefaultClauseLiteral(newColumn) : " DROP DEFAULT") : "");
  }


  private String addColumnComment(Table table, Column column) {
    StringBuilder comment = new StringBuilder ("COMMENT ON COLUMN " + schemaNamePrefix(table) + table.getName() + "." + column.getName() + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + column.getName() + "]/TYPE:[" + column.getType().toString() + "]");
    if(column.isAutoNumbered()) {
      int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
      comment.append("/AUTONUMSTART:[").append(autoNumberStart).append("]");
    }
    comment.append("'");
    return comment.toString();
  }


  private String dropPrimaryKeyConstraint(Table table) {
    return "ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " DROP CONSTRAINT " + table.getName() + "_PK";
  }


  private String addPrimaryKeyConstraint(Table table) {
    return "ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " ADD CONSTRAINT " + table.getName() + "_PK PRIMARY KEY(" + Joiner.on(", ").join(namesOfColumns(SchemaUtils.primaryKeysForTable(table))) + ")";
  }


  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    return ImmutableList.of("ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " DROP COLUMN " + column.getName());
  }


  @Override
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ")
             .append(index.getName())
             .append(" ON ")
             .append(schemaNamePrefix(table))
             .append(table.getName())
             .append(" (")
             .append(Joiner.on(", ").join(index.columnNames()))
             .append(")");

    return ImmutableList.<String>builder()
      .add(statement.toString())
      .add(addIndexComment(index.getName()))
      .build();
  }


  private String addIndexComment(String indexName) {
    return "COMMENT ON INDEX " + indexName + " IS '"+REAL_NAME_COMMENT_LABEL+":[" + indexName + "]'";
  }


  @Override
  public void prepareStatementParameters(NamedParameterPreparedStatement statement, DataValueLookup values, SqlParameter parameter) throws SQLException {
    switch (parameter.getMetadata().getType()) {
      case BLOB:
        byte[] blobVal = values.getByteArray(parameter.getImpliedName());
        if (blobVal == null) {
          InputStream inputStream = new ByteArrayInputStream(new byte[]{});
          statement.setBinaryStream(parameter, inputStream);
        } else {
          InputStream inputStream = new ByteArrayInputStream(blobVal);
          statement.setBinaryStream(parameter, inputStream);
        }
        return;

      default:
        super.prepareStatementParameters(statement, values, parameter);
        return;
    }
  }


  @Override
  protected String getSqlFrom(Boolean literalValue) {
    return literalValue ? "TRUE" : "FALSE";
  }


  @Override
  protected String getSqlForSome(Function function) {
    return "BOOL_OR(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  @Override
  protected String getSqlForEvery(Function function) {
    return "BOOL_AND(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(DeleteStatement)
   */
  @Override
  protected String getSqlFrom(DeleteStatement statement) {
    if (statement.getLimit().isPresent()) {
      StringBuilder sqlBuilder = new StringBuilder();

      DeleteStatementBuilder deleteStatement = DeleteStatement.delete(statement.getTable());
      sqlBuilder.append(super.getSqlFrom(deleteStatement.build()));

      // Now add the limit clause, using the current table id.
      sqlBuilder.append(" WHERE ctid IN (");

      SelectStatementBuilder selectStatement = select().fields(field("ctid")).from(statement.getTable());
      if (statement.getWhereCriterion() != null) {
        selectStatement = selectStatement.where(statement.getWhereCriterion());
      }
      sqlBuilder.append(getSqlFrom(selectStatement.build()));

      // We have already checked statement.getLimit().isPresent() here, but Sonar gives a false postive on the .get() below
      sqlBuilder.append(" LIMIT ").append(statement.getLimit().get()).append(")"); //NOSONAR

      return sqlBuilder.toString();
    }
    return super.getSqlFrom(statement);
  }


  @Override
  protected String getSqlFrom(PortableSqlFunction function) {
    return super.getSqlForPortableFunction(function.getFunctionForDatabaseType(PostgreSQL.IDENTIFIER));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableNameWithSchemaName(org.alfasoftware.morf.sql.element.TableReference)
   */
  @Override
  protected String tableNameWithSchemaName(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getDblink())) {
      return schemaNamePrefix(tableRef) + tableRef.getName();
    } else {
      return tableRef.getDblink() + "." + tableRef.getName();
    }
  }

  //
  // ====== Auto-healing below ======
  //

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSchemaConsistencyStatements(org.alfasoftware.morf.metadata.SchemaResource)
   */
  @Override
  public List<String> getSchemaConsistencyStatements(SchemaResource schemaResource) {
    return getPostgreSQLMetaDataProvider(schemaResource)
            .map(this::getSchemaConsistencyStatements)
            .orElseGet(() -> super.getSchemaConsistencyStatements(schemaResource));
  }


  private List<String> getSchemaConsistencyStatements(PostgreSQLMetaDataProvider metaDataProvider) {
    return FluentIterable.from(metaDataProvider.tables())
            .transformAndConcat(table -> healTable(metaDataProvider, table))
            .toList(); // turn all the concatenated fluent iterables into a firm immutable list
  }


  private Iterable<String> healTable(PostgreSQLMetaDataProvider metaDataProvider, Table table) {
    Iterable<String> statements = healIndexes(metaDataProvider, table);

    if (statements.iterator().hasNext()) {
      List<String> intro = ImmutableList.of(convertCommentToSQL("Auto-Healing table: " + table.getName()));
      return Iterables.concat(intro, statements);
    }
    return ImmutableList.of();
  }


  private Iterable<String> healIndexes(PostgreSQLMetaDataProvider metaDataProvider, Table table) {
    // Postgres 15 can deal with duplicate NULLs in unique indexes on it's own
    if (Integer.parseInt(metaDataProvider.getDatabaseInformation().get(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION)) >= 15) {
      // TODO
      // See https://www.postgresql.org/docs/current/sql-createindex.html
      // Once we support Postgres 15, we should introduce CREATE INDEX ... NULLS NOT DISTINCT
      return ImmutableList.of();
    }

    return ImmutableList.of();
  }


  private Optional<PostgreSQLMetaDataProvider> getPostgreSQLMetaDataProvider(SchemaResource schemaResource) {
    return schemaResource.getAdditionalMetadata()
            .filter(instanceOf(PostgreSQLMetaDataProvider.class))
            .map(PostgreSQLMetaDataProvider.class::cast);
  }
}