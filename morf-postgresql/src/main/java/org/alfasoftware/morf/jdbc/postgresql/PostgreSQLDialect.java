package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class PostgreSQLDialect extends SqlDialect {

  public PostgreSQLDialect(String schemaName) {
   super(schemaName);
  }


  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(PostgreSQL.IDENTIFIER);
  }


  @Override
  public boolean supportsWindowFunctions() {
    return true;
  }


  @Override
  public String schemaNamePrefix() {
    String schemaName = getSchemaName();

    if (StringUtils.isEmpty(schemaName)) {
      return "";
    }

    return "\"" + schemaName.toUpperCase() + "\"" + ".";
  }


  @Override
  protected String schemaNamePrefix(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getSchemaName())) {
      return schemaNamePrefix();
    } else {
      return "\"" + tableRef.getSchemaName().toUpperCase() + "\"" + ".";
    }
  }


  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
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
  protected Collection<String> internalTableDeploymentStatements(Table table) {
    List<String> preStatements = new ArrayList<>();
    List<String> postStatements = new ArrayList<>();

    StringBuilder createTableStatement = new StringBuilder();

    createTableStatement.append("CREATE ");

    if(table.isTemporary()) {
      createTableStatement.append("TEMP TABLE ");
    }
    else {
      createTableStatement.append("TABLE ")
                          .append(schemaNamePrefix());
    }
    createTableStatement.append(table.getName())
                        .append(" (");

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;

    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }
      createTableStatement.append(column.getName())
                          .append(" ")
                          .append(sqlRepresentationOfColumnType(column));
      if(column.isAutoNumbered()) {
        int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        String autoNumberSequenceName = schemaNamePrefix() + table.getName() + "_" + column.getName() + "_seq";
        preStatements.add("DROP SEQUENCE IF EXISTS " + autoNumberSequenceName);
        preStatements.add("CREATE SEQUENCE " + autoNumberSequenceName + " START " + autoNumberStart);
        createTableStatement.append(" DEFAULT nextval('")
                            .append(autoNumberSequenceName)
                            .append("')");
        postStatements.add("ALTER SEQUENCE " + autoNumberSequenceName + " OWNED BY " + schemaNamePrefix() + table.getName() + "." + column.getName());
      }
      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      postStatements.add(addColumnComment(table, column));
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

    return ImmutableList.<String>builder()
        .addAll(preStatements)
        .add(createTableStatement.toString())
        .addAll(postStatements)
        .build();
  }


  @Override
  public Collection<String> truncateTableStatements(Table table) {
    List<String> statements = new ArrayList<>();

    StringBuilder truncateStatement = new StringBuilder();

    truncateStatement.append("TRUNCATE TABLE ");

    if(!table.isTemporary()){
      truncateStatement.append(schemaNamePrefix());
    }
    truncateStatement.append(table.getName());

    statements.add(truncateStatement.toString());
    return statements;
  }


  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {
    Iterable<String> renameTable = ImmutableList.of("ALTER TABLE " + schemaNamePrefix() + from.getName() + " RENAME TO " + to.getName());

    Iterable<String> renamePk = SchemaUtils.primaryKeysForTable(from).isEmpty()
        ? ImmutableList.of()
        : renameIndexStatements(null, from.getName() + "_pk", to.getName() + "_pk");

    Iterable<String> renameSeq = SchemaUtils.autoNumbersForTable(from).isEmpty()
        ? ImmutableList.of()
        : renameSequenceStatements(from.getName() + "_seq", to.getName() + "_seq");

    return ImmutableList.<String>builder()
        .addAll(renameTable)
        .addAll(renamePk)
        .addAll(renameSeq)
        .build();
  }


  @Override
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.of("ALTER INDEX " + schemaNamePrefix() + fromIndexName + " RENAME TO " + toIndexName);
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
  public Collection<String> deleteAllFromTableStatements(Table table) {
    return ImmutableList.of("DELETE FROM " + table.getName());
  }


  @Override
  public String connectionTestStatement() {
    return "SELECT 1";
  }


  @Override
  public Collection<String> dropStatements(Table table) {
    ImmutableList.Builder<String> statements = ImmutableList.builder();

    StringBuilder dropStatement = new StringBuilder();

    dropStatement.append("DROP TABLE ");

    if(!table.isTemporary()){
      dropStatement.append(schemaNamePrefix());
    }
    dropStatement.append(table.getName());

    statements.add(dropStatement.toString());

    return statements.build();
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
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return getSqlFrom(toDate) + " - " + getSqlFrom(fromDate);
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
  protected String getSqlForIsNull(Function function) {
     return "COALESCE(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ") ";
  }


  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_CHAR("+ getSqlFrom(field) + ",'YYYYMMDD')";
  }


  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_CHAR("+ getSqlFrom(field) + ",'YYYYMMDDHH24MISS')";
  }


  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "TO_DATE("+ getSqlFrom(field) + ",'YYYYMMDD')";
  }


  @Override
  protected String getSqlForNow(Function function) {
    return "NOW()";
  }


  @Override
  protected String leftTrim(Function function) {
    return "LTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  @Override
  protected String rightTrim(Function function) {
    return "RTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  @Override
  protected String getSqlForAddDays(Function function) {
    AliasedField date = function.getArguments().get(0);
    AliasedField days = function.getArguments().get(1);
    return String.format(
      "((%s) + (%s) * INTERVAL '1 DAY')",
      getSqlFrom(date), getSqlFrom(days));
  }


  @Override
  protected String getSqlForAddMonths(Function function) {
    AliasedField date = function.getArguments().get(0);
    AliasedField months = function.getArguments().get(1);
    return String.format(
      "((%s) + (%s) * INTERVAL '1 MONTH')",
      getSqlFrom(date), getSqlFrom(months));
  }


  @Override
  protected String getSqlForRandomString(Function function) {
    String lengthSql = getSqlFrom(function.getArguments().get(0));
    String randomString = "(SELECT STRING_AGG(MD5(RANDOM() :: TEXT), ''))";
    return "UPPER(SUBSTRING(" + randomString + ", 1, " + lengthSql + " :: INT))";
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

    Iterable<String> setStatements = Iterables.transform(
        statement.getSelectStatement().getFields(),
        field -> String.format("%s = EXCLUDED.%s", field.getImpliedName(), field.getImpliedName()));

    Iterable<String> keyFields = Iterables.transform(
        statement.getTableUniqueKey(),
        AliasedField::getImpliedName);

    StringBuilder sqlBuilder = new StringBuilder();

    sqlBuilder.append("INSERT INTO ")
              .append(schemaNamePrefix(statement.getTable()))
              .append(statement.getTable().getName())
              .append(" (")
              .append(Joiner.on(", ").join(destinationFields))
              .append(") ")
              .append(getSqlFrom(statement.getSelectStatement()))
              .append(" ON CONFLICT (")
              .append(Joiner.on(",").join(keyFields))
              .append(") DO UPDATE SET ")
              .append(Joiner.on(", ").join(setStatements));

    return sqlBuilder.toString();
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
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    return ImmutableList.of("ALTER TABLE " + schemaNamePrefix(table) + table.getName() + " ADD COLUMN " + column.getName() + " " + sqlRepresentationOfColumnType(column, true),
        addColumnComment(table, column));
  }


  @Override
  public Collection<String> getSqlForAnalyseTable(Table table) {
    return ImmutableList.of("ANALYZE " + schemaNamePrefix() + table.getName());
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
    boolean alterDefaultValue = oldColumn.getDefaultValue() != newColumn.getDefaultValue();

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
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("ALTER TABLE " + schemaNamePrefix(table) + table.getName()
                + (alterNullable ? " ALTER COLUMN " + newColumn.getName() + (newColumn.isNullable() ? " DROP NOT NULL" : " SET NOT NULL") : "")
                + (alterNullable && alterType ? "," : "")
                + (alterType ? " ALTER COLUMN " + newColumn.getName() + " TYPE " + sqlRepresentationOfColumnType(newColumn, false, false, true) : "")
                + (alterDefaultValue && (alterNullable || alterType) ? "," : "")
                + (alterDefaultValue ? " ALTER COLUMN " + newColumn.getName() + (!newColumn.getDefaultValue().isEmpty() ? " SET DEFAULT " + newColumn.getDefaultValue() : " DROP DEFAULT") : "")
        );
    return sqlBuilder.toString();
  }


  private String addColumnComment(Table table, Column column) {
    StringBuilder comment = new StringBuilder ("COMMENT ON COLUMN " + schemaNamePrefix() + table.getName() + "." + column.getName() + " IS 'REALNAME:[" + column.getName() + "]/TYPE:[" + column.getType().toString() + "]");
    if(column.isAutoNumbered()) {
      int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
      comment.append("/AUTONUMSTART:[" + autoNumberStart + "]");
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
  public Collection<String> rebuildTriggers(Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


  @Override
  public String indexDeploymentStatement(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ")
             .append(index.getName())
             .append(" ON ");
    if(!table.isTemporary()) {
      statement.append(schemaNamePrefix());
    }
    statement.append(table.getName())
             .append(" (")
             .append(Joiner.on(",").join(index.columnNames()))
             .append(")");

    return statement.toString();
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
  protected String getSqlForSome(AliasedField aliasedField) {
    return "bool_or(" + getSqlFrom(aliasedField) + ")";
  }


  @Override
  protected String getSqlForEvery(AliasedField aliasedField) {
    return "bool_and(" + getSqlFrom(aliasedField) + ")";
  }


}