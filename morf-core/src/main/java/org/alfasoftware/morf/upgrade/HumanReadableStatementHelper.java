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

package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.AbstractSelectStatement;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.BracketedExpression;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldFromSelectFirst;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.FunctionType;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * A helper class which generates human-readable schema and data change statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class HumanReadableStatementHelper {

  /**
   * Metadata for forming human readable forms of the function types.
   */
  private static final class FunctionTypeMetaData {

    /**
     * Prefix string, before the first argument.
     */
    final String prefix;

    /**
     * Suffix string, after the last argument.
     */
    final String suffix;

    /**
     * Argument separator.
     */
    final String sep;

    /**
     * Wrap the expression in parenthesis when used as part of a criterion or nested expression.
     */
    final boolean paren;

    /**
     * Whether to reverse the arguments to display.
     */
    final boolean reverseArgs;

    public FunctionTypeMetaData(final String prefix, final String suffix, final String sep, final boolean paren, final boolean reverseArgs) {
      this.prefix = prefix;
      this.suffix = suffix;
      this.sep = sep;
      this.paren = paren;
      this.reverseArgs = reverseArgs;
    }

  }

  private static final Map<FunctionType, FunctionTypeMetaData> functionTypeMetaData = new ImmutableMap.Builder<FunctionType, FunctionTypeMetaData>()
      .put(FunctionType.ADD_DAYS, new FunctionTypeMetaData("", " days", " plus ", false, false))
      .put(FunctionType.COALESCE, new FunctionTypeMetaData("first non-null of (", ")", ", ", false, false))
      .put(FunctionType.GREATEST, new FunctionTypeMetaData("maximum of (", ")", ", ", false, false))
      .put(FunctionType.LEAST, new FunctionTypeMetaData("minimum of (", ")", ", ", false, false))
      .put(FunctionType.COUNT, new FunctionTypeMetaData("count of ", "", "", false, false))
      .put(FunctionType.COUNT_DISTINCT, new FunctionTypeMetaData("count of distinct ", "", "", false, false))
      .put(FunctionType.DAYS_BETWEEN, new FunctionTypeMetaData("days between ", "", " and ", true, true))
      .put(FunctionType.DATE_TO_YYYYMMDD, new FunctionTypeMetaData("", "", "", false, false))
      .put(FunctionType.DATE_TO_YYYYMMDDHHMMSS, new FunctionTypeMetaData("", "", "", false, false))
      .put(FunctionType.NOW, new FunctionTypeMetaData("now", "", "", false, false))
      .put(FunctionType.FLOOR, new FunctionTypeMetaData("floor(", ")", "", false, false))
      .put(FunctionType.IS_NULL, new FunctionTypeMetaData("", " if null", " or ", true, false))
      .put(FunctionType.LEFT_PAD, new FunctionTypeMetaData("leftPad(", ")", ", ", false, false))
      .put(FunctionType.LEFT_TRIM, new FunctionTypeMetaData("left trimmed ", "", "", false, false))
      .put(FunctionType.LENGTH, new FunctionTypeMetaData("length of ", "", "", false, false))
      .put(FunctionType.BLOB_LENGTH, new FunctionTypeMetaData("length of blob ", "", "", false, false))
      .put(FunctionType.LOWER, new FunctionTypeMetaData("lower case ", "", "", false, false))
      .put(FunctionType.MAX, new FunctionTypeMetaData("highest ", "", "", false, false))
      .put(FunctionType.MIN, new FunctionTypeMetaData("lowest ", "", "", false, false))
      .put(FunctionType.AVERAGE, new FunctionTypeMetaData("average of ", "", "", false, false))
      .put(FunctionType.AVERAGE_DISTINCT, new FunctionTypeMetaData("average of distinct ", "", "", false, false))
      .put(FunctionType.SOME, new FunctionTypeMetaData("logical OR over ", "", "", false, false))
      .put(FunctionType.EVERY, new FunctionTypeMetaData("logical AND over ", "", "", false, false))
      .put(FunctionType.MONTHS_BETWEEN, new FunctionTypeMetaData("months between ", "", " and ", true, true))
      .put(FunctionType.YYYYMMDD_TO_DATE, new FunctionTypeMetaData("", "", "", false, false))
      .put(FunctionType.MOD, new FunctionTypeMetaData("", "", " mod ", false, false))
      .put(FunctionType.POWER, new FunctionTypeMetaData("", "", " to the power ", false, false))
      .put(FunctionType.RANDOM, new FunctionTypeMetaData("random", "", "", false, false))
      .put(FunctionType.RANDOM_STRING, new FunctionTypeMetaData("random ", " character string", "", false, false))
      .put(FunctionType.RIGHT_TRIM, new FunctionTypeMetaData("right trimmed ", "", "", false, false))
      .put(FunctionType.ROUND, new FunctionTypeMetaData("", " decimal places", " rounded to ", true, false))
      .put(FunctionType.SUBSTRING, new FunctionTypeMetaData("substring(", ")", ", ", false, false))
      .put(FunctionType.SUM, new FunctionTypeMetaData("sum of ", "", "", false, false))
      .put(FunctionType.SUM_DISTINCT, new FunctionTypeMetaData("sum of distinct ", "", "", false, false))
      .put(FunctionType.TRIM, new FunctionTypeMetaData("trimmed ", "", "", false, false))
      .put(FunctionType.UPPER, new FunctionTypeMetaData("upper case ", "", "", false, false))
      .put(FunctionType.LAST_DAY_OF_MONTH, new FunctionTypeMetaData("last day of month ", "", "", false, false))
      .build();

  /**
   * Generates a nullable / non-null string for the specified definition.
   *
   * @param definition the column definition
   * @return a string representation of nullable / non-null
   */
  private static String generateNullableString(final Column definition) {
    return definition.isNullable() ? "nullable" : "non-null";
  }


  /**
   * Generates a column definition string of the format "TYPE(LENGTH,PRECISION)".
   *
   * @param definition the column definition
   * @return the column definition as a string
   */
  private static String generateColumnDefinitionString(final Column definition) {
    if (definition.getType().hasScale()) {
      return String.format("%s(%d,%d)", definition.getType(), definition.getWidth(), definition.getScale());
    }
    if (definition.getType().hasWidth()) {
      return String.format("%s(%d)", definition.getType(), definition.getWidth());
    }
    return String.format("%s", definition.getType());
  }


  /**
   * @param tableName - the table name which needs its primary key columns changed
   * @param oldPrimaryKeyColumns - the list of table names to change the primary key columns from
   * @param newPrimaryKeyColumns - the list of table names to change the primary key columns to
   * @return a string containing the human-readable version of the action
   */
  public static String generateChangePrimaryKeyColumnsString(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    StringBuilder changePrimaryKeyColumnsBuilder = new StringBuilder();
    changePrimaryKeyColumnsBuilder.append(String.format("Change primary key columns on %s from %s to %s",
                                            tableName,
                                            "(" + Joiner.on(", ").join(oldPrimaryKeyColumns) + ")",
                                            "(" + Joiner.on(", ").join(newPrimaryKeyColumns) + ")"));
    return changePrimaryKeyColumnsBuilder.toString();
  }


  /**
   * @param tableName - the table name which needs its primary key columns changed
   * @param newPrimaryKeyColumns - the list of table names to primary key columns will become
   * @return a string containing the human-readable version of the action
   */
  public static String generateChangePrimaryKeyColumnsString(String tableName, List<String> newPrimaryKeyColumns) {
    StringBuilder changePrimaryKeyColumnsBuilder = new StringBuilder();
    changePrimaryKeyColumnsBuilder.append(String.format("Change primary key columns on %s to become %s",
                                                        tableName,
                                                        "(" + Joiner.on(", ").join(newPrimaryKeyColumns) + ")"));
    return changePrimaryKeyColumnsBuilder.toString();
  }


  /**
   * Generates a unique / non-unique string for the specified index definition.
   *
   * @param definition the definition of the index
   * @return a string representation of unique / non-unique
   */
  private static String generateUniqueIndexString(final Index definition) {
    return definition.isUnique() ? "unique" : "non-unique";
  }


  /**
   * Generates a string which represents the number of indexes specified.
   *
   * @param indexCount the number of indexes
   * @return a string representation of the number of indexes
   */
  private static String generateIndexCountString(final int indexCount) {
    if (indexCount == 0) {
      return "no indexes";
    }

    if (indexCount == 1) {
      return "1 index";
    }

    return indexCount + " indexes";
  }


  /**
   * Generates a string which represents the number of columns specified.
   *
   * @param columnCount the number of columns
   * @return a string representation of the number of columns
   */
  private static String generateColumnCountString(final int columnCount) {
    if (columnCount == 0) {
      return "no columns";
    }

    if (columnCount == 1) {
      return "1 column";
    }

    return columnCount + " columns";
  }


  /**
   * Generates a human-readable column description string for columns of a new table.
   *
   * @param columnDefinition the definition of the column
   * @return a string containing a human-readable description of the column
   */
  private static String generateNewTableColumnString(final Column columnDefinition) {
    return String.format("%n    - A %s column called %s [%s]%s",
      generateNullableString(columnDefinition),
      columnDefinition.getName(),
      generateColumnDefinitionString(columnDefinition),
      generateColumnDefaultValueClause(columnDefinition));
  }


  /**
   * Generates a human-readable "Add Column" string.
   *
   * @param tableName the table name to which the column is being added
   * @param definition the definition of the new column
   * @return a string containing the human-readable version of the action
   */
  public static String generateAddColumnString(final String tableName, final Column definition) {
    return String.format("Add a %s column to %s called %s [%s]%s",
      generateNullableString(definition),
      tableName,
      definition.getName(),
      generateColumnDefinitionString(definition),
      generateColumnDefaultValueClause(definition));
  }


  /**
   * Generates a clause for any default column value.
   *
   * @param definition the column definition.
   * @return a string containing a readable version of the default value
   */
  private static String generateColumnDefaultValueClause(final Column definition) {
    if (StringUtils.isEmpty(definition.getDefaultValue())) {
      return "";
    }
    if (NumberUtils.isNumber(definition.getDefaultValue())) {
      return ", set to " + definition.getDefaultValue();
    } else {
      return ", set to " + generateLiteral(definition.getDefaultValue());
    }
  }


  /**
   * Generates a human-readable "Add Column" string for a column with no default value
   * set at the database level but with a default initialiser for any existing records.
   *
   * @param tableName the table name to which the column is being added
   * @param definition the definition of the new column
   * @return a string containing the human-readable version of the action
   */
  public static String generateAddColumnString(final String tableName, final Column definition, final FieldLiteral defaultValue) {
    if (defaultValue == null) {
      return generateAddColumnString(tableName, definition);
    }

    return String.format("Add a %s column to %s called %s [%s], set to %s",
      generateNullableString(definition),
      tableName,
      definition.getName(),
      generateColumnDefinitionString(definition),
      generateFieldValueString(defaultValue));
  }


  /**
   * Generates a human-readable "Add Index" string.
   *
   * @param tableName the name of the table to add the index to
   * @param index the definition of the index to add
   * @return a string containing the human-readable version of the action
   */
  public static String generateAddIndexString(final String tableName, final Index index) {
    return String.format("Add %s index called %s to %s", generateUniqueIndexString(index), index.getName(), tableName);
  }


  /**
   * Generates a human-readable "Add Table" string.
   *
   * @param definition the definition of the new table
   * @return a string containing the human-readable version of the action
   */
  public static String generateAddTableString(final Table definition) {
    StringBuilder addTableBuilder = new StringBuilder();
    addTableBuilder.append(String.format("Create table %s with %s and %s",
                            definition.getName(),
                            generateColumnCountString(definition.columns().size()),
                            generateIndexCountString(definition.indexes().size()))
    );

    for (Column column : definition.columns()) {
      addTableBuilder.append(generateNewTableColumnString(column));
    }

    return addTableBuilder.toString();
  }


  /**
   * Generates a human-readable "Add Table From" string.
   *
   * @param definition the definition of the new table
   * @return a string containing the human-readable version of the action
   */
  public static String generateAddTableFromString(final Table definition, final SelectStatement selectStatement) {
    StringBuilder addTableBuilder = new StringBuilder()
        .append(generateAddTableString(definition));

    addTableBuilder
        .append(" from ")
        .append(generateSelectStatementString(selectStatement, false));

    return addTableBuilder
            .toString();
  }


  /**
   * Generates human-readable "Analyse Table" string.
   *
   * @param The table to analyse.
   */
  public static String generateAnalyseTableFromString(String tableName) {
    return String.format("Analyse table %s", tableName);
  }


  /**
   * @param from - The table name to change from
   * @param to - The table name to change to
   * @return a string containing the human-readable version of the action
   */
  public static String generateRenameTableString(String from, String to) {
    StringBuilder renameTableBuilder = new StringBuilder();
    renameTableBuilder.append(String.format("Rename table %s to %s",
                                from,
                                to));
    return renameTableBuilder.toString();
  }


  /**
   * Generates a human-readable "Change Column" string.
   *
   * @param tableName the name of the table on which the column currently exists
   * @param fromDefinition the original definition of the column
   * @param toDefinition the replacement definition of the column
   * @return a string containing the human-readable version of the action
   */
  public static String generateChangeColumnString(final String tableName, final Column fromDefinition, final Column toDefinition) {
    // If this is not a rename operation
    if (fromDefinition.getName().equals(toDefinition.getName())) {
      return String.format("Change column %s on %s from %s %s to %s %s", fromDefinition.getName(), tableName, generateNullableString(fromDefinition), generateColumnDefinitionString(fromDefinition), generateNullableString(toDefinition), generateColumnDefinitionString(toDefinition));
    }

    return String.format("Rename %s column %s [%s] on %s to %s %s [%s]", generateNullableString(fromDefinition), fromDefinition.getName(), generateColumnDefinitionString(fromDefinition), tableName, generateNullableString(toDefinition), toDefinition.getName(), generateColumnDefinitionString(toDefinition));
  }


  /**
   * Generates a human-readable "Change Index" string.
   *
   * @param tableName the name of the table to change the index on
   * @param fromIndex the original index definition
   * @param toIndex the replacement definition for the index
   * @return a string containing the human-readable version of the action
   */
  public static String generateChangeIndexString(final String tableName, final Index fromIndex, final Index toIndex) {
    if (fromIndex.isUnique() == toIndex.isUnique()) {
      return String.format("Change %s index %s on %s", generateUniqueIndexString(fromIndex), fromIndex.getName(), tableName);
    }

    return String.format("Change %s index %s on %s to be %s", generateUniqueIndexString(fromIndex), fromIndex.getName(), tableName, generateUniqueIndexString(toIndex));
  }


  /**
   * Generates a human-readable "Rename Index" string.
   *
   * @param tableName the name of the table to rename the index on
   * @param fromIndex the original index name
   * @param toIndex the replacement name for the index
   * @return a string containing the human-readable version of the action
   */
  public static String generateRenameIndexString(final String tableName, final String fromIndexName, final String toIndexName) {
    return String.format("Rename index %s on %s to %s", fromIndexName, tableName, toIndexName);
  }


  /**
   * Generates a human-readable "Remove Column" string.
   *
   * @param tableName the name of the table on which the column currently resides
   * @param definition the definition of the column to remove
   * @return a string containing the human-readable version of the action
   */
  public static String generateRemoveColumnString(final String tableName, final Column definition) {
    return String.format("Remove column %s from %s", definition.getName(), tableName);
  }


  /**
   * Generates a human-readable "Remove Index" string.
   *
   * @param tableName the name of the table on which the index resides.
   * @param index the definition of the index which will be removed
   * @return a string containing the human-readable version of the action
   */
  public static String generateRemoveIndexString(final String tableName, final Index index) {
    return String.format("Remove index %s from %s", index.getName(), tableName);
  }


  /**
   * Generates a human-readable "Remove Table" string.
   *
   * @param table the name of the table which will be removed
   * @return a string containing the human-readable version of the action
   */
  public static String generateRemoveTableString(final Table table) {
    return String.format("Remove table %s", table.getName());
  }


  /**
   * Generates a human-readable data upgrade description.
   *
   * @param statement the data upgrade statement to describe.
   * @param preferredSQLDialect the dialect to use, by preference, when a human readable description is not available. If
   *          SQL is not available in this dialect, or none was specified, then an arbitrary choice is made from the bundle
   *          of available raw SQL fragments.
   * @return a string containing the human-readable description of the action.
   */
  public static String generateDataUpgradeString(final Statement statement, final String preferredSQLDialect) {
    if (statement instanceof DeleteStatement) {
      return generateDeleteStatementString((DeleteStatement)statement);
    } else if (statement instanceof InsertStatement) {
      return generateInsertStatementString((InsertStatement)statement);
    } else if (statement instanceof MergeStatement) {
      return generateMergeStatementString((MergeStatement)statement);
    } else if (statement instanceof PortableSqlStatement) {
      return generatePortableSqlStatementString((PortableSqlStatement)statement, preferredSQLDialect);
    } else if (statement instanceof TruncateStatement) {
      return generateTruncateStatementString((TruncateStatement)statement);
    } else if (statement instanceof UpdateStatement) {
      return generateUpdateStatementString((UpdateStatement)statement);
    } else {
      throw new UnsupportedOperationException("Unable to generate data upgrade string for: [" + statement.getClass().getName() + "]");
    }
  }


  /**
   * Generates a human-readable description of a data delete operation.
   *
   * @param statement the data upgrade statement to describe.
   * @return a string containing the human-readable description of the operation.
   */
  private static String generateDeleteStatementString(final DeleteStatement statement) {
    if (statement.getWhereCriterion() == null) {
      // When no where clause, use the same text as truncation operations
      return generateTruncateStatementString(statement.getTable());
    } else {
      return String.format("Delete records in %s%s", statement.getTable().getName(), generateWhereClause(statement.getWhereCriterion()));
    }
  }


  /**
   * Generates a human-readable description of a data insert operation.
   *
   * @param statement the data upgrade statement to describe.
   * @return a string containing the human-readable description of the operation.
   */
  private static String generateInsertStatementString(final InsertStatement statement) {
    final StringBuilder sb = new StringBuilder();
    final SelectStatement source = statement.getSelectStatement();
    if (source == null) {
      // No select statement; single record insert
      sb.append(String.format("Add record into %s:", statement.getTable().getName()));
      for (AliasedField field : statement.getValues()) {
        sb.append(generateAliasedFieldAssignmentString(field));
      }
    } else {
      // Multiple record insert
      sb.append(String.format("Add records into %s: ", statement.getTable().getName()));
      sb.append(generateFieldSymbolStrings(source.getFields()));
      sb.append(generateFromAndWhereClause(source, true));
    }
    return sb.toString();
  }


  /**
   * Generates a human-readable description of a data merge operation.
   *
   * @param statement the data upgrade statement to describe.
   * @return a string containing the human-readable description of the operation.
   */
  private static String generateMergeStatementString(final MergeStatement statement) {
    final SelectStatement source = statement.getSelectStatement();
    if (source.getTable() == null) {
      // No select statement; single record merge
      final StringBuilder sb = new StringBuilder();
      sb.append(String.format("Merge record into %s:", statement.getTable().getName()));
      for (AliasedField field : source.getFields()) {
        sb.append(generateAliasedFieldAssignmentString(field));
      }
      return sb.toString();
    } else {
      // Multiple record insert
      return String.format("Merge records into %s from %s%s",
        statement.getTable().getName(), source.getTable().getName(), generateWhereClause(source.getWhereCriterion()));
    }
  }


  /**
   * Generates a human-readable description of a raw SQL operation.
   *
   * @param statement the data upgrade statement to describe.
   * @param preferredSQLDialect the SQL dialect to use, if available. If this is {@code null} or the preferred
   *          dialect is not available in the statement bundle then an arbitrary choice is made.
   * @return a string containing the human-readable description of the operation.
   */
  private static String generatePortableSqlStatementString(final PortableSqlStatement statement, final String preferredSQLDialect) {
    final Map<String, String> sqlStrings = statement.getStatements();
    String sql = sqlStrings.get(preferredSQLDialect);
    if (sql == null) {
      sql = sqlStrings.values().iterator().next();
    }
    final StringBuilder sb = new StringBuilder("Run the following raw SQL statement");
    // Raw SQL fragments may have either "\n" written into them or use the platform separator
    final String[] lines = sql.split(System.lineSeparator() + "|\\n");
    for (int i = 0; i < lines.length; i++) {
      if (i > 0) {
        sb.append(System.lineSeparator()).append("      ");
      } else {
        sb.append(System.lineSeparator()).append("    - ");
      }
      sb.append(lines[i]);
    }
    return sb.toString();
  }


  /**
   * Generates a human-readable description of a data truncate operation.
   *
   * @param statement the data upgrade statement to describe.
   * @return a string containing the human-readable description of the operation.
   */
  private static String generateTruncateStatementString(final TruncateStatement statement) {
    return generateTruncateStatementString(statement.getTable());
  }


  /**
   * Generates a human-readable description of a data truncate operation.
   *
   * @param statement the data upgrade statement to describe
   * @return a string containing the human-readable description of the operation
   */
  private static String generateTruncateStatementString(final TableReference table) {
    return String.format("Delete all records in %s", table.getName());
  }


  /**
   * Generates a human-readable description of a data update operation.
   *
   * @param statement the data upgrade statement to describe
   * @return a string containing the human-readable description of the operation
   */
  private static String generateUpdateStatementString(final UpdateStatement statement) {
    final StringBuilder sb = new StringBuilder();
    if (statement.getWhereCriterion() == null) {
      sb.append(String.format("Update records in %s", statement.getTable().getName()));
    } else {
      sb.append(String.format("Update %s%s", statement.getTable().getName(), generateWhereClause(statement.getWhereCriterion())));
    }
    for (AliasedField field : statement.getFields()) {
      sb.append(generateAliasedFieldAssignmentString(field));
    }
    return sb.toString();
  }


  /**
   * Generates the from and where clause for a select statement. If there is no selection
   * expression then the 'where' fragment will be omitted.
   *
   * @param statement the statement to describe.
   * @param prefix whether to include the " from " prefix in the clause
   * @return a string containing the human-readable description of the source data.
   */
  private static String generateFromAndWhereClause(final AbstractSelectStatement<?> statement, final boolean prefix) {
    final StringBuilder sb = new StringBuilder();

    if (statement.getTable() == null) {
      if (statement.getFromSelects() == null) {
        throw new UnsupportedOperationException("No table or sub-selects for: [" + statement.getClass().getName() + "]");
      } else {
        boolean comma = false;
        for (AbstractSelectStatement<?> subSelect : statement.getFromSelects()) {
          if (comma) {
            sb.append(", ");
          } else {
            if (prefix) {
              sb.append(" from ");
            }
            comma = true;
          }
          sb.append(generateSelectStatementString(subSelect, true));
        }
        return sb.toString();
      }
    } else {
      if (prefix) {
        sb.append(" from ");
      }
      sb.append(statement.getTable().getName());
    }

    if (statement.getJoins() != null) {
      for (Join join : statement.getJoins()) {
        sb.append(" and ");
        if (join.getTable() == null) {
          if (join.getSubSelect() == null) {
            throw new UnsupportedOperationException("No table or sub-selects for: [" + join.getClass().getName() + "]");
          } else {
            sb.append('(').append(generateSelectStatementString(join.getSubSelect(), false)).append(')');
          }
        } else {
          sb.append(join.getTable().getName());
        }
        if (join.getCriterion() != null) {
          sb.append(", joined on ").append(generateCriterionString(join.getCriterion(), false)).append(',');
        }
      }
      if (sb.charAt(sb.length() - 1) == ',' && statement.getWhereCriterion() == null) {
        sb.delete(sb.length() - 1, sb.length());
      }
    }

    sb.append(generateWhereClause(statement.getWhereCriterion()));
    return sb.toString();
  }


  /**
   * Generates a string describing record selection criteria. If there are no selection
   * criteria then an empty string is returned.
   *
   * @param criterion the criterion to describe.
   * @return a string containing the human-readable description of the clause.
   */
  private static String generateWhereClause(final Criterion criterion) {
    if (criterion == null) {
      return "";
    } else {
      return " where " + generateCriterionString(criterion, false);
    }
  }


  /**
   * Generates a string describing a record ordering clause. If there is no ordering clause
   * then an empty string is returned.
   */
  private static String generateOrderByClause(final List<AliasedField> fields) {
    if (fields == null || fields.isEmpty()) {
      return "";
    } else {
      return " ordered by " + generateFieldSymbolStrings(fields);
    }
  }


  /**
   * Generates a string describing a predicate. This gets used as part of generating a
   * 'where' clause for record selection as well as the {@link CaseStatement} fields.
   *
   * <p>This has package visibility for testing.</p>
   *
   * @param criterion the criterion to describe.
   * @return a string containing the human-readable description of the clause.
   */
  static String generateCriterionString(final Criterion criterion) {
    return generateCriterionString(criterion, false);
  }


  /**
   * Generates a string describing a predicate.
   *
   * <p>The human-readable form of some of the logical operators, for example IN and LIKE, requires
   * that an inversion alter the display of the operator rather than use the machine-readable
   * NOT prefix. This is implemented through the {@code invert} parameter.</p>
   *
   * @param criterion the criterion to describe.
   * @param invert whether the predicate has been inverted.
   * @return a string containing the human-readable description of the clause.
   */
  private static String generateCriterionString(final Criterion criterion, final boolean invert) {
    switch (criterion.getOperator()) {
      case AND:
        if (invert) {
          // NOT(AND(a,b,...)) === OR(NOT(A),NOT(B),...)
          return generateListCriterionString(criterion, " or ", true);
        } else {
          return generateListCriterionString(criterion, " and ", false);
        }
      case EQ:
        return generateBinaryOperatorString(criterion, invert ? "is not" : "is");
      case EXISTS:
        if (invert) {
          return String.format("not exists %s", generateFromAndWhereClause(criterion.getSelectStatement(), false));
        } else {
          return String.format("exists %s", generateFromAndWhereClause(criterion.getSelectStatement(), false));
        }
      case GT:
        return generateBinaryOperatorString(criterion, invert ? "is less than or equal to" : "is greater than");
      case GTE:
        return generateBinaryOperatorString(criterion, invert ? "is less than" : "is greater than or equal to");
      case IN:
        return generateInCriterionString(criterion, invert);
      case ISNOTNULL:
        return String.format("%s is%s null", generateFieldSymbolString(criterion.getField()), invert ? "" : " not");
      case ISNULL:
        return String.format("%s is%s null", generateFieldSymbolString(criterion.getField()), invert ? " not" : "");
      case LIKE:
        return generateBinaryOperatorString(criterion, invert ? "is not like" : "is like");
      case LT:
        return generateBinaryOperatorString(criterion, invert ? "is greater than or equal to" : "is less than");
      case LTE:
        return generateBinaryOperatorString(criterion, invert ? "is greater than" : "is less than or equal to");
      case NEQ:
        return generateBinaryOperatorString(criterion, invert ? "is" : "is not");
      case NOT:
        return generateCriterionString(criterion.getCriteria().get(0), !invert);
      case OR:
        if (invert) {
          // NOT(OR(a,b,...)) === AND(NOT(A),NOT(B),...)
          return generateListCriterionString(criterion, " and ", true);
        } else {
          return generateListCriterionString(criterion, " or ", false);
        }
      default:
        throw new UnsupportedOperationException("Unable to generate data upgrade string for: [" + criterion.getOperator().name() + "]");
    }
  }


  /**
   * Tests if a field instance is sufficiently complicated to warrant putting brackets around it
   * when it is used as a criterion operand.
   *
   * @param field the field to test.
   * @return {@code true} if it should be placed in parenthesis.
   */
  private static boolean isComplexField(final AliasedField field) {
    if (field instanceof Cast) {
      return isComplexField(((Cast)field).getExpression());
    } else if (field instanceof ConcatenatedField
        || field instanceof FieldLiteral
        || field instanceof FieldReference) {
      return false;
    } else if (field instanceof Function) {
      final FunctionTypeMetaData metaData = functionTypeMetaData.get(((Function)field).getType());
      return metaData != null && metaData.paren;
    } else {
      return true;
    }
  }


  /**
   * Wraps a string in parenthesis if the field is considered {@link #isComplexField}.
   *
   * @param string the string to process.
   * @param field the field to evaluate.
   * @return the original string, or one wrapped in parenthesis.
   */
  private static String paren(final String string, final AliasedField field) {
    if (isComplexField(field)) {
      return "(" + string + ")";
    } else {
      return string;
    }
  }


  /**
   * Generates a string describing a criterion made up of a list of sub-criteria and a joining
   * operator, for example the OR or AND operators.
   *
   * @param criterion the criterion with a list of sub-criteria to be composed.
   * @param join the joining operator, including spaces, for example " or ".
   * @param invert whether to use the inverse of each sub-criteria.
   * @return the string.
   */
  private static String generateListCriterionString(final Criterion criterion, final String join, final boolean invert) {
    final StringBuilder sb = new StringBuilder();
    boolean comma = false;
    for (Criterion componentCriterion : criterion.getCriteria()) {
      if (comma) {
        sb.append(join);
      } else {
        comma = true;
      }
      // Put brackets around AND and OR inner clauses
      if (Operator.AND.equals(componentCriterion.getOperator()) || Operator.OR.equals(componentCriterion.getOperator())) {
        sb.append('(').append(generateCriterionString(componentCriterion, invert)).append(')');
      } else {
        sb.append(generateCriterionString(componentCriterion, invert));
      }
    }
    return sb.toString();
  }


  /**
   * Generates a string describing a binary criterion operator.
   *
   * @param criterion the item to describe.
   * @param operator the string operator to separate the first and second parameter, not including spaces.
   * @return the string.
   */
  private static String generateBinaryOperatorString(final Criterion criterion, final String operator) {
    final String left = paren(generateFieldSymbolString(criterion.getField()), criterion.getField());
    final String right = generateCriterionValueString(criterion.getValue());
    return String.format("%s %s %s", left, operator, right);
  }


  /**
   * Generates a string describing the IN binary operator. This may generate either a list of literal
   * values, such as "x in ('1', '2')", or a sub-select statement. For the single-field sub-select form
   * a more readable "x in Foo table" form is produced rather than "x in (select x from foo)".
   *
   * @param criterion the item to describe.
   * @param invert {@code true} for the NOT IN operation, {@code false} for the IN operation.
   * @return the string.
   */
  private static String generateInCriterionString(final Criterion criterion, final boolean invert) {
    final StringBuilder sb = new StringBuilder();
    sb.append(generateFieldSymbolString(criterion.getField()));
    sb.append(" is ");
    if (invert) {
      sb.append("not ");
    }
    sb.append("in ");

    final SelectStatement source = criterion.getSelectStatement();
    if (source == null) {
      // List of literals
      sb.append('(');
      final List<?> values = (List<?>)criterion.getValue();
      boolean comma = false;
      for (Object value : values) {
        if (comma) {
          sb.append(", ");
        } else {
          comma = true;
        }
        sb.append(generateCriterionValueString(value));
      }
      sb.append(')');
    } else {
      if (source.getFields().size () == 1 && source.getTable() != null && (source.getJoins() == null || source.getJoins().isEmpty())) {
        sb.append(source.getTable().getName()).append(generateWhereClause(source.getWhereCriterion()));
      } else {
        sb.append(generateSelectStatementString(source, false));
      }
    }
    return sb.toString();
  }


  /**
   * Generates a literal string from a value held in a {@link Criterion} object. The value might be a
   * either a standard Java type, such as {@link String} or {@link Double}, or a {@link FieldLiteral}.
   *
   * @param value the value to produce a string for.
   * @return the string.
   */
  private static String generateCriterionValueString(final Object value) {
    if (value instanceof AliasedField) {
      final AliasedField field = (AliasedField)value;
      return paren(generateFieldValueString(field), field);
    } else {
      return generateLiteral(value);
    }
  }


  /**
   * Generates a string for a select, or sub-select, statement. The "select" prefixing keyword is optional;
   * depending on the context it is being used the result may be more readable if it is omitted.
   *
   * @param statement the select statement to describe.
   * @param prefix {@code true} to include a leading "select ", {@code false} otherwise.
   * @return the string.
   */
  @SuppressWarnings("rawtypes")
  private static String generateSelectStatementString(final AbstractSelectStatement<?> statement, final boolean prefix) {
    final StringBuilder sb = new StringBuilder();
    if (prefix) {
      sb.append("select");
    }
    if ((AbstractSelectStatement)statement instanceof SelectFirstStatement) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append("first");
    }
    boolean comma = false;
    for (AliasedField field : statement.getFields()) {
      if (comma) {
        sb.append(", ");
      } else {
        comma = true;
        if (sb.length() > 0) {
          sb.append(' ');
        }
      }
      sb.append(generateFieldSymbolString(field));
    }
    sb.append(generateFromAndWhereClause(statement, true));
    sb.append(generateOrderByClause(statement.getOrderBys()));
    return sb.toString();
  }


  /**
   * Generates a string describing a field. This is a symbolic name, if available, otherwise any literal values.
   *
   * @param field the field to describe.
   * @return a string containing the name of the field.
   */
  private static String generateFieldSymbolString(final AliasedField field) {
    if (StringUtils.isEmpty(field.getImpliedName())) {
      if (field instanceof Cast) {
        return generateFieldSymbolString(((Cast)field).getExpression());
      } else {
        return generateFieldValueString(field);
      }
    } else {
      return field.getImpliedName();
    }
  }


  /**
   * Generates a string describing a collection of fields.
   *
   * @param fields the fields to describe.
   * @return a string with a comma separated list of field names.
   */
  private static String generateFieldSymbolStrings(final Collection<AliasedField> fields) {
    final StringBuilder sb = new StringBuilder();
    int count = 0;
    for (AliasedField field : fields) {
      count++;
      if (count == fields.size()) {
        if (count > 1) {
          sb.append(" and ");
        }
      } else if (count > 1) {
        sb.append(", ");
      }
      sb.append(generateFieldSymbolString(field));
    }
    return sb.toString();
  }


  /**
   * Generates a string with a literal value. If the value is numeric then it is unquoted, otherwise it
   * is surrounded by single quote characters.
   *
   * @param value the value to process.
   * @return the string form, created by the value's {@link Object#toString} method with quotes if necessary.
   */
  private static String generateLiteral(final Object value) {
    if (value == null) {
      return "null";
    } else if (value instanceof Number) {
      return value.toString();
    } else {
      return "'" + value + "'";
    }
  }


  /**
   * Generates a string describing a field or literal value.
   *
   * @param field the field to describe.
   * @return a string containing the literal value.
   */
  private static String generateFieldValueString(final AliasedField field) {
    if (field instanceof CaseStatement) {
      final StringBuilder sb = new StringBuilder("(");
      for (WhenCondition when : ((CaseStatement)field).getWhenConditions()) {
        if (sb.length() > 1) {
          sb.append("; ");
        }
        sb.append(String.format("%s if %s", generateFieldValueString(when.getValue()), generateCriterionString(when.getCriterion(), false)));
      }
      if (sb.length() > 0) {
        sb.append("; otherwise ");
      }
      sb.append(generateFieldValueString(((CaseStatement)field).getDefaultValue()));
      return sb.append(')').toString();
    } else if (field instanceof Cast) {
      return generateFieldValueString(((Cast)field).getExpression());
    } else if (field instanceof ConcatenatedField) {
      return "the concatenation of " + generateFieldSymbolStrings(((ConcatenatedField)field).getConcatenationFields());
    } else if (field instanceof FieldFromSelect) {
      return generateSelectStatementString(((FieldFromSelect)field).getSelectStatement(), false);
    } else if (field instanceof FieldFromSelectFirst) {
      return generateSelectStatementString(((FieldFromSelectFirst)field).getSelectFirstStatement(), false);
    } else if (field instanceof FieldLiteral) {
      final String value = ((FieldLiteral)field).getValue();
      if (NumberUtils.isNumber(value)) {
        return value;
      } else {
        return generateLiteral(value);
      }
    } else if (field instanceof FieldReference) {
      return ((FieldReference)field).getName();
    } else if (field instanceof Function) {
      final FunctionType type = ((Function)field).getType();
      List<AliasedField> args = ((Function)field).getArguments();
      if (type == FunctionType.COUNT && args.isEmpty()) {
        // Handle COUNT(*) as a special case
        return "record count";
      } else {
        final FunctionTypeMetaData function = functionTypeMetaData.get(type);
        final StringBuilder sb = new StringBuilder(function.prefix);
        boolean comma = false;
        if (function.reverseArgs) {
          args = Lists.reverse(args);
        }
        for (AliasedField arg : args) {
          if (comma) {
            sb.append(function.sep);
          } else {
            comma = true;
          }
          sb.append(generateFieldValueString(arg));
        }
        return sb.append(function.suffix).toString();
      }
    } else if (field instanceof MathsField) {
      final MathsField maths = (MathsField)field;
      return String.format("%s %s %s", generateFieldValueString(maths.getLeftField()), maths.getOperator(), generateFieldValueString(maths.getRightField()));
    } else if (field instanceof BracketedExpression) {
      final BracketedExpression bracketExpression = (BracketedExpression)field;
      return String.format("(%s)", generateFieldValueString(bracketExpression.getInnerExpression()));
    } else {
      throw new UnsupportedOperationException("Unable to generate data upgrade string for: [" + field.getClass().getName() + "]");
    }
  }


  /**
   * Generates a string describing a field reference update.
   *
   * <p>Package visibility for testing.</p>
   *
   * @param field the field to describe.
   * @return a string containing the human-readable description of the field update.
   */
  static String generateAliasedFieldAssignmentString(final AliasedField field) {
    if (field instanceof CaseStatement) {
      final StringBuilder sb = new StringBuilder();
      for (WhenCondition when : ((CaseStatement)field).getWhenConditions()) {
        sb.append(String.format("%n    - If %s then set %s to %s", generateCriterionString(when.getCriterion(), false), generateFieldSymbolString(field), generateFieldValueString(when.getValue())));
      }
      sb.append(String.format("%n    - Otherwise set %s to %s", generateFieldSymbolString(field), generateFieldValueString(((CaseStatement)field).getDefaultValue())));
      return sb.toString();
    } else if (field instanceof Cast) {
      if (((Cast)field).getExpression() instanceof FieldReference) {
        return String.format("%n    - Set %s to %s's value", generateFieldSymbolString(field), ((FieldReference)((Cast)field).getExpression()).getName());
      } else {
        return String.format("%n    - Set %s to %s", generateFieldSymbolString(field), generateFieldValueString(field));
      }
    } else if (field instanceof FieldReference) {
      return String.format("%n    - Set %s to %s's value", generateFieldSymbolString(field), ((FieldReference)field).getName());
    } else {
      return String.format("%n    - Set %s to %s", generateFieldSymbolString(field), generateFieldValueString(field));
    }
  }

}
