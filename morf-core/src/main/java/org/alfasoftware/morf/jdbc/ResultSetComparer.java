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

package org.alfasoftware.morf.jdbc;

import static java.sql.Types.BIGINT;
import static java.sql.Types.CHAR;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISMATCH;
import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISSING_LEFT;
import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISSING_RIGHT;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.stringcomparator.DatabaseEquivalentStringComparator;
import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Compares two {@link ResultSet}s.
 * <p>
 * These must <em>either</em> have a single row, <em>or</em>:
 * </p>
 * <ul>
 * <li>Have common key column(s) <em>and</em></li>
 * <li>Be ordered by those key column(s) in the order they are expressed in the
 * keyColumns argument.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class ResultSetComparer {

  /**
   * Factory to create {@link ResultSetComparer} instances
   *
   * @author Copyright (c) Alfa Financial Software 2016
   */
  @ImplementedBy(FactoryImpl.class)
  public interface Factory {

    /**
     * @param leftConnectionResources resources for the left query
     * @param rightConnectionResources resources for the right query
     * @param terminatePredicate a Predicate to terminate the query
     * @return {@link ResultSetComparer} that uses the supplied corresponding connections, each for the corresponding query
     */
    ResultSetComparer create(ConnectionResources leftConnectionResources, ConnectionResources rightConnectionResources,Predicate<Void> terminatePredicate);


    /**
     * @param leftConnectionResources resources for the left query
     * @param rightConnectionResources resources for the right query
     * @return {@link ResultSetComparer} that uses the supplied corresponding connections, each for the corresponding query
     */
    ResultSetComparer create(ConnectionResources leftConnectionResources, ConnectionResources rightConnectionResources);


    /**
     * @return {@link ResultSetComparer} that uses the {@link ConnectionResources} bound in in order to perform both queries
     */
    ResultSetComparer create();
  }


  /**
   * Implementation of {@link Factory}
   *
   * @author Copyright (c) Alfa Financial Software 2016
   */
  static final class FactoryImpl implements Factory {

    private final ConnectionResources connectionResources;
    private final Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator;

    @Inject
    FactoryImpl(ConnectionResources connectionResources, Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator) {
      this.connectionResources = connectionResources;
      this.databaseEquivalentStringComparator = databaseEquivalentStringComparator;
    }

    @Override
    public ResultSetComparer create(ConnectionResources leftConnectionResources, ConnectionResources rightConnectionResources, Predicate<Void> terminatePredicate) {
      return new ResultSetComparer(leftConnectionResources, rightConnectionResources, terminatePredicate, databaseEquivalentStringComparator);
    }

    @Override
    public ResultSetComparer create(ConnectionResources leftConnectionResources, ConnectionResources rightConnectionResources) {
      return new ResultSetComparer(leftConnectionResources, rightConnectionResources, databaseEquivalentStringComparator);
    }

    @Override
    public ResultSetComparer create() {
      return new ResultSetComparer(connectionResources, databaseEquivalentStringComparator);
    }
  }



  /** Value used to represent a field value where the record does not exist in the record set */
  public static final String RECORD_NOT_PRESENT = "<Not present>";

  private final SqlDialect leftSqlDialect;
  private final SqlDialect rightSqlDialect;
  private final Optional<Predicate<Void>> terminatePredicate;
  private final Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator;


  /**
   * Injected constructor.
   */
  ResultSetComparer(ConnectionResources connectionResources,
                    Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator) {
    this.leftSqlDialect = connectionResources.sqlDialect();
    this.rightSqlDialect = connectionResources.sqlDialect();
    this.terminatePredicate = Optional.absent();
    this.databaseEquivalentStringComparator = databaseEquivalentStringComparator;
  }



  /**
   * Injected constructor.
   */
  ResultSetComparer(ConnectionResources leftConnectionResources,
                    ConnectionResources rightConnectionResources,
                    Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator) {
    this.leftSqlDialect = leftConnectionResources.sqlDialect();
    this.rightSqlDialect = rightConnectionResources.sqlDialect();
    this.terminatePredicate = Optional.absent();
    this.databaseEquivalentStringComparator = databaseEquivalentStringComparator;
  }


  /**
   * Injected constructor.
   */
  ResultSetComparer(ConnectionResources leftConnectionResources,
                    ConnectionResources rightConnectionResources,
                    Predicate<Void> terminatePredicate,
                    Provider<DatabaseEquivalentStringComparator> databaseEquivalentStringComparator) {
    this.leftSqlDialect = leftConnectionResources.sqlDialect();
    this.rightSqlDialect = rightConnectionResources.sqlDialect();
    this.terminatePredicate = Optional.of(terminatePredicate);
    this.databaseEquivalentStringComparator = databaseEquivalentStringComparator;
  }


  /**
   * Given 2 data sets, return the number of mismatches between them, and
   * callback with the details of any mismatches as they are found. This method
   * will generate the result sets itself by executing two select statements
   * using the supplied connection. See {@link ResultSetMismatch} for definition
   * of a mismatch.
   *
   * @param keyColumns The indexes of the key columns common to both data sets.
   *          If this is empty, the result sets must return only one record.
   * @param left The left hand data set {@link SelectStatement}
   * @param right The right hand data set {@link SelectStatement}
   * @param connection a database connection
   * @param callback the mismatch callback interface implementation.
   * @return the number of mismatches between the two data sets.
   */
  public int compare(int[] keyColumns, SelectStatement left, SelectStatement right, Connection connection, CompareCallback callback) {
    return compare(keyColumns, left, right, connection, connection, callback);
  }


  /**
   * Given 2 data sets, return the number of mismatches between them, and
   * callback with the details of any mismatches as they are found. This method
   * will generate the result sets itself by executing two select statements
   * using the supplied connection. See {@link ResultSetMismatch} for definition
   * of a mismatch.
   *
   * @param keyColumns The indexes of the key columns common to both data sets.
   *          If this is empty, the result sets must return only one record.
   * @param left The left hand data set {@link SelectStatement}
   * @param right The right hand data set {@link SelectStatement}
   * @param leftConnection a database connection to use for the left statement.
   * @param rightConnection a database connection to use for the right statement.
   * @param callback the mismatch callback interface implementation.
   * @return the number of mismatches between the two data sets.
   */
  public int compare(int[] keyColumns, SelectStatement left, SelectStatement right, Connection leftConnection, Connection rightConnection, CompareCallback callback) {
    Statement statementLeft = null;
    Statement statementRight = null;
    ResultSet rsLeft = null;
    ResultSet rsRight = null;
    String leftSql = leftSqlDialect.convertStatementToSQL(left);
    String rightSql = rightSqlDialect.convertStatementToSQL(right);
    try {
      try {
        statementLeft = leftConnection.createStatement();
        statementRight = rightConnection.createStatement();
        rsLeft = statementLeft.executeQuery(leftSql);
        rsRight = statementRight.executeQuery(rightSql);
        return compare(keyColumns, rsLeft, rsRight, callback);
      } finally {
        if (statementLeft != null) statementLeft.close();
        if (statementRight != null) statementRight.close();
        if (rsLeft != null) rsLeft.close();
        if (rsRight != null) rsRight.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error comparing SQL statements [" + leftSql + ", " + rightSql + "]", e);
    }
  }


  /**
   * Given 2 data sets, return the number of mismatches between them, and
   * callback with the details of any mismatches as they are found. See
   * {@link ResultSetMismatch} for definition of a mismatch.
   *
   * @param keyColumns The indexes of the key columns common to both data sets.
   *          If this is empty, the result sets must return only one record.
   * @param left The left hand data set.
   * @param right The right hand data set.
   * @param callBack the mismatch callback interface implementation.
   * @return the number of mismatches between the two data sets.
   */
  public int compare(int[] keyColumns, ResultSet left, ResultSet right, CompareCallback callBack) {
    boolean expectingSingleRowResult = keyColumns.length == 0;
    int misMatchCount = 0;
    try {

      // Check metaData matches
      ResultSetMetaData metadataLeft = left.getMetaData();
      ResultSetMetaData metadataRight = right.getMetaData();
      compareMetadata(metadataLeft, metadataRight);

      List<Integer> valueCols = getNonKeyColumns(metadataLeft, Sets.newHashSet(ArrayUtils.toObject(keyColumns)));

      boolean leftHasRow = left.next();
      boolean rightHasRow = right.next();
      while (leftHasRow || rightHasRow) {
        String[] keys = new String[0];

        // First compare the key columns.  If the key columns mismatch, advance the result set with the
        // lower value for the key columns until we get a match.
        if (!expectingSingleRowResult) {
          MismatchType mismatchType;
          do {

            // Check for key column mismatches
            mismatchType = null;
            List<String> keyValues = Lists.newArrayList();
            for (int keyCol : keyColumns) {
              int columnType = metadataLeft.getColumnType(keyCol);
              if (mismatchType == null) {
                mismatchType = compareKeyColumn(left, right, keyCol, columnType, leftHasRow, rightHasRow);
              }
              keyValues.add(valueToString(columnToValue(mismatchType == MISSING_LEFT ? right : left, keyCol, columnType), columnType));
            }
            keys = keyValues.toArray(new String[keyValues.size()]);

            // If we find a mismatch...
            if (mismatchType != null) {
              // Fire a callback for each missing non-key value
              misMatchCount += callbackValueMismatches(left, right, callBack, metadataRight, valueCols, keys, mismatchType);

              // Advance the recordset the missing key was found in
              if (mismatchType == MISSING_RIGHT) {           // NOPMD
                leftHasRow = leftHasRow && left.next();      // NOPMD
              } else if (mismatchType == MISSING_LEFT) {     // NOPMD
                rightHasRow = rightHasRow && right.next();   // NOPMD
              }
            }
          } while (mismatchType != null && (leftHasRow || rightHasRow));
        }

        // Compare non-key columns after matching row is found and is not end of result set.
        // Remember to check the situation where a single row data set comparison might
        // actually have yielded no rows on one side, or the other, or both.
        if (expectingSingleRowResult) {
          if (!leftHasRow) {
            misMatchCount += callbackValueMismatches(left, right, callBack, metadataRight, valueCols, keys, MISSING_LEFT);
          }
          if (!rightHasRow) {
            misMatchCount += callbackValueMismatches(left, right, callBack, metadataRight, valueCols, keys, MISSING_RIGHT);
          }
        }

        // Finally actually compare the values for key matched rows.
        if (leftHasRow && rightHasRow) {
          misMatchCount += callbackValueMismatches(left, right, callBack, metadataRight, valueCols, keys, MISMATCH);
        }

        // Move cursor forward
        leftHasRow = leftHasRow && left.next();
        rightHasRow = rightHasRow && right.next();

        if ((leftHasRow || rightHasRow) && expectingSingleRowResult) {
          throw new IllegalStateException("Comparison can only handle one row for keyless result sets");
        }

        if(terminatePredicate.isPresent() && terminatePredicate.get().apply(null)) {
          return misMatchCount;
        }
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error traversing result set", e);
    }
    return misMatchCount;
  }


  /**
   * Fire callbacks for any mismatches on value columns.
   */
  private int callbackValueMismatches(ResultSet left, ResultSet right, CompareCallback callBack, ResultSetMetaData metadataRight, List<Integer> valueCols, String[] keys, MismatchType mismatchType) throws SQLException {
    int misMatchCount = 0;
    for (int i : valueCols) {
      Optional<ResultSetMismatch> mismatch = valueCheck(left, right, keys, i, metadataRight.getColumnType(i), mismatchType);
      if (mismatch.isPresent()) {
        callBack.mismatch(mismatch.get());
        misMatchCount++;
      }
    }
    return misMatchCount;
  }


  @SuppressWarnings("rawtypes")
  private Comparable columnToValue(ResultSet resultSet, int columnIndex, int columnType) throws SQLException {
    if (columnTypeIsString(columnType)) {
      return resultSet.getString(columnIndex);
    } else if (columnTypeIsNumeric(columnType)) {
      BigDecimal bigDecimal = resultSet.getBigDecimal(columnIndex);
      return bigDecimal == null ? null : bigDecimal.stripTrailingZeros();
    } else if (columnTypeIsBoolean(columnType)) {
      return resultSet.getBoolean(columnIndex);
    } else if (columnTypeIsDate(columnType)) {
      return resultSet.getDate(columnIndex);
    } else {
      throw new IllegalArgumentException("Column type " + columnType + " not supported for comparison");
    }
  }


  @SuppressWarnings("rawtypes")
  private String valueToString(Comparable value, int columnType) {
    if (value == null) {
      return null;
    } else if (columnTypeIsNumeric(columnType)) {
      return ((BigDecimal)value).toPlainString();
    } else {
      return value.toString();
    }
  }


  /**
   * Produces a mismatch if the specified column index mismatches.
   */
  @SuppressWarnings("rawtypes")
  private Optional<ResultSetMismatch> valueCheck(ResultSet left, ResultSet right, String[] keys, int i, int columnType, MismatchType checkForMismatchType) throws SQLException {
    Comparable leftValue;
    Comparable rightValue;
    switch(checkForMismatchType) {
      case MISMATCH:
        leftValue = columnToValue(left, i, columnType);
        rightValue = columnToValue(right, i, columnType);
        return compareColumnValue(leftValue, rightValue, keys, i, columnType, checkForMismatchType);
      case MISSING_LEFT:
        rightValue = columnToValue(right, i, columnType);
        return Optional.of(new ResultSetMismatch(
          MISSING_LEFT, i,
          RECORD_NOT_PRESENT, valueToString(rightValue, columnType),
          keys
        ));
      case MISSING_RIGHT:
        leftValue = columnToValue(left, i, columnType);
        return Optional.of(new ResultSetMismatch(
          MISSING_RIGHT, i,
          valueToString(leftValue, columnType), RECORD_NOT_PRESENT,
          keys
        ));
      default:
        throw new IllegalStateException("Unknown mismatch type");
    }
  }


  /**
   * Verify the meta data of data sets matches, throw {@link IllegalStateException} if not.
   *
   */
  private void compareMetadata(ResultSetMetaData metadataLeft, ResultSetMetaData metadataRight) throws SQLException {
    if (metadataLeft.getColumnCount() != metadataRight.getColumnCount()) {
      throw new IllegalArgumentException("Column counts mismatch");
    }
    for (int i = 1; i <= metadataLeft.getColumnCount(); i++) {
      int left = metadataLeft.getColumnType(i);
      int right = metadataRight.getColumnType(i);
      if (columnTypeIsBoolean(left)) {
        if (!columnTypeIsBoolean(right)) throwTypeMismatch(metadataLeft, metadataRight, i);
        continue;
      }
      if (columnTypeIsDate(left)) {
        if (!columnTypeIsDate(right)) throwTypeMismatch(metadataLeft, metadataRight, i);
        continue;
      }
      if (columnTypeIsNumeric(left)) {
        if (!columnTypeIsNumeric(right)) throwTypeMismatch(metadataLeft, metadataRight, i);
        continue;
      }
      if (columnTypeIsString(left)) {
        if (!columnTypeIsString(right)) throwTypeMismatch(metadataLeft, metadataRight, i);
        continue;
      }
      throw new IllegalArgumentException(String.format(
        "Unknown column type for comparison: %s[%s(%d,%d)]",
        metadataLeft.getColumnLabel(i),
        metadataLeft.getColumnTypeName(i),
        metadataLeft.getPrecision(i),
        metadataLeft.getScale(i)
      ));
    }
  }


  private void throwTypeMismatch(ResultSetMetaData metadataLeft, ResultSetMetaData metadataRight, int i) throws SQLException {
    throw new IllegalArgumentException(
      String.format(
        "Column metadata does not match: %s[%s(%d,%d)] != %s[%s(%d,%d)]",
        metadataLeft.getColumnLabel(i),
        metadataLeft.getColumnTypeName(i),
        metadataLeft.getPrecision(i),
        metadataLeft.getScale(i),
        metadataRight.getColumnLabel(i),
        metadataRight.getColumnTypeName(i),
        metadataRight.getPrecision(i),
        metadataRight.getScale(i)
      )
    );
  }


  /**
   * Works out the mismatch type for a given key column over the two result sets.
   *
   */
  @SuppressWarnings({ "rawtypes" })
  private MismatchType compareKeyColumn(ResultSet left, ResultSet right, int keyCol, int columnType, boolean leftHasRow, boolean rightHasRow) throws SQLException {
    Optional<Comparable> leftValue = leftHasRow ? Optional.fromNullable(columnToValue(left, keyCol, columnType)) : null;
    Optional<Comparable> rightValue = rightHasRow ? Optional.fromNullable(columnToValue(right, keyCol, columnType)) : null;
    return compareKeyValue(leftValue, rightValue);
  }


  /**
   * Given key values from right and left data set, compare and record mismatch.
   *
   * @return type The mismatch type {@link MismatchType#MISSING_LEFT} or
   *         {@link MismatchType#MISSING_RIGHT}, null if value matches
   */
  @SuppressWarnings({ "rawtypes" })
  private MismatchType compareKeyValue(Optional<? extends Comparable> leftValue, Optional<? extends Comparable> rightValue) {
    if (leftValue == null && rightValue == null) {
      throw new IllegalStateException("Cannot compare two nonexistent keys.");
    }
    if (leftValue == null) {
      return MISSING_LEFT;
    }
    if (rightValue == null) {
      return MISSING_RIGHT;
    }
    if (!leftValue.isPresent() || !rightValue.isPresent()) {
      throw new IllegalStateException("Cannot compare null keys.");
    }

    int result = databaseEquivalentStringComparator.get().compare(leftValue.get(), rightValue.get());
    return result < 0 ? MISSING_RIGHT : result > 0 ? MISSING_LEFT : null;
  }


  /**
   * Given data values from right and left data sets, compare and record mismatch.
   *
   * @return An optional mismatch.
   */
  @SuppressWarnings({ "rawtypes" })
  private Optional<ResultSetMismatch> compareColumnValue(Comparable leftValue, Comparable rightValue, String[] keys, int columnIndex, int columnType, MismatchType mismatchTypeToRaise) {

    if (leftValue == null && rightValue == null) {
      return Optional.absent();
    }

    if (rightValue == null && leftValue != null) {
      return Optional.of(new ResultSetMismatch(mismatchTypeToRaise, columnIndex, valueToString(leftValue, columnType), null, keys));
    }

    if (rightValue != null && leftValue == null) {
      return Optional.of(new ResultSetMismatch(mismatchTypeToRaise, columnIndex, null, valueToString(rightValue, columnType), keys));
    }

    if ( databaseEquivalentStringComparator.get().compare(leftValue, rightValue) != 0) {
      return Optional.of(new ResultSetMismatch(
        mismatchTypeToRaise,
        columnIndex,
        valueToString(leftValue, columnType),
        valueToString(rightValue, columnType),
        keys
      ));
    }

    return Optional.absent();
  }


  /**
   * @return a list of non-Key column indexes
   */
  private List<Integer> getNonKeyColumns(ResultSetMetaData metaData, Set<Integer> keyCols) throws SQLException {
    List<Integer> valueCols = Lists.newArrayList();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (!keyCols.contains(i)) {
        valueCols.add(i);
      }
    }
    return valueCols;
  }


  private boolean columnTypeIsDate(int columnType) {
    return columnType == Types.DATE || columnType == Types.TIMESTAMP;
  }


  private boolean columnTypeIsString(int columnType) {
    return columnType == CHAR || columnType == VARCHAR || columnType == LONGVARCHAR || columnType == NCHAR
        || columnType == NVARCHAR || columnType == LONGNVARCHAR;
  }


  private boolean columnTypeIsBoolean(int columnType) {
    return columnType == Types.BOOLEAN || columnType == Types.BIT;
  }


  private boolean columnTypeIsJDBCDecimal(int columnType) {
    return columnType == DECIMAL || columnType == NUMERIC || columnType == DOUBLE || columnType == FLOAT || columnType == REAL;
  }


  private boolean columnTypeIsJDBCInteger(int columnType) {
    return columnType == INTEGER || columnType == BIGINT || columnType == SMALLINT || columnType == TINYINT;
  }


  private boolean columnTypeIsNumeric(int columnType) {
    return columnTypeIsJDBCInteger(columnType) || columnTypeIsJDBCDecimal(columnType);
  }


  /**
   * Implement this interface to handle reconciliation mismatch callbacks from
   * {@link ResultSetComparer#compare(int[], SelectStatement, SelectStatement, Connection, CompareCallback)}
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  public interface CompareCallback {
    /**
     * Handles a mismatch.
     *
     * @param mismatch The mismatch details.
     */
    void mismatch(ResultSetMismatch mismatch);
  }
}
