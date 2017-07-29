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

package org.alfasoftware.morf.metadata;

import static com.google.common.collect.Iterables.filter;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Class that validates {@link Schema} objects meet the rules.
 *
 * <p>This class is intended to impose Alfa development rules on database
 * attributes like name uniqueness, length and other conventions.</p>
 *
 * <p>Usage: create a new instance of this class and call {@link #validate(Schema)}.</p>
 *
 * <p>The current database rules are:</p>
 * <ul>
 * <li>An object (table, column, index) name must not be &gt; 30 characters long (an Oracle restriction)</li>
 * <li>An object name must not be an SQL reserved word</li>
 * <li>Table, column and index names must match [a-zA-Z][a-zA-Z0-9_]* (any letters or numbers or underscores, but must start with a letter)</li>
 * <li>Indexes may not be simply by 'id'. This would duplicate the primary key and is superfluous.</li>
 * <li>No duplicated indexes are allowed.</li>
 * </ul>
 *
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SchemaValidator {

  /**
   * Maximum length allowed for entity names.
   */
  private static final int MAX_LENGTH = 30;

  /**
   * All the words we can't use because they're special in some SQL dialect or other.
   *
   * <p>Note the following words are technically reserved, but we currently allow them because our database currently uses them:</p>
   * <ul>
   * <li>type</li>
   * <li>operation</li>
   * <li>method</li>
   * <li>language</li>
   * <li>location</li>
   * <li>year</li>
   * <li>day</li>
   * <li>security</li>
   * </ul>
   *
   * This site is quite useful: http://www.petefreitag.com/tools/sql_reserved_words_checker/
   */
  private static final Supplier<Set<String>> sqlReservedWords = Suppliers.memoize(new Supplier<Set<String>>() {
    @Override
    public Set<String> get() {
      StringWriter writer = new StringWriter();
      try {
        try (InputStream inputStream = getClass().getResourceAsStream("SQL_RESERVED_WORDS.txt")) {
          if (inputStream == null) {
            throw new RuntimeException("Could not find resource: [SQL_RESERVED_WORDS.txt] near [" + getClass() + "]");
          }
          IOUtils.copy(new InputStreamReader(inputStream, "UTF-8"), writer);
          HashSet<String> sqlReservedWords = Sets.newHashSet(Splitter.on("\r\n").split(writer.toString()));

          // temporary removal of words we currently have to allow
          sqlReservedWords.remove("TYPE");  // DB2
          sqlReservedWords.remove("OPERATION"); // DB2, SQL Server "future", PostGres
          sqlReservedWords.remove("METHOD"); // PostGres
          sqlReservedWords.remove("LANGUAGE"); // DB2, ODBC (?), SQL Server "future", PostGres
          sqlReservedWords.remove("LOCATION"); // PostGres
          sqlReservedWords.remove("YEAR"); // DB2, ODBC (?), SQL Server "future", PostGres
          sqlReservedWords.remove("DAY"); // DB2, ODBC (?), SQL Server "future", PostGres
          sqlReservedWords.remove("SECURITY"); // DB2, PostGres

          return ImmutableSet.copyOf(sqlReservedWords);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to load [SQL_RESERVED_WORDS.txt]", e);
      }
    }
  });

  private static final Function<Column, String> COLUMN_TO_NAME = new Function<Column, String> () {
    @Override
    public String apply(Column input) {
      return input.getName();
    }
  };

  private static final Function<AliasedField, String> FIELD_TO_NAME = new Function<AliasedField, String> () {
    @Override
    public String apply(AliasedField input) {
      return input.getImpliedName();
    }
  };


  /**
   * The regular expression that defines a valid index name (underscores allowed)
   */
  private static final Pattern validNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]*"); // <-- I know this is the same as \w, but this is clearer


  /**
   * The list of all failures
   */
  private final List<String> validationFailures = new LinkedList<>();


  /**
   * Validates a {@link Schema} meets the rules.
   *
   * @param schema the {@link Schema} to validate.
   */
  public void validate(Schema schema) {
    for (Table table : schema.tables()) {
      validateTable(table);
    }
    for (View view : schema.views()) {
      validateView(view);
    }

    checkForValidationErrors();
  }


  /**
   * Validate a {@link Table} meets the rules
   *
   * @param table The {@link Table} to validate
   */
  public void validate(Table table) {
    validateTable(table);

    checkForValidationErrors();
  }


  /**
   * Validate a {@link View} meets the rules
   *
   * @param view The {@link View} to validate
   */
  public void validate(View view) {
    validateView(view);

    checkForValidationErrors();
  }


  /**
   * <p>
   * Method to establish if a name matches an allowed pattern of characters to
   * follow the correct naming convention.
   * </p>
   * <p>
   * The name must:
   * </p>
   * <ul>
   * <li>begin with an alphabetic character [a-zA-Z]</li>
   * <li>only contain alphanumeric characters or underscore [a-zA-Z0-9_]</li>
   * </ul>
   *
   * @param name The string to check if it follows the correct naming convention
   * @return true if the name is valid otherwise false
   */
  boolean isNameConventional(String name){
    return validNamePattern.matcher(name).matches();
  }


  /**
   * Method to establish if a given string is an SQL Reserved Word
   *
   * @param word the string to establish if its a SQL Reserved Word
   * @return true if its a SQL Reserved Word otherwise false.
   */
  boolean isSQLReservedWord(String word){
    return sqlReservedWords.get().contains(word.toUpperCase());
  }


  /**
   * Method to establish if a given string representing an Entity name is within the allowed length of charchaters.
   *
   * @see #MAX_LENGTH
   *
   * @param word the string to establish if its within the allowed length
   * @return true if its within the allowed length otherwise false.
   */
  boolean isEntityNameLengthValid(String name){
    return name.length() <= MAX_LENGTH;
  }


  /**
   * Check whether any errors have been added
   */
  private void checkForValidationErrors() {
    // check for errors
    if (!validationFailures.isEmpty()) {
      StringBuilder message = new StringBuilder("Schema validation failures:");
      for (String failure : validationFailures) {
        message.append("\n");
        message.append(failure);
      }
      throw new RuntimeException(message.toString());
    }
  }


  /**
   * Validates a {@link Table} meets the rules.
   *
   * @param table The {@link Table} to validate.
   */
  private void validateTable(Table table) {
    validateName(table.getName());

    validateIndices(table);
    validateColumns(table);
  }


  /**
   * Validates a {@link Table} meets the rules.
   *
   * @param table The {@link Table} to validate.
   */
  private void validateView(View view) {
    validateName(view.getName());
    if (view.knowsSelectStatement()) {
      validateColumnNames(FluentIterable.from(view.getSelectStatement().getFields()).transform(FIELD_TO_NAME).toList(), view.getName());
    }
  }


  /**
   * Validates the basic naming rules for a database object (currently a table or view).
   */
  private void validateName(String tableOrViewName) {
    if (!isEntityNameLengthValid(tableOrViewName)) {
      validationFailures.add("Name of table or view [" + tableOrViewName + "] is not allowed - it is over " + MAX_LENGTH + " characters long");
    }

    if (isSQLReservedWord(tableOrViewName)) {
      validationFailures.add("Name of table or view [" + tableOrViewName + "] is not allowed - it is an SQL reserved word");
    }

    if (!isNameConventional(tableOrViewName)) {
      validationFailures.add("Name of table or view [" + tableOrViewName + "] is not allowed - it must match " + validNamePattern.toString());
    }
  }


  /**
   * Validates a {@link Table}'s {@link Column}s meet the rules.
   *
   * @param table The {@link Table} on which to validate columns.
   */
  private void validateColumns(Table table) {
    validateColumnNames(FluentIterable.from(table.columns()).transform(COLUMN_TO_NAME).toList(), table.getName());

    for (Column column : table.columns()) {
      if (column.getType().hasWidth() && column.getWidth() == 0) {
        validationFailures.add("Column [" + column.getName() + "] on table [" + table.getName() + "] is not allowed - column data type [" + column.getType() + "] requires width to be specified");
      }

      if (column.isPrimaryKey() && column.isNullable()) {
        validationFailures.add("Column [" + column.getName() + "] on table [" + table.getName() + "] is both nullable and in the primary key. This is not permitted.");
      }
    }
  }

  /**
   * Validates a {@link Table}'s {@link Column}s meet the rules.
   *
   * @param table The {@link Table} on which to validate columns.
   */
  private void validateColumnNames(Collection<String> columnNames, String tableOrViewName) {
    for (String columnName : columnNames) {
      if (!isEntityNameLengthValid(columnName)) {
        validationFailures.add("Name of column [" + columnName + "] on [" + tableOrViewName + "] is not allowed - it is over " + MAX_LENGTH + " characters");
      }

      if (isSQLReservedWord(columnName)) {
        validationFailures.add("Name of column [" + columnName + "] on [" + tableOrViewName + "] is not allowed - it is an SQL reserved word");
      }

      if (!isNameConventional(columnName)) {
        validationFailures.add("Name of column [" + columnName + "] on [" + tableOrViewName + "] is not allowed - it must match " + validNamePattern.toString());
      }
    }
  }


  /**
   * Validates a {@link Table}'s {@link Index}s meet the rules.
   *
   * @param table The {@link Table} on which to validate indexes.
   */
  private void validateIndices(Table table) {

    Map<IndexSignature, Index> indexesBySignature = new HashMap<>();

    // -- First add the 'PRIMARY' index which is always present in the database
    //    for the column annotated @Id or @GeneratedId...
    //
    Iterable<Column> primaryKeyColumns = filter(table.columns(), new Predicate<Column>() {
      @Override public boolean apply(Column input) {
        return input.isPrimaryKey();
      }
    });

    Iterable<String> primaryKeyColumnNames = Iterables.transform(
      primaryKeyColumns,
      new Function<Column, String>() {
        @Override
        public String apply(Column input) {
          return input.getName();
        }
    });

    Index primaryKeyIndex = index("PRIMARY")
      .unique()
      .columns(primaryKeyColumnNames);

    indexesBySignature.put(new IndexSignature(primaryKeyIndex), primaryKeyIndex);

    for (Index index : table.indexes()) {
      if (!isEntityNameLengthValid(index.getName())) {
        validationFailures.add("Name of index [" + index.getName() + "] on table [" + table.getName() + "] is not allowed - it is over " + MAX_LENGTH + " characters long");
      }

      if (isSQLReservedWord(index.getName())) {
        validationFailures.add("Name of index [" + index.getName() + "] on table [" + table.getName() + "] is not allowed - it is an SQL reserved word");
      }

      if (!isNameConventional(index.getName())) {
        validationFailures.add("Name of index [" + index.getName() + "] on table [" + table.getName() + "] is not allowed - it must match " + validNamePattern.toString());
      }

      // validate that there isn't an index by "id", which people have added in the past. This isn't necessary since id is the primary key, and in
      // fact it breaks Oracle.
      if (index.columnNames().equals(Arrays.asList("id"))) {
        validationFailures.add("Index [" + index.getName() + "] on table [" + table.getName() + "] is not allowed - indexes by 'id' only are superfluous since 'id' is the primary key.");
      }

      // validate that there aren't any duplicate indexes
      Index other = indexesBySignature.put(new IndexSignature(index), index);

      if (other != null) {
        validationFailures.add("Indexes [" + index.getName() + "] and [" + other.getName() + "] on table [" + table.getName() + "] are not allowed - one is a duplicate of the other");
      }
    }
  }


  /**
   * Implementation of hashCode and equals for index comparison.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private class IndexSignature {
    /***/
    private final Index index;


    /**
     * @param index The {@link Index} to wrap.
     */
    public IndexSignature(Index index) {
      super();
      this.index = index;
    }


    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      return index.columnNames().hashCode() + Boolean.valueOf(index.isUnique()).hashCode();
    }


    /**
     * {@inheritDoc}
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IndexSignature)) return false;

      IndexSignature other = (IndexSignature) obj;

      return ObjectUtils.equals(other.index.columnNames(), index.columnNames()) && other.index.isUnique() == index.isUnique();
    }
  }
}