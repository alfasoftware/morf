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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
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
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests that the string generation for human readable upgrade paths is correct.
 * <p>
 * Examples of readable forms of common data upgrade operations are given in WEB-29077. Some
 * of the tests here assert that the implementation produces text matching these examples.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestHumanReadableStatementHelper {


  /**
   * Test for {@link HumanReadableStatementHelper#generateChangePrimaryKeyColumnsString(String, List, List)}.
   */
  @Test
  public void testChangePrimaryKeyColumnsGeneration() {
    List<String> fromList = ImmutableList.of("column1", "column2", "column3");
    List<String> newList = ImmutableList.of("column4", "column5");

    assertEquals("Should have the correct text - Simple", "Change primary key columns on my_table from (column1, column2, column3) to (column4, column5)", HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString("my_table", fromList, newList));
  }


  /**
   * Tests the generation of "Add Column" text.
   */
  @Test
  public void testAddColumnGeneration() {
    assertEquals("Should have correct text - NON NULL STRING", "Add a non-null column to my_table called newColumn [STRING(10)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.STRING, 10)));
    assertEquals("Should have correct text - NULLABLE STRING", "Add a nullable column to my_table called newColumn [STRING(10)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.STRING, 10).nullable()));
    assertEquals("Should have correct text - NON NULL DECIMAL", "Add a non-null column to my_table called newColumn [DECIMAL(9,5)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.DECIMAL, 9, 5)));
    assertEquals("Should have correct text - NULLABLE DECIMAL", "Add a nullable column to my_table called newColumn [DECIMAL(9,5)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.DECIMAL, 9, 5).nullable()));
    assertEquals("Should have correct text - NON NULL DECIMAL 2", "Add a non-null column to my_table called newColumn [DECIMAL(9,0)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.DECIMAL, 9, 0)));
    assertEquals("Should have correct text - NULLABLE DECIMAL 2", "Add a nullable column to my_table called newColumn [DECIMAL(9,0)]", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.DECIMAL, 9, 0).nullable()));
    assertEquals("Should have correct text - NON NULL WITH DEFAULT", "Add a non-null column to my_table called newColumn [STRING(1)], set to 'N'", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.STRING, 1).defaultValue("N")));
    assertEquals("Should have correct text - NON NULL POPULATED ", "Add a non-null column to my_table called newColumn [STRING(1)], set to 'N'", HumanReadableStatementHelper.generateAddColumnString("my_table", column("newColumn", DataType.STRING, 1), new FieldLiteral("N")));
  }


  /**
   * Tests the generation of "Add Index" text.
   */
  @Test
  public void testAddIndexGeneration() {
    assertEquals("Should have correct text - Unique", "Add unique index called my_table_1 to my_table", HumanReadableStatementHelper.generateAddIndexString("my_table", index("my_table_1").unique().columns("columnOne", "columnTwo")));
    assertEquals("Should have correct text - Unique", "Add non-unique index called my_table_1 to my_table", HumanReadableStatementHelper.generateAddIndexString("my_table", index("my_table_1").columns("columnOne", "columnTwo")));
  }


  /**
   * Tests the generation of "Add Table" text.
   */
  @Test
  public void testAddTableOneColumnNoIndex() {
    Table newTable = table("new_table").columns(
        column("column_one", DataType.STRING, 10)
      );

    assertEquals(
      String.format("Create table new_table with 1 column and no indexes" +
      "%n    - A non-null column called column_one [STRING(10)]"),
      HumanReadableStatementHelper.generateAddTableString(newTable)
    );
  }


  @Test
  public void testAddTableFromSelectSingleColumn() {
    Table newTable = table("new_table").columns(
        column("column_one", DataType.STRING, 10)
      );

    assertEquals(
      String.format("Create table new_table with 1 column and no indexes" +
      "%n    - A non-null column called column_one [STRING(10)] from foo from Wherever where bar is 1"),
      HumanReadableStatementHelper.generateAddTableFromString(newTable, select(field("foo")).from("Wherever").where(field("bar").eq(1)))
    );
  }


  @Test
  public void testAddTableFromSelectMultiColumn() {
    Table newTable = table("new_table").columns(
      column("column_one", DataType.STRING, 10),
      column("column_two", DataType.STRING, 10)
    );

    assertEquals(
      String.format("Create table new_table with 2 columns and no indexes" +
      "%n    - A non-null column called column_one [STRING(10)]" +
      "%n    - A non-null column called column_two [STRING(10)] from foo, bar from Wherever where bar is 1"),
      HumanReadableStatementHelper.generateAddTableFromString(newTable, select(field("foo"), field("bar")).from("Wherever").where(field("bar").eq(1)))
    );
  }


  @Test
  public void testAddTableOneColumnOneIndex() {
    Table newTable = table("new_table").columns(
      column("column_one", DataType.STRING, 10)
    ).indexes(
      index("new_table_1").unique().columns("column_one")
    );

    assertEquals(
      String.format("Create table new_table with 1 column and 1 index" +
      "%n    - A non-null column called column_one [STRING(10)]"),
      HumanReadableStatementHelper.generateAddTableString(newTable)
    );
  }


  @Test
  public void testAddTableTwoColumnsNoIndex() {
    Table newTable = table("new_table").columns(
        column("column_one", DataType.STRING, 10),
        column("column_two", DataType.DECIMAL, 9, 5).nullable()
      );

    assertEquals(
      String.format("Create table new_table with 2 columns and no indexes" +
      "%n    - A non-null column called column_one [STRING(10)]" +
      "%n    - A nullable column called column_two [DECIMAL(9,5)]"),
      HumanReadableStatementHelper.generateAddTableString(newTable)
    );
  }


  @Test
  public void testAddTableTwoColumnsOneIndex() {
    Table newTable = table("new_table").columns(
        column("column_one", DataType.STRING, 10),
        column("column_two", DataType.DECIMAL, 9, 5).nullable()
      ).indexes(
        index("new_table_1").unique().columns("column_one")
      );

    assertEquals(
        String.format("Create table new_table with 2 columns and 1 index" +
        "%n    - A non-null column called column_one [STRING(10)]" +
        "%n    - A nullable column called column_two [DECIMAL(9,5)]"),
        HumanReadableStatementHelper.generateAddTableString(newTable)
      );
  }


  @Test
  public void testAddTableTwoColumnsTwoIndexes() {
    Table newTable = table("new_table").columns(
        column("column_one", DataType.STRING, 10),
        column("column_two", DataType.DECIMAL, 9, 5).nullable()
      ).indexes(
      index("new_table_1").unique().columns("column_one"),
        index("new_table_2").columns("column_two")
      );

    assertEquals(
      String.format("Create table new_table with 2 columns and 2 indexes" +
      "%n    - A non-null column called column_one [STRING(10)]" +
      "%n    - A nullable column called column_two [DECIMAL(9,5)]"),
      HumanReadableStatementHelper.generateAddTableString(newTable)
    );
  }


  @Test
  public void testAddTableTenColumns() {
    Table newTable = table("new_table").columns(
      column("column_one", DataType.STRING, 10).defaultValue("Foo"),
      column("column_two", DataType.STRING, 1).nullable(),
      column("column_three", DataType.DECIMAL, 9, 5),
      column("column_four", DataType.DECIMAL, 15, 0).nullable(),
      column("column_five", DataType.INTEGER).defaultValue("42"),
      column("column_six", DataType.INTEGER).nullable(),
      column("column_seven", DataType.BIG_INTEGER),
      column("column_eight", DataType.BIG_INTEGER).nullable(),
      column("column_bool", DataType.BOOLEAN),
      column("column_nullbool", DataType.BOOLEAN).nullable(),
      column("column_date", DataType.DATE),
      column("column_nulldate", DataType.DATE).nullable(),
      column("column_clob", DataType.CLOB),
      column("column_nullclob", DataType.CLOB).nullable(),
      column("column_nine", DataType.BLOB),
      column("column_ten", DataType.BLOB).nullable()
    );

    assertEquals(
      String.format("Create table new_table with 16 columns and no indexes" +
          "%n    - A non-null column called column_one [STRING(10)], set to 'Foo'" +
          "%n    - A nullable column called column_two [STRING(1)]" +
          "%n    - A non-null column called column_three [DECIMAL(9,5)]" +
          "%n    - A nullable column called column_four [DECIMAL(15,0)]" +
          "%n    - A non-null column called column_five [INTEGER], set to 42" +
          "%n    - A nullable column called column_six [INTEGER]" +
          "%n    - A non-null column called column_seven [BIG_INTEGER]" +
          "%n    - A nullable column called column_eight [BIG_INTEGER]" +
          "%n    - A non-null column called column_bool [BOOLEAN]" +
          "%n    - A nullable column called column_nullbool [BOOLEAN]" +
          "%n    - A non-null column called column_date [DATE]" +
          "%n    - A nullable column called column_nulldate [DATE]" +
          "%n    - A non-null column called column_clob [CLOB]" +
          "%n    - A nullable column called column_nullclob [CLOB]" +
          "%n    - A non-null column called column_nine [BLOB]" +
          "%n    - A nullable column called column_ten [BLOB]"),
      HumanReadableStatementHelper.generateAddTableString(newTable)
    );
  }


  /**
   * Tests the generation of "Change Column" text.
   */
  @Test
  public void testChangeColumnGeneration() {
    assertEquals("Should have the correct text - Lengthen String", "Change column columnOne on my_table from non-null STRING(10) to non-null STRING(20)", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnOne", DataType.STRING, 20)));
    assertEquals("Should have the correct text - Change nullability String", "Change column columnOne on my_table from non-null STRING(10) to nullable STRING(10)", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnOne", DataType.STRING, 10).nullable()));
    assertEquals("Should have the correct text - Change column type", "Change column columnOne on my_table from non-null STRING(10) to non-null DECIMAL(9,5)", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnOne", DataType.DECIMAL, 9, 5)));
  }


  /**
   * Tests the generation of "Rename Column" text.
   */
  @Test
  public void testRenameColumnGeneration() {
    assertEquals("Should have the correct text - Rename no length change", "Rename non-null column columnOne [STRING(10)] on my_table to non-null columnTwo [STRING(10)]", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnTwo", DataType.STRING, 10)));
    assertEquals("Should have the correct text - Rename with length change", "Rename non-null column columnOne [STRING(10)] on my_table to non-null columnTwo [STRING(20)]", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnTwo", DataType.STRING, 20)));
    assertEquals("Should have the correct text - Rename with null change", "Rename non-null column columnOne [STRING(10)] on my_table to nullable columnTwo [STRING(10)]", HumanReadableStatementHelper.generateChangeColumnString("my_table", column("columnOne", DataType.STRING, 10), column("columnTwo", DataType.STRING, 10).nullable()));
  }


  /**
   * Tests the generation of "Change Index" text.
   */
  @Test
  public void testChangeIndexGeneration() {
    assertEquals("Should have the correct text - Change to non-unique", "Change unique index my_table_1 on my_table to be non-unique", HumanReadableStatementHelper.generateChangeIndexString("my_table", index("my_table_1").unique().columns("columnOne", "columnTwo"), index("my_table_1").columns("columnOne", "columnTwo")));
    assertEquals("Should have the correct text - Change to unique", "Change non-unique index my_table_1 on my_table to be unique", HumanReadableStatementHelper.generateChangeIndexString("my_table", index("my_table_1").columns("columnOne", "columnTwo"), index("my_table_1").unique().columns("columnOne", "columnTwo")));
    assertEquals("Should have the correct text - No change", "Change unique index my_table_1 on my_table", HumanReadableStatementHelper.generateChangeIndexString("my_table", index("my_table_1").unique().columns("columnOne", "columnTwo"), index("my_table_1").unique().columns("columnOne", "columnTwo")));
  }


  /**
   * Tests the generation of "Remove Column" text.
   */
  @Test
  public void testRemoveColumnGeneration() {
    assertEquals("Should have the correct text - Simple", "Remove column columnOne from my_table", HumanReadableStatementHelper.generateRemoveColumnString("my_table", column("columnOne", DataType.STRING, 10)));
  }


  /**
   * Tests the generation of "Remove Index" text.
   */
  @Test
  public void testRemoveIndexGeneration() {
    assertEquals("Should have the correct text - Simple", "Remove index my_table_1 from my_table", HumanReadableStatementHelper.generateRemoveIndexString("my_table", index("my_table_1").unique().columns("columnOne", "columnTwo")));
  }


  /**
   * Tests the generation of "Remove Table" text.
   */
  @Test
  public void testRemoveTableGeneration() {
    assertEquals("Should have the correct text - Simple", "Remove table my_table", HumanReadableStatementHelper.generateRemoveTableString(table("my_table")));
  }


  /**
   * Tests the generation of "Rename table" test.
   */
  @Test
  public void testRenameTableGeneration() {
    assertEquals("Should have the correct text - Simple", "Rename table my_table to your_table", HumanReadableStatementHelper.generateRenameTableString("my_table", "your_table"));
  }


  /**
   * Tests the generation of "Rename index" test.
   */
  @Test
  public void testRenameIndexGeneration() {
    assertEquals("Should have the correct text - Simple",
      "Rename index SomeBadlyNamedTable_1 on SomeBadlyNamedTable to ABetterNamedTable_1",
      HumanReadableStatementHelper.generateRenameIndexString("SomeBadlyNamedTable", "SomeBadlyNamedTable_1", "ABetterNamedTable_1"));
  }


  /**
   * Tests the generation of insert statement text for a single record operation.
   */
  @Test
  public void testSingleRecordInsertStatementGeneration() {
    final InsertStatement statement = new InsertStatement()
      .into(new TableReference("TransactionCode"))
      .values(new FieldLiteral("Z02").as("transactionCode"), new FieldLiteral("Example Description").as("transactionDescription"));

    assertEquals("Data upgrade description incorrect",
      String.format("Add record into TransactionCode:"
        + "%n    - Set transactionCode to 'Z02'"
        + "%n    - Set transactionDescription to 'Example Description'"),
      HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of insert statement text for multiple record operation.
   */
  @Test
  public void testMultipleRecordInsertStatementGeneration() {
    final InsertStatement statement = new InsertStatement()
      .into(new TableReference("AgrmmentBllngAddrssFrmTmp"))
      .from(
        new SelectStatement(new FieldReference("agreementNumber"), new FieldReference("billingAddressFrameAgreement"))
          .from("AgreementbillingAddressFrame")
          .where(Criterion.eq(new FieldReference("extracted"), "N")));

  assertEquals("Data upgrade description incorrect",
          "Add records into AgrmmentBllngAddrssFrmTmp: agreementNumber and billingAddressFrameAgreement from AgreementbillingAddressFrame where extracted is 'N'",
    HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of update statement text when there is no selection criteria.
   */
  @Test
  public void testUpdateWithoutCriterionStatementGeneration() {
    final UpdateStatement statement = new UpdateStatement(new TableReference("GenericGlPosting"))
      .set(new FieldReference("currencyConversionDate").as("valueDate"));

    assertEquals("Data upgrade description incorrect",
      String.format("Update records in GenericGlPosting"
        + "%n    - Set valueDate to currencyConversionDate's value"),
      HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of update statement text when there is selection criteria.
   */
  @Test
  public void testUpdateWithCriterionStatementGeneration() {
    final UpdateStatement statement = new UpdateStatement(new TableReference("Agreement"))
      .set(new NullFieldLiteral().as("frequency"), new NullFieldLiteral().as("unit"))
      .where(Criterion.and(
        Criterion.isNotNull(new FieldReference("documentCode")),
        Criterion.or(
          Criterion.isNull(new FieldReference("productCode")),
          Criterion.not(Criterion.in(new FieldReference("documentCode"), new SelectStatement(new FieldReference("documentCode")).from("Document"))))));

    assertEquals("Data upgrade description incorrect",
      String.format("Update Agreement where documentCode is not null and (productCode is null or documentCode is not in Document)"
        + "%n    - Set frequency to null"
        + "%n    - Set unit to null"),
            HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of delete statement text with no selection criteria.
   */
  @Test
  public void testDeleteWithoutCriterionStatementGeneration() {
    final DeleteStatement statement = new DeleteStatement(new TableReference("TransactionCode"));

    assertEquals("Data upgrade description incorrect",
            "Delete all records in TransactionCode",
            HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of delete statement text with selection criteria.
   */
  @Test
  public void testDeleteWithCriterionStatementGeneration() {
    final DeleteStatement statement = new DeleteStatement(new TableReference("TransactionCode"))
      .where(Criterion.in(new FieldReference("transactionCode"), "Z02", "Z03"));

    assertEquals("Data upgrade description incorrect",
            "Delete records in TransactionCode where transactionCode is in ('Z02', 'Z03')",
            HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of truncate statement text.
   */
  @Test
  public void testTruncateStatementGeneration() {
    final TruncateStatement statement = new TruncateStatement(new TableReference("TransactionCode"));

    assertEquals("Data upgrade description incorrect",
            "Delete all records in TransactionCode",
            HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of merge statement text when there is no selection criteria.
   */
  @Test
  public void testMergeWithoutCriterionStatementGeneration() {
    final MergeStatement statement = new MergeStatement()
      .from(new SelectStatement().from("AgreementbillingAddressFrame"))
      .into(new TableReference("AgrmmentBllngAddrssFrmTmp"));

    assertEquals("Data upgrade description incorrect",
            "Merge records into AgrmmentBllngAddrssFrmTmp from AgreementbillingAddressFrame",
            HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of merge statement text when there is a selection criteria.
   */
  @Test
  public void testMergeWithCriterionStatementGeneration() {
    final MergeStatement statement = new MergeStatement()
    .from(new SelectStatement().from("AgreementbillingAddressFrame").where(Criterion.eq(new FieldReference("extracted"), "N")))
    .into(new TableReference("AgrmmentBllngAddrssFrmTmp"));

  assertEquals("Data upgrade description incorrect",
          "Merge records into AgrmmentBllngAddrssFrmTmp from AgreementbillingAddressFrame where extracted is 'N'",
          HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of merge statement text when there is no source table.
   */
  @Test
  public void testMergeWithNoSourceTable() {
    final MergeStatement statement = new MergeStatement()
      .from(new SelectStatement(new FieldLiteral("Z02").as("transactionCode"), new FieldLiteral("Modified example description").as("transactionDescription")))
      .into(new TableReference("TransactionCode"));

    assertEquals("Data upgrade description incorrect",
      String.format("Merge record into TransactionCode:"
        + "%n    - Set transactionCode to 'Z02'"
        + "%n    - Set transactionDescription to 'Modified example description'"),
        HumanReadableStatementHelper.generateDataUpgradeString(statement, null));
  }


  /**
   * Tests the generation of raw sql statement text.
   */
  @Test
  public void testRawSQLStatementGeneration() {
    final PortableSqlStatement statement = new PortableSqlStatement()
      .add("FOO", String.format("mysql statements%non multiple lines%nend"))
      .add("BAR", "oracle statements");

    // Request specific dialect
    final String oracleResult = HumanReadableStatementHelper.generateDataUpgradeString(statement, "BAR");
    final String expectedOracleResult = String.format("Run the following raw SQL statement"
        + "%n    - oracle statements");
    assertEquals("Data upgrade description incorrect", expectedOracleResult, oracleResult);

    // Request specific dialect
    final String mySqlResult = HumanReadableStatementHelper.generateDataUpgradeString(statement, "FOO");
    final String expectedMySqlResult = String.format("Run the following raw SQL statement"
        + "%n    - mysql statements"
        + "%n      on multiple lines"
        + "%n      end");
    assertEquals("Data upgrade description incorrect", expectedMySqlResult, mySqlResult);

    // No dialect specified; either string is acceptable
    final String arbitraryResult = HumanReadableStatementHelper.generateDataUpgradeString(statement, null);
    assertTrue("Data upgrade description incorrect", expectedOracleResult.equals(arbitraryResult) || expectedMySqlResult.equals(arbitraryResult));
  }


  /**
   * Tests the construction of criterion clause strings.
   */
  @Test
  public void testCriteriaStrings() {
    final Criterion eq = Criterion.eq(new FieldReference("a"), 42);
    final Criterion exists = Criterion.exists(new SelectStatement().from("Bar").where(eq));
    final Criterion gt = Criterion.greaterThan(new FieldReference("b"), 42);
    final Criterion gte = Criterion.greaterThanOrEqualTo(new FieldReference("c"), 42);
    final Criterion inValues = Criterion.in(new FieldReference("d"), 1, 2, 3);
    final Criterion inSelect = Criterion.in(new FieldReference("e"), new SelectStatement(new FieldReference("foo")).from("Bar"));
    final Criterion inList = Criterion.in(new FieldReference("f"), Lists.newArrayList(1, 2, 3));
    final Criterion isNotNull = Criterion.isNotNull(new FieldReference("g"));
    final Criterion isNull = Criterion.isNull(new FieldReference("h"));
    final Criterion like = Criterion.like(new FieldReference("i"), "%x%");
    final Criterion lt = Criterion.lessThan(new FieldReference("j"), 42);
    final Criterion lte = Criterion.lessThanOrEqualTo(new FieldReference("k"), 42);
    final Criterion neq = Criterion.neq(new FieldReference("l"), 42);
    final Criterion and = Criterion.and(eq, inSelect, inValues, isNull);
    final Criterion or = Criterion.or(eq, and, gt, inSelect);

    assertEquals("EQ", "a is 42", HumanReadableStatementHelper.generateCriterionString(eq));
    assertEquals("EXISTS", "exists Bar where a is 42", HumanReadableStatementHelper.generateCriterionString(exists));
    assertEquals("GT", "b is greater than 42", HumanReadableStatementHelper.generateCriterionString(gt));
    assertEquals("GTE", "c is greater than or equal to 42", HumanReadableStatementHelper.generateCriterionString(gte));
    assertEquals("IN", "d is in (1, 2, 3)", HumanReadableStatementHelper.generateCriterionString(inValues));
    assertEquals("IN", "e is in Bar", HumanReadableStatementHelper.generateCriterionString(inSelect));
    assertEquals("IN", "f is in (1, 2, 3)", HumanReadableStatementHelper.generateCriterionString(inList));
    assertEquals("ISNOTNULL", "g is not null", HumanReadableStatementHelper.generateCriterionString(isNotNull));
    assertEquals("ISNULL", "h is null", HumanReadableStatementHelper.generateCriterionString(isNull));
    assertEquals("LIKE", "i is like '%x%'", HumanReadableStatementHelper.generateCriterionString(like));
    assertEquals("LT", "j is less than 42", HumanReadableStatementHelper.generateCriterionString(lt));
    assertEquals("LTE", "k is less than or equal to 42", HumanReadableStatementHelper.generateCriterionString(lte));
    assertEquals("NEQ", "l is not 42", HumanReadableStatementHelper.generateCriterionString(neq));
    assertEquals("AND", "a is 42 and e is in Bar and d is in (1, 2, 3) and h is null", HumanReadableStatementHelper.generateCriterionString(and));
    assertEquals("OR", "a is 42 or (a is 42 and e is in Bar and d is in (1, 2, 3) and h is null) or b is greater than 42 or e is in Bar",
      HumanReadableStatementHelper.generateCriterionString(or));

    assertEquals("!EQ", "a is not 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(eq)));
    assertEquals("!EXISTS", "not exists Bar where a is 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(exists)));
    assertEquals("!GT", "b is less than or equal to 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(gt)));
    assertEquals("!GTE", "c is less than 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(gte)));
    assertEquals("!IN", "d is not in (1, 2, 3)", HumanReadableStatementHelper.generateCriterionString(Criterion.not(inValues)));
    assertEquals("!IN", "e is not in Bar", HumanReadableStatementHelper.generateCriterionString(Criterion.not(inSelect)));
    assertEquals("!IN", "f is not in (1, 2, 3)", HumanReadableStatementHelper.generateCriterionString(Criterion.not(inList)));
    assertEquals("!ISNOTNULL", "g is null", HumanReadableStatementHelper.generateCriterionString(Criterion.not(isNotNull)));
    assertEquals("!ISNULL", "h is not null", HumanReadableStatementHelper.generateCriterionString(Criterion.not(isNull)));
    assertEquals("!LIKE", "i is not like '%x%'", HumanReadableStatementHelper.generateCriterionString(Criterion.not(like)));
    assertEquals("!LT", "j is greater than or equal to 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(lt)));
    assertEquals("!LTE", "k is greater than 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(lte)));
    assertEquals("!NEQ", "l is 42", HumanReadableStatementHelper.generateCriterionString(Criterion.not(neq)));
    assertEquals("!AND", "a is not 42 or e is not in Bar or d is not in (1, 2, 3) or h is not null", HumanReadableStatementHelper.generateCriterionString(Criterion.not(and)));
    assertEquals("!OR", "a is not 42 and (a is not 42 or e is not in Bar or d is not in (1, 2, 3) or h is not null) and b is less than or equal to 42 and e is not in Bar",
      HumanReadableStatementHelper.generateCriterionString(Criterion.not(or)));
  }


  /**
   * Tests handling of {@link CaseStatement}.
   */
  @Test
  public void testCaseStatementField() {
    final NullFieldLiteral defaultValue = new NullFieldLiteral();
    final WhenCondition when1 = new WhenCondition(Criterion.eq(new FieldReference("foo"), "Y"), new FieldLiteral(1234));
    final WhenCondition when2 = new WhenCondition(Criterion.eq(new FieldReference("foo"), "N"), new FieldLiteral(5678));
    CaseStatement field = new CaseStatement(defaultValue, when1, when2);

    field = (CaseStatement)field.as("bar");

    assertEquals("Incorrect strings generated",
      String.format("%n    - If foo is 'Y' then set bar to 1234" +
          "%n    - If foo is 'N' then set bar to 5678" +
          "%n    - Otherwise set bar to null"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link Cast}.
   */
  @Test
  public void testCastField() {
     Cast field1 = new Cast(new FieldReference("foo"), DataType.DECIMAL, 10);
     field1 = field1.as("bar");
     Cast field2 = new Cast(new FieldLiteral("1234"), DataType.DECIMAL, 10);
     field2 = field2.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to foo's value"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field1));
    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to 1234"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field2));
  }


  /**
   * Tests handling of {@link ConcatenatedField}.
   */
  @Test
  public void testConcatenatedField() {

    ConcatenatedField field = new ConcatenatedField(new FieldReference("foo"), new FieldLiteral(1234), new FieldLiteral("bar"));
    field =  (ConcatenatedField)field.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to the concatenation of foo, 1234 and 'bar'"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link FieldFromSelect}.
   */
  @Test
  public void testFieldFromSelect() {
    final SelectStatement select = new SelectStatement(new FieldReference("foo")).from("ExampleData");
    FieldFromSelect field = new FieldFromSelect(select);
    field = (FieldFromSelect)field.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to foo from ExampleData"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link FieldFromSelect} with a table join.
   */
  @Test
  public void testFieldFromSelectWithJoin() {
    final SelectStatement select = new SelectStatement(new FieldReference("foo")).from("ExampleData").innerJoin(new TableReference("OtherTable"), Criterion.eq(new FieldReference("x"), new FieldReference("y")));
    FieldFromSelect field = new FieldFromSelect(select);
    field = (FieldFromSelect)field.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to foo from ExampleData and OtherTable, joined on x is y"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link FieldFromSelect} with a table join and trailing where.
   */
  @Test
  public void testFieldFromSelectWithJoinAndWhere() {
    final SelectStatement select = new SelectStatement(new FieldReference("foo")).from("ExampleData")
        .innerJoin(new TableReference("OtherTable"), Criterion.eq(new FieldReference("x"), new FieldReference("y")))
        .where(Criterion.eq(new FieldReference("z"), 1));
    FieldFromSelect field = new FieldFromSelect(select);
    field = (FieldFromSelect)field.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to foo from ExampleData and OtherTable, joined on x is y, where z is 1"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link FieldFromSelectFirst}.
   */
  @Test
  public void testFieldFromSelectFirst() {
    final SelectFirstStatement select = new SelectFirstStatement(new FieldReference("foo")).from("ExampleData").orderBy(new FieldReference("foo"));
    FieldFromSelectFirst field = new FieldFromSelectFirst(select);
    field = (FieldFromSelectFirst)field.as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to first foo from ExampleData ordered by foo"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link Function}.
   */
  @Test
  public void testFunctionField() {
    final Function countRows = Function.count();
    final Function countValues = Function.count(new FieldReference("foo"));
    final Function countDistinct = Function.countDistinct(new FieldReference("foo"));
    final Function max = Function.max(new FieldReference("foo"));
    final Function min = Function.min(new FieldReference("foo"));
    final Function average = Function.average(new FieldReference("foo"));
    final Function averageDistinct = Function.averageDistinct(new FieldReference("foo"));
    final Function sum = Function.sum(new FieldReference("foo"));
    final Function sumDistinct = Function.sumDistinct(new FieldReference("foo"));
    final Function length = Function.length(new FieldReference("foo"));
    final Function yyyymmddToDate = Function.yyyymmddToDate(new FieldReference("foo"));
    final Function dateToYyyymmdd = Function.dateToYyyymmdd(new FieldReference("foo"));
    final Function substring = Function.substring(new FieldReference("foo"), new FieldLiteral(3), Function.length(new FieldReference("foo")).minus(new FieldLiteral(1)));
    final Function addDays = Function.addDays(new FieldReference("foo"), new FieldLiteral(42));
    final Function round = Function.round(new FieldReference("foo"), new FieldLiteral(2));
    final Function floor = Function.floor(new FieldReference("foo"));
    final Function isnull = Function.isnull(new FieldReference("foo"), new FieldLiteral("N"));
    final Function mod = Function.mod(new FieldReference("foo"), new FieldLiteral(100));
    final Function coalesce = Function.coalesce(new FieldReference("foo"), new FieldReference("foo2"), new FieldLiteral(42));
    final AliasedField daysBetween = Function.daysBetween(new FieldReference("foo"), new FieldReference("foo2"));
    final Function monthsBetween = Function.monthsBetween(new FieldReference("foo"), new FieldReference("foo2"));
    final Function trim = Function.trim(new FieldReference("foo"));
    final Function leftTrim = Function.leftTrim(new FieldReference("foo"));
    final Function rightTrim = Function.rightTrim(new FieldReference("foo"));
    final Function random = Function.random();
    final Function randomString = Function.randomString(new FieldLiteral(16));
    final Function power = Function.power(new FieldReference("foo"), new FieldLiteral(2));
    final Function lowerCase = Function.lowerCase(new FieldReference("foo"));
    final Function upperCase = Function.upperCase(new FieldReference("foo"));
    final Function leftPad = Function.leftPad(new FieldReference("foo"), new FieldLiteral(32), new FieldLiteral(' '));
    final Function now = Function.now();
    final Function lastDayOfMonth = Function.lastDayOfMonth(new FieldReference("foo"));

    assertEquals("COUNT", String.format("%n    - Set bar to record count"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(countRows.as("bar")));
    assertEquals("COUNT", String.format("%n    - Set bar to count of foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(countValues.as("bar")));
    assertEquals("COUNT_DISTINCT", String.format("%n    - Set bar to count of distinct foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(countDistinct.as("bar")));
    assertEquals("MAX", String.format("%n    - Set bar to highest foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(max.as("bar")));
    assertEquals("MIN", String.format("%n    - Set bar to lowest foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(min.as("bar")));
    assertEquals("AVG", String.format("%n    - Set bar to average of foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(average.as("bar")));
    assertEquals("AVG_DISTINCT", String.format("%n    - Set bar to average of distinct foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(averageDistinct.as("bar")));
    assertEquals("SUM", String.format("%n    - Set bar to sum of foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(sum.as("bar")));
    assertEquals("SUM_DISTINCT", String.format("%n    - Set bar to sum of distinct foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(sumDistinct.as("bar")));
    assertEquals("LENGTH", String.format("%n    - Set bar to length of foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(length.as("bar")));
    assertEquals("YYYYMMDD_TO_DATE", String.format("%n    - Set bar to foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(yyyymmddToDate.as("bar")));
    assertEquals("DATE_TO_YYYYMMDD", String.format("%n    - Set bar to foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(dateToYyyymmdd.as("bar")));
    assertEquals("SUBSTRING", String.format("%n    - Set bar to substring(foo, 3, length of foo - 1)"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(substring.as("bar")));
    assertEquals("ADD_DAYS", String.format("%n    - Set bar to foo plus 42 days"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(addDays.as("bar")));
    assertEquals("ROUND", String.format("%n    - Set bar to foo rounded to 2 decimal places"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(round.as("bar")));
    assertEquals("FLOOR", String.format("%n    - Set bar to floor(foo)"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(floor.as("bar")));
    assertEquals("IS_NULL", String.format("%n    - Set bar to foo or 'N' if null"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(isnull.as("bar")));
    assertEquals("MOD", String.format("%n    - Set bar to foo mod 100"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(mod.as("bar")));
    assertEquals("COALESCE", String.format("%n    - Set bar to first non-null of (foo, foo2, 42)"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(coalesce.as("bar")));
    assertEquals("DAYS_BETWEEN", String.format("%n    - Set bar to days between foo and foo2"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(daysBetween.as("bar")));
    assertEquals("MONTHS_BETWEEN", String.format("%n    - Set bar to months between foo and foo2"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(monthsBetween.as("bar")));
    assertEquals("TRIM", String.format("%n    - Set bar to trimmed foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(trim.as("bar")));
    assertEquals("LEFT_TRIM", String.format("%n    - Set bar to left trimmed foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(leftTrim.as("bar")));
    assertEquals("RIGHT_TRIM", String.format("%n    - Set bar to right trimmed foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(rightTrim.as("bar")));
    assertEquals("RANDOM", String.format("%n    - Set bar to random"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(random.as("bar")));
    assertEquals("RANDOM_STRING", String.format("%n    - Set bar to random 16 character string"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(randomString.as("bar")));
    assertEquals("POWER", String.format("%n    - Set bar to foo to the power 2"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(power.as("bar")));
    assertEquals("LOWER_CASE", String.format("%n    - Set bar to lower case foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(lowerCase.as("bar")));
    assertEquals("UPPER_CASE", String.format("%n    - Set bar to upper case foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(upperCase.as("bar")));
    assertEquals("LEFT_PAD", String.format("%n    - Set bar to leftPad(foo, 32, ' ')"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(leftPad.as("bar")));
    assertEquals("NOW", String.format("%n    - Set bar to now"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(now.as("bar")));
    assertEquals("LAST_DAY_OF_MONTH", String.format("%n    - Set bar to last day of month foo"), HumanReadableStatementHelper.generateAliasedFieldAssignmentString(lastDayOfMonth.as("bar")));
  }


  /**
   * Tests handling of {@link MathsField}.
   */
  @Test
  public void testMathsField() {
    final AliasedField field = new MathsField(new FieldReference("foo"), MathsOperator.PLUS, new FieldLiteral(42)).as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to foo + 42"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(field));
  }


  /**
   * Tests handling of {@link MathsField} wrapped by {@link BracketedExpression} .
   */
  @Test
  public void testBracketExpression() {
    final MathsField field = new MathsField(new FieldReference("foo"), MathsOperator.PLUS, new FieldLiteral(42));
    final AliasedField bracketExpression = new BracketedExpression(field).as("bar");

    assertEquals("Incorrect string generated",
      String.format("%n    - Set bar to (foo + 42)"),
      HumanReadableStatementHelper.generateAliasedFieldAssignmentString(bracketExpression));
  }
}
