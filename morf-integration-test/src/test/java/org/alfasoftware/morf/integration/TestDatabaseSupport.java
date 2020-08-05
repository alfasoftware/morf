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

package org.alfasoftware.morf.integration;

import static org.alfasoftware.morf.metadata.DataSetUtils.dataSetProducer;
import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

import net.jcip.annotations.NotThreadSafe;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetHomology;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.jdbc.SchemaModificationAdapter;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Tests that our tooling works on the currently configured database.
 *
 * <p>This tests makes actual database connections and schema changes.<p>
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
@NotThreadSafe
public class TestDatabaseSupport {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Inject
  private Provider<DatabaseDataSetConsumer> databaseDataSetConsumer;

  @Inject
  private Provider<DatabaseDataSetProducer> databaseDataSetProducer;

  @Inject
  private Provider<DatabaseSchemaManager> schemaManager;

  /**
   * The test schema.
   */
  private final Schema schema = schema(
    table("SimpleTypes")
      .columns(
        column("stringCol", DataType.STRING, 50).primaryKey(),
        column("nullableStringCol", DataType.STRING, 10).nullable(),
        column("decimalTenZeroCol", DataType.DECIMAL, 10),
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("bigIntegerCol", DataType.BIG_INTEGER),
        column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable(),
        column("booleanCol", DataType.BOOLEAN),
        column("nullableBooleanCol", DataType.BOOLEAN).nullable(),
        column("dateCol", DataType.DATE),
        column("nullableDateCol", DataType.DATE).nullable()
      ),
    table("WithABlob")
      .columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("blobColumn", DataType.BLOB)
      ),
    table("WithAClob")
      .columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("clobColumn", DataType.CLOB)
      ),
    table("WithAutoNum")
      .columns(
        column("autonumfield", DataType.BIG_INTEGER).autoNumbered(5000).primaryKey()
      )
  );


  /**
   * The test dataset
   */
  private final DataSetProducer dataSet = dataSetProducer(schema)
    .table("SimpleTypes",
      record() /* Use string conversion */
        .setString("stringCol", "hello world AA")
        .setString("nullableStringCol", "not null")
        .setString("decimalTenZeroCol", "9876543210")
        .setString("decimalNineFiveCol", "9234.12345")
        .setString("bigIntegerCol", "9234567890123456")
        .setString("nullableBigIntegerCol", "56732")
        .setString("booleanCol", "true")
        .setString("nullableBooleanCol", "false")
        .setString("dateCol", "2011-02-03")
        .setString("nullableDateCol", "2013-04-05"),
      record() /* Use actual types */
        .setString("stringCol", "hello world AB")
        .setString("nullableStringCol", "not null")
        .setBigDecimal("decimalTenZeroCol", new BigDecimal("9876543210"))
        .setBigDecimal("decimalNineFiveCol", new BigDecimal("9234.12345"))
        .setLong("bigIntegerCol", 9234567890123456L)
        .setInteger("nullableBigIntegerCol", 56732)
        .setBoolean("booleanCol", true)
        .setBoolean("nullableBooleanCol", false)
        .setDate("dateCol", java.sql.Date.valueOf("2011-02-03"))
        .setLocalDate("nullableDateCol", new LocalDate(2013, 4, 5)),
      record() /* Use string conversion */
        .setString("stringCol", "hello world ZY")
        // nullableStringCol is null
        .setString("decimalTenZeroCol", "-1")
        .setString("decimalNineFiveCol", "1.0")
        .setString("bigIntegerCol", "1")
        // nullableBigIntegerCol is null
        .setString("booleanCol", "true")
        // nullableBooleanCol is null
        .setString("dateCol", "2012-03-04"),
        // nullableDateCol is null
      record() /* Use actual types */
        .setString("stringCol", "hello world ZZ")
        // nullableStringCol is null
        .setBigDecimal("decimalTenZeroCol", BigDecimal.ONE.negate())
        .setBigDecimal("decimalNineFiveCol", BigDecimal.ONE.setScale(1))
        .setLong("bigIntegerCol", 1L)
        // nullableBigIntegerCol is null
        .setBoolean("booleanCol", true)
        // nullableBooleanCol is null
        .setDate("dateCol", java.sql.Date.valueOf("2012-03-04"))
        // nullableDateCol is null
    ).table("WithABlob",
      record()
        .setInteger("id", 123)
        .setString("blobColumn", "aGVsbG8gd29ybGQ="), // "hello world" base 64 encoded
      record()
        .setInteger("id", 124)
        .setByteArray("blobColumn", "hello world".getBytes()),
      record()
        .setLong("id", 456L)
        .setString("blobColumn", "Zm9v"), // "foo" base 64 encoded
      record()
        .setLong("id", 457L)
        .setByteArray("blobColumn", "foo".getBytes())
    ).table("WithAClob",
      record()
        .setBigDecimal("id", new BigDecimal(124))
        .setString("clobColumn", randomLongString(1024)),
      record()
        .setString("id", "457")
        .setString("clobColumn", randomLongString(1024))
    ).table("WithAutoNum",
      record()
        .setLong("autonumfield", 5L)
    );


  /**
   * After, tidy up...
   */
  @After
  public void after() {
    // tell the schema manager we've messed with the DB
    schemaManager.get().invalidateCache();
  }


  /**
   * Test that a normal import/export works.
   */
  @Test
  public void testBasicDatabaseTypes() {
    // a database consumer that mutates the DB
    doConnectAndCompare(schema, dataSet, databaseDataSetConsumer.get());
  }


  /**
   * Tests that when an attempt is made to insert a value that is too large for a column into the database that an exception is thrown.
   */
  @Test
  public void testDataTruncation() {
    thrown.expect(RuntimeException.class);

    DataSetProducer dataSetWithInvalidValue = dataSetProducer(schema)
        .table("SimpleTypes",
          record()
            .setString("stringCol", "A")
            .setString("nullableStringCol", "A")
            .setInteger("decimalTenZeroCol", 1)
            .setBigDecimal("decimalNineFiveCol", new BigDecimal("27832.231"))  // This value has too many digits
            .setInteger("bigIntegerCol", 1)
            .setInteger("nullableBigIntegerCol", 1)
            .setBoolean("booleanCol", false)
            .setBoolean("nullableBooleanCol", true)
            .setDate("dateCol", java.sql.Date.valueOf("2011-02-03"))
            .setDate("nullableDateCol", java.sql.Date.valueOf("2013-04-05"))
         )
        .table("WithABlob")
        .table("WithAClob")
        .table("WithAutoNum");

    doConnectAndCompare(schema, dataSetWithInvalidValue, databaseDataSetConsumer.get());
  }


  /**
   * Import the data into the DB then read it back out again, checking it comes out exactly as it went in.
   */
  private void doConnectAndCompare(Schema testSchema, DataSetProducer testDataSet, DatabaseDataSetConsumer databaseConsumer) {
    // transfer the dataset into the DB
    schemaManager.get().dropAllViews();
    new DataSetConnector(testDataSet, new SchemaModificationAdapter(databaseConsumer)).connect();

    final List<String> differences = Lists.newArrayList();

    DatabaseDataSetProducer databaseProducer = databaseDataSetProducer.get();
    databaseProducer.open();
    try {
      DifferenceWriter differenceWriter = new DifferenceWriter() {
        @Override
        public void difference(String message) {
          differences.add(message);
        }
      };

      new SchemaHomology(
        differenceWriter,
        "Test schema",
        "Database schema"
      ).schemasMatch(testSchema, databaseProducer.getSchema(), Sets.<String>newHashSet());

    } finally {
      databaseProducer.close();
    }

    assertEquals("Expect no differences", "", StringUtils.join(differences, "\n"));

    // create a comparator
    DataSetHomology dataSetHomology = new DataSetHomology();

    // check the database-produced content is the same as what we put in
    boolean dataSetsMatch = dataSetHomology.dataSetProducersMatch(testDataSet, databaseDataSetProducer.get());
    assertTrue(StringUtils.join(dataSetHomology.getDifferences(), "\n"), dataSetsMatch);
  }


  /**
   * @return a random String of a minimum of 1024 characters.
   */
  private String randomLongString(int length) {
    Random random = new Random();

    char[] fill = new char[length];

    StringBuilder nextValue = new StringBuilder(new String(fill));

    for (int position = 0; position < length; position++) {
      int result = random.nextInt(9999);
      char digit = (char) ('A' + result % 26);
      nextValue.setCharAt(position, digit);
    }

    return nextValue.toString();
  }
}
