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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link InsertStatementDefaulter}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestInsertStatementDefaulter {

  private Schema schema;


  /**
   * @see junit.framework.TestCase#setUp()
   */
  @Before
  public void setUp() throws Exception {

    Table car = table("Car")
      .columns(
        idColumn(),
        versionColumn(),
        column("name", DataType.STRING).nullable(),
        column("engineCapacity", DataType.DECIMAL).nullable()
      );

    Table otherTable = table("Other")
      .columns(
        idColumn(),
        versionColumn(),
        column("stringField", DataType.STRING, 3).nullable(),
        column("intField", DataType.DECIMAL, 8).nullable(),
        column("floatField", DataType.DECIMAL, 13, 2)
      );

    Table testTable = table("Test")
      .columns(
        idColumn(),
        versionColumn(),
        column("stringField", DataType.STRING, 3).nullable(),
        column("intField", DataType.DECIMAL, 8).nullable(),
        column("floatField", DataType.DECIMAL, 13, 2).nullable(),
        column("dateField", DataType.DATE).nullable(),
        column("booleanField", DataType.BOOLEAN).nullable(),
        column("charField", DataType.STRING, 1).nullable(),
        column("blobField", DataType.BLOB, 16384).nullable()
      );

    schema = schema(car, otherTable, testTable);
  }


  /**
   * Test that decimal values are defaulted correctly.
   */
  @Test
  public void testInsertDefaultedDecimalValue() {
    InsertStatementDefaulter defaulter = new InsertStatementDefaulter(schema);

    InsertStatement statement = insert().into(new TableReference("Car")).values(new FieldLiteral("bob").as("name")).build();
    statement = defaulter.defaultMissingFields(statement);

    assertEquals("Field default size", 3, statement.getFieldDefaults().size());
    assertFieldValue("engineCapacity", "0", statement.getFieldDefaults());
  }


  /**
   * Test that string values are defaulted correctly.
   */
  @Test
  public void testInsertDefaultedStringValue() {
    InsertStatementDefaulter defaulter = new InsertStatementDefaulter(schema);

    InsertStatement statement = insert().into(new TableReference("Car")).values(new FieldLiteral(1.0).as("engineCapacity")).build();
    statement = defaulter.defaultMissingFields(statement);

    assertEquals("Field default size", 3, statement.getFieldDefaults().size());
    assertFieldValue("name", "", statement.getFieldDefaults());
  }


  /**
   * Test that the version is defaulted correctly.
   */
  @Test
  public void testInsertDefaultedVersion() {
    InsertStatementDefaulter defaulter = new InsertStatementDefaulter(schema);

    InsertStatement statement =  insert().into(new TableReference("Car")).values(new FieldLiteral("bob").as("name")).build();
    statement = defaulter.defaultMissingFields(statement);

    assertEquals("Field default size", 3, statement.getFieldDefaults().size());
    assertFieldValue("version", "0", statement.getFieldDefaults());
  }


  /**
   * Test insert from.
   */
  @Test
  public void testFrom() {
    InsertStatementDefaulter defaulter = new InsertStatementDefaulter(schema);

    InsertStatement statement = insert().into(new TableReference("Test")).from(new TableReference("Other")).build();
    statement = defaulter.defaultMissingFields(statement);

    assertEquals("Field default size", 4, statement.getFieldDefaults().size());
    assertFieldValue("dateField", null, statement.getFieldDefaults());
    assertFieldValue("booleanField", "false", statement.getFieldDefaults());
    assertFieldValue("charField", "", statement.getFieldDefaults());
    assertFieldValue("blobField", null, statement.getFieldDefaults());
  }


  /**
   * Asserts that a field is as expected.
   *
   * @param name the field name.
   * @param expected the expected value.
   * @param fields the map of fields.
   */
  private void assertFieldValue(String name, String expected, Map<String, AliasedField> fields) {
    assertFieldValue(name, expected, fields.get(name));
  }


  /**
   * Asserts that a field is as expected.
   *
   * @param name the field name.
   * @param expected the expected value.
   * @param field the field.
   */
  private void assertFieldValue(String name, String expected, AliasedField field) {
    assertNotNull("Field null", field);
    assertEquals("Field name", name, field.getAlias());
    if (field instanceof FieldLiteral) {
      assertEquals("Field value: " + name, expected, ((FieldLiteral) field).getValue());
    } else if (field instanceof NullFieldLiteral) {
      assertNull("Field value: " + name, expected);
    } else {
      fail("Field type not supported");
    }
  }
}
