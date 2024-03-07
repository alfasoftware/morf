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

import static org.alfasoftware.morf.metadata.SchemaUtils.copy;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Objects;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * Tests {@link SchemaUtils}.  Not a lot here yet.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestSchemaUtils {

  /**
   * Tests that the type method creates a {@link ColumnType} with the requested properties.
   */
  @Test
  public void testCreatingAColumnTypeNotNullable() {
    ColumnType columnType = SchemaUtils.type(DataType.DECIMAL, 10, 4, false);
    assertTrue("ColumnType type incorrect", columnType.getType().equals(DataType.DECIMAL));
    assertTrue("ColumnType width incorrect", columnType.getWidth() == 10);
    assertTrue("ColumnType scale incorrect", columnType.getScale() == 4);
    assertFalse("ColumnType should not be nullable", columnType.isNullable());
  }


  /**
   * Tests that the type method creates a {@link ColumnType} with the requested properties and is nullable.
   */
  @Test
  public void testCreatingAColumnTypeNullable() {
    ColumnType columnType = SchemaUtils.type(DataType.STRING, 10, 0, true);
    assertTrue("ColumnType type incorrect", columnType.getType().equals(DataType.STRING));
    assertTrue("ColumnType width incorrect", columnType.getWidth() == 10);
    assertTrue("ColumnType scale incorrect", columnType.getScale() == 0);
    assertTrue("ColumnType should be nullable", columnType.isNullable());
  }


  /**
   * Test {@link SchemaUtils#copy()} excludes tables and views that match the given regex.
   */
  @Test
  public void testCopySchemaWithExclusions() {
    String excludePrefix = "^EXCLUDE_.*$";
    String excludeMatch= "^Drivers$";
    String exlcudeSequenceMatch = "^SequenceExclude$";

    Schema schema  = new SchemaBean(ImmutableList.<Table>of(table("EXCLUDE_Boo"), table("EXCLUDE_Foo"), table("table1"), table("table2")),
      ImmutableList.of(SchemaUtils.view("Driver", select()), SchemaUtils.view("Drivers", select())),
      ImmutableList.of(SchemaUtils.sequence("Sequence1"), SchemaUtils.sequence("SequenceExclude")));

    assertEquals("4 tables should exist", 4, schema.tables().size());
    assertEquals("2 views should exist", 2, schema.views().size());
    assertEquals("2 sequences should exist", 2, schema.sequences().size());
    assertTrue(schema.tables().stream().anyMatch(t -> t.getName().equals("EXCLUDE_Boo")));
    assertTrue(schema.views().stream().anyMatch(t -> t.getName().equals("Drivers")));
    assertTrue(schema.sequences().stream().anyMatch(t -> t.getName().equals("SequenceExclude")));

    //method under test
    schema = copy(schema, Lists.newArrayList(excludePrefix, excludeMatch, exlcudeSequenceMatch));

    //...assert during the copy excluded tables, views and sequences are being removed
    assertEquals("2 tables should exist", 2, schema.tables().size());
    assertEquals("1 view should exist", 1, schema.views().size());
    assertEquals("1 sequence should exist", 1, schema.sequences().size());
    assertFalse(schema.tables().stream().anyMatch(t -> Objects.equals(t.getName(), excludePrefix)));
    assertFalse(schema.views().stream().anyMatch(t -> Objects.equals(t.getName(), excludeMatch)));
    assertFalse(schema.sequences().stream().anyMatch(t -> Objects.equals(t.getName(), exlcudeSequenceMatch)));
  }


  @Test
  public void testToUpperCase() {
    assertEquals(
      Arrays.asList("ONE", "TWO", "THREE"),
      SchemaUtils.toUpperCase(Arrays.asList("One", "two", "thReE"))
    );
  }
}
