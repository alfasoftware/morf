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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

/**
 * Test building schemas with {@link SchemaUtils}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestSchemaBuilder {

  /**
   * Test the basic table syntax
   */
  @Test
  public void testSimplestTable() {
    Table test = table("Test")
      .columns(
        column("bar", DataType.STRING, 255)
      );

    assertEquals("Test", test.getName());
    assertEquals(1, test.columns().size());
    assertEquals("bar", test.columns().get(0).getName());
    assertEquals(DataType.STRING, test.columns().get(0).getType());
    assertEquals(255, test.columns().get(0).getWidth());
    assertEquals(0, test.columns().get(0).getScale());
    assertEquals("", test.columns().get(0).getDefaultValue());
    assertFalse(test.columns().get(0).isPrimaryKey());
  }


  /**
   * Test all options on the schema builder.
   */
  @Test
  public void testAllOptions() {
    Table test = table("Test")
      .columns(
        idColumn(),
        versionColumn(),
        column("foo", DataType.DECIMAL, 13, 2).primaryKey().defaultValue("1"),
        column("bar", DataType.STRING, 255).nullable().defaultValue("x"),
        column("baz", DataType.STRING, 255).defaultValue("x").nullable()
      )
      .indexes(
        index("myIndex").columns("foo", "bar").unique(),
        index("myIndex").unique().columns("foo"),
        index("myOtherIndex").columns("bar")
      );

    assertEquals("Test", test.getName());
    assertEquals(5, test.columns().size());
    assertEquals(3, test.indexes().size());

    assertEquals("myIndex", test.indexes().get(0).getName());
    assertEquals(Arrays.asList("foo", "bar"), test.indexes().get(0).columnNames());
    assertTrue(test.indexes().get(0).isUnique());

    assertEquals(Arrays.asList("bar"), test.indexes().get(2).columnNames());
    assertFalse(test.indexes().get(2).isUnique());
  }


  /**
   * Test creating a Schema.
   */
  @Test
  public void testSchema() {
    Schema schema = schema(
      table("Test1")
        .columns(
          column("foo", DataType.STRING, 255)
        ),
      table("Test2")
        .columns(
          column("bar", DataType.STRING, 255)
        )
    );

    assertEquals(2, schema.tables().size());
  }
}
