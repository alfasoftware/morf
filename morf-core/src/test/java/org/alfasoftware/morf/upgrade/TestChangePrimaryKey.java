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
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;

/**
 * Tests for {@link ChangePrimaryKeyColumns}.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestChangePrimaryKey {

  @Test
  public void changePrimaryKeyOrder() {
    List<String> from = Arrays.asList("col1", "col3");
    List<String> to = Arrays.asList("col3", "col1");

    ChangePrimaryKeyColumns changePrimaryKeyColumns = new ChangePrimaryKeyColumns("Table", from, to);
    Schema schema = schema(
      table("Table").columns(
        column("col1", DataType.STRING).primaryKey(),
        column("col3", DataType.STRING).primaryKey(),
        column("col2", DataType.STRING)));

    Schema after = changePrimaryKeyColumns.apply(schema);
    List<String> resultingColumns = newArrayList();
    for (Column column : after.getTable("Table").columns()) {
      resultingColumns.add(column.getName());
    }

    assertEquals("Complete column order after re-ordering primary key", Arrays.asList("col3", "col1", "col2"), resultingColumns);
  }


  /**
   * Tests that the change can be made regardless of the case of the input.
   */
  @Test
  public void changePrimaryKeyOrderCaseInsensitive() {
    List<String> from = Arrays.asList("cOL1", "Col3");
    List<String> to = Arrays.asList("COL3", "Col1");

    ChangePrimaryKeyColumns changePrimaryKeyColumns = new ChangePrimaryKeyColumns("Table", from, to);
    Schema schema = schema(
      table("Table").columns(
        column("col1", DataType.STRING).primaryKey(),
        column("col3", DataType.STRING).primaryKey(),
        column("col2", DataType.STRING)));

    Schema after = changePrimaryKeyColumns.apply(schema);
    List<String> resultingColumns = newArrayList();
    for (Column column : after.getTable("Table").columns()) {
      resultingColumns.add(column.getName());
    }

    assertEquals("Complete column order after re-ordering primary key", Arrays.asList("col3", "col1", "col2"), resultingColumns);
  }


  /**
   * Test that we cannot change the primary key to a combination of columns that is indexed
   */
  @Test
  public void testCannotChangePrimaryKeyToAnAlreadyIndexedSetOfColumns() {
    List<String> from = Arrays.asList("col1", "col2");
    List<String> to = Arrays.asList("col3", "col1");

    ChangePrimaryKeyColumns changePrimaryKeyColumns = new ChangePrimaryKeyColumns("Table", from, to);
    Schema schema = schema(
      table("Table").columns(
        column("col1", DataType.STRING).primaryKey(),
        column("col2", DataType.STRING).primaryKey(),
        column("col3", DataType.STRING)
      ).indexes(
        index("Table_1").columns("col3", "col1")
      )
    );

    try {
      changePrimaryKeyColumns.apply(schema);
      fail("Expect exception");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("col3"));
      assertTrue(e.getMessage().contains("col1"));
    }
  }
}
