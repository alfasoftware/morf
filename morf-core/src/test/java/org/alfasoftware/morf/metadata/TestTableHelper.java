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
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;


/**
 * Test the database helper
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestTableHelper {

  /**
   * Test that we can get the column object given only
   * the column's name.
   */
  @Test
  public void testColumnWithName() {
    String columnName = "columnName";
    Table table = table("tableName").columns(
      idColumn(),
      versionColumn(),
      column(columnName, DataType.STRING, 10).nullable()
    );
    assertEquals("Incorrect column", TableHelper.columnWithName(table, columnName).getName(), columnName);
  }

  /**
   * Test that we can get the index object given only
   * the index's name.
   */
  @Test
  public void testIndexWithName() {
    String indexName = "indexName";
    Table table = table("tableName").indexes(index(indexName));
    assertEquals("Incorrect index", TableHelper.indexWithName(table, indexName).getName(), indexName);
  }

  /**
   * Test that we can build a list of column names
   * from the column definitions (preceded by "id"
   * and "version").
   */
  @Test
  public void testBuildColumnNameList() {
    Table table = table("tableName").columns(
        idColumn(),
        versionColumn(),
        column("columnName1", DataType.STRING, 10).nullable(),
        column("columnName2", DataType.STRING, 10).nullable()
      );
    assertEquals("Column list", Arrays.asList("id", "version", "columnName1", "columnName2"), TableHelper.buildColumnNameList(table));
  }

}
