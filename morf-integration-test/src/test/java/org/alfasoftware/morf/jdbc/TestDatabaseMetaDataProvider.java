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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.alfasoftware.morf.jdbc.h2.H2;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link DatabaseMetaDataProvider}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestDatabaseMetaDataProvider {

  /**
   * Checks the blob type columns have the correct type
   *
   * @throws SQLException not really thrown
   */
  @Test
  public void testBlobType() throws SQLException {

    ConnectionResourcesBean databaseConnection = new ConnectionResourcesBean();
    databaseConnection.setDatabaseName("database");
    databaseConnection.setDatabaseType(H2.IDENTIFIER);
    Connection connection = databaseConnection.getDataSource().getConnection();

    try {
      Statement createStatement = connection.createStatement();
      createStatement.execute("CREATE TABLE test ( BLOBCOL longvarbinary )");

      DatabaseMetaDataProvider provider = new DatabaseMetaDataProvider(connection, null);

      Table table = provider.getTable("test");
      assertEquals("Exactly 1 column expected", 1, table.columns().size());
      assertEquals("Column name not correct", "BLOBCOL", table.columns().get(0).getName());
      assertEquals("Column type not correct", DataType.BLOB, table.columns().get(0).getType());
      assertEquals("Column width not correct", 2147483647, table.columns().get(0).getWidth());
      assertEquals("Column scale not correct", 0, table.columns().get(0).getScale());
    } finally {
      connection.close();
    }
  }


  /**
   * Checks composite primary key indication, where the primary key is defined out of order and has a column in the middle
   *
   * @throws SQLException not really thrown
   */
  @Test
  public void testCompositePrimaryKeyWithInterleavedColumn() throws SQLException {

    ConnectionResourcesBean databaseConnection = new ConnectionResourcesBean();
    databaseConnection.setDatabaseName("database");
    databaseConnection.setDatabaseType(H2.IDENTIFIER);
    Connection connection = databaseConnection.getDataSource().getConnection();

    try {
      Statement createStatement = connection.createStatement();
      createStatement.execute("CREATE TABLE CompositePrimaryKeyTest ( AID2 VARCHAR(10), COL VARCHAR(10), ZID1 VARCHAR(10), CONSTRAINT CompositePrimaryKeyTest_PK PRIMARY KEY (ZID1, AID2) )");

      DatabaseMetaDataProvider provider = new DatabaseMetaDataProvider(connection, null);

      Table table = provider.getTable("compositeprimarykeytest");
      assertEquals("Exactly 3 column expected", 3, table.columns().size());
      assertTrue("First column primary key", table.columns().get(0).isPrimaryKey());
      assertEquals("First column should also be first of primary key", "ZID1", table.columns().get(0).getName());
      assertFalse("Normal column", table.columns().get(1).isPrimaryKey());
      assertEquals("First normal column should remain in the middle of the primary key columns", "COL", table.columns().get(1).getName());
      assertTrue("Second column primary key", table.columns().get(2).isPrimaryKey());
      assertEquals("Second column should also be second of primary key", "AID2", table.columns().get(2).getName());
    } finally {
      connection.close();
    }
  }


  /**
   * Checks composite primary key indication, where the primary key is defined out of order and has a column at the end.
   * @throws SQLException
   */
  @Test
  public void testCompositePrimaryKeyWithColumnsAtEnd() throws SQLException {

    ConnectionResourcesBean databaseConnection = new ConnectionResourcesBean();
    databaseConnection.setDatabaseName("database");
    databaseConnection.setDatabaseType(H2.IDENTIFIER);
    Connection connection = databaseConnection.getDataSource().getConnection();

    try {
      Statement createStatement = connection.createStatement();
      createStatement.execute("CREATE TABLE CompositePrimaryKeyTest2 ( AID2 VARCHAR(10), ZID1 VARCHAR(10), COL VARCHAR(10), CONSTRAINT CompositePrimaryKeyTest2_PK PRIMARY KEY (ZID1, AID2) )");

      DatabaseMetaDataProvider provider = new DatabaseMetaDataProvider(connection, null);

      Table table = provider.getTable("compositeprimarykeytest2");
      assertEquals("Exactly 3 column expected", 3, table.columns().size());
      assertTrue("First column primary key", table.columns().get(0).isPrimaryKey());
      assertEquals("First column should also be first of primary key", "ZID1", table.columns().get(0).getName());
      assertTrue("Second column primary key", table.columns().get(1).isPrimaryKey());
      assertEquals("Second column should also be second of primary key", "AID2", table.columns().get(1).getName());
      assertFalse("Normal column", table.columns().get(2).isPrimaryKey());
      assertEquals("The normal column should remain after the primary key columns", "COL", table.columns().get(2).getName());
    } finally {
      connection.close();
    }
  }


  /**
   * Checks that we ignore schema-level default values.
   *
   * @throws SQLException not really thrown
   */
  @Test
  public void testDefaultValueIgnored() throws SQLException {

    ConnectionResourcesBean databaseConnection = new ConnectionResourcesBean();
    databaseConnection.setDatabaseName("database");
    databaseConnection.setDatabaseType(H2.IDENTIFIER);
    Connection connection = databaseConnection.getDataSource().getConnection();

    try {
      Statement createStatement = connection.createStatement();
      createStatement.execute("CREATE TABLE TestOther ( version integer )");
      createStatement.execute("ALTER TABLE TestOther ADD COLUMN stringField_with_default VARCHAR(6) DEFAULT 'N' NOT NULL");

      DatabaseMetaDataProvider provider = new DatabaseMetaDataProvider(connection, null);

      Table table = provider.getTable("testother");
      assertEquals("Exactly 2 column expected", 2, table.columns().size());
      assertTrue("Default value is empty if not version", table.columns().get(0).getName().equals("VERSION")?table.columns().get(0).getDefaultValue().equals("0"):table.columns().get(0).getDefaultValue().isEmpty());
      assertTrue("Default value is empty if not version", table.columns().get(1).getName().equals("VERSION")?table.columns().get(1).getDefaultValue().equals("0"):table.columns().get(1).getDefaultValue().isEmpty());
    } finally {
      connection.close();
    }
  }


  /**
   * Checks view metadata can be read.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testViews() throws SQLException {
    ConnectionResourcesBean databaseConnection = new ConnectionResourcesBean();
    databaseConnection.setDatabaseName("database");
    databaseConnection.setDatabaseType(H2.IDENTIFIER);
    Connection connection = databaseConnection.getDataSource().getConnection();

    try {
      Statement createStatement = connection.createStatement();
      createStatement.execute("CREATE TABLE TestForView ( version integer )");
      createStatement.execute("CREATE VIEW TestView AS (SELECT version + 1 AS Foo FROM TestForView)");

      DatabaseMetaDataProvider provider = new DatabaseMetaDataProvider(connection, null);
      View view = provider.getView("testview");
      assertEquals("Name", "TESTVIEW", view.getName());
      try {
        SelectStatement selectStatement = view.getSelectStatement();
        fail("Expected UnsupportedOperationException, got " + selectStatement);
      } catch (UnsupportedOperationException e) {
        assertEquals("Message", "Cannot return SelectStatement as [TESTVIEW] has been loaded from the database", e.getMessage());
      }

      try {
        String[] dependencies = view.getDependencies();
        fail("Expected UnsupportedOperationException, got " + dependencies);
      } catch (UnsupportedOperationException e) {
        assertEquals("Message", "Cannot return dependencies as [TESTVIEW] has been loaded from the database", e.getMessage());
      }

      assertEquals("View names", "[TESTVIEW]", provider.viewNames().toString());
      assertEquals("Views", ImmutableSet.of(view), ImmutableSet.copyOf(provider.views()));
      assertTrue("View exists", provider.viewExists("TestVIEW"));

    } finally {
      connection.close();
    }
  }
}
