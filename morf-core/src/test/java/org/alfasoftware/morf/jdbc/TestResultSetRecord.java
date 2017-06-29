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
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;

/**
 * Test that we correctly ask the dialect to format values in a
 * {@link ResultSetRecord}.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestResultSetRecord extends TestCase {

  /**
   * Test that we correctly ask the dialect to format values in a
   * {@link ResultSetRecord}.
   */
  @Test
  public void testCacheValues() throws SQLException {

    Table testTable = table("Test")
        .columns(
          column("test1", DataType.DECIMAL).nullable(),
          column("test2", DataType.BOOLEAN)
        );

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.findColumn("test1")).thenReturn(1);
    when(resultSet.findColumn("test2")).thenReturn(2);

    DatabaseSafeStringToRecordValueConverter converter = mock(DatabaseSafeStringToRecordValueConverter.class);

    when(
      converter.databaseSafeStringtoRecordValue(eq(DataType.DECIMAL), eq(resultSet), eq(1), eq("TEST1"))
    ).thenReturn("optimus");

    when(
      converter.databaseSafeStringtoRecordValue(eq(DataType.BOOLEAN), eq(resultSet), eq(2), eq("TEST2"))
    ).thenReturn("prime");

    ResultSetRecord record = new ResultSetRecord(testTable, resultSet, converter);

    record.cacheValues();

    verify(converter, times(1)).databaseSafeStringtoRecordValue(DataType.DECIMAL, resultSet, 1, "TEST1");
    verify(converter, times(1)).databaseSafeStringtoRecordValue(DataType.BOOLEAN, resultSet, 2, "TEST2");

    assertEquals(record.getValue("test1"), "optimus");
    assertEquals(record.getValue("test2"), "prime");
  }

}


