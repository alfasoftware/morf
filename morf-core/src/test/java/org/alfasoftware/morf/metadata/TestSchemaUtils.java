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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;


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


  @Test
  public void testToUpperCase() {
    assertEquals(
      Arrays.asList("ONE", "TWO", "THREE"),
      SchemaUtils.toUpperCase(Arrays.asList("One", "two", "thReE"))
    );
  }
}
