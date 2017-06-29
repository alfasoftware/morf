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

import org.junit.Test;


/**
 * Tests for {@link ColumnBean}
 */
public class TestColumnBean {

  @Test
  public void testConstructors() {
    Column col;
    Column col2 = new ColumnBean("a", DataType.DECIMAL, 1, 2, true, "z", true);
    Column col3 = new ColumnBean("a", DataType.DECIMAL, 1, 2, false, "z", false, true, 3);

    col = new ColumnBean(col2);
    assertEquals("Test 1 name successful",col2.getName(), col.getName());
    assertEquals("Test 1 type successful",col2.getType(), col.getType());
    assertEquals("Test 1 length successful",col2.getWidth(), col.getWidth());
    assertEquals("Test 1 scale successful",col2.getScale(), col.getScale());
    assertEquals("Test 1 nullable successful",col2.isNullable(), col.isNullable());
    assertEquals("Test 1 default value successful",col2.getDefaultValue(), col.getDefaultValue());
    assertEquals("Test 1 is key successful",col2.isPrimaryKey(), col.isPrimaryKey());

    col = new ColumnBean("b", DataType.STRING, 10);
    assertEquals("Test 2 name successful", "b", col.getName());
    assertEquals("Test 2 type successful", DataType.STRING, col.getType());
    assertEquals("Test 2 length successful", 10, col.getWidth());
    assertEquals("Test 2 scale successful", 0, col.getScale());
    assertEquals("Test 2 nullable successful", true, col.isNullable());
    assertEquals("Test 2 default value successful", "", col.getDefaultValue());
    assertEquals("Test 2 is key successful", false, col.isPrimaryKey());

    col = new ColumnBean(col2, false, "y", false);
    assertEquals("Test 3 name successful",col2.getName(), col.getName());
    assertEquals("Test 3 type successful",col2.getType(), col.getType());
    assertEquals("Test 3 length successful",col2.getWidth(), col.getWidth());
    assertEquals("Test 3 scale successful",col2.getScale(), col.getScale());
    assertEquals("Test 3 nullable successful", false, col.isNullable());
    assertEquals("Test 3 default value successful", "y", col.getDefaultValue());
    assertEquals("Test 3 is key successful", false, col.isPrimaryKey());

    col = new ColumnBean(col3, true, "q", true);
    assertEquals("Test 4 name successful",col2.getName(), col.getName());
    assertEquals("Test 4 type successful",col2.getType(), col.getType());
    assertEquals("Test 4 length successful",col2.getWidth(), col.getWidth());
    assertEquals("Test 4 scale successful",col2.getScale(), col.getScale());
    assertEquals("Test 4 nullable successful", true, col.isNullable());
    assertEquals("Test 4 default value successful", "q", col.getDefaultValue());
    assertEquals("Test 4 is key successful", true, col.isPrimaryKey());
    assertEquals("Test 4 is autonumbered successful", true, col.isAutoNumbered());
    assertEquals("Test 4 autonumber start successful", 3, col.getAutoNumberStart());

    col = new ColumnBean("c", DataType.DECIMAL, 6, 7, false);
    assertEquals("Test 5 name successful", "c", col.getName());
    assertEquals("Test 5 type successful", DataType.DECIMAL, col.getType());
    assertEquals("Test 5 length successful", 6, col.getWidth());
    assertEquals("Test 5 scale successful", 7, col.getScale());
    assertEquals("Test 5 nullable successful", false, col.isNullable());
    assertEquals("Test 5 default value successful", "", col.getDefaultValue());
    assertEquals("Test 5 is key successful", false, col.isPrimaryKey());

    col = new ColumnBean("d", DataType.DECIMAL, 8, 9, true, "u");
    assertEquals("Test 6 name successful", "d", col.getName());
    assertEquals("Test 6 type successful", DataType.DECIMAL, col.getType());
    assertEquals("Test 6 length successful", 8, col.getWidth());
    assertEquals("Test 6 scale successful", 9, col.getScale());
    assertEquals("Test 6 nullable successful", true, col.isNullable());
    assertEquals("Test 6 default value successful", "u", col.getDefaultValue());
    assertEquals("Test 6 is key successful", false, col.isPrimaryKey());

    assertEquals("toString() correct", "Column-d-DECIMAL-8-9-true-u-false-false-0", col.toString());

  }

}
