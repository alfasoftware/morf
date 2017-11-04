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

package org.alfasoftware.morf.sql.element;

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * Tests for {@link TableReference}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestTableReference {

  private final TableReference onTest = tableRef("A");
  private final TableReference isEqual = tableRef("A");
  private final TableReference notEqualDueToSchema = new TableReference("Schema", "A");
  private final TableReference notEqualDueToName = tableRef("B");
  private final TableReference notEqualDueToAlias = tableRef("A").as("X");

  private final TableReference onTestAliased = new TableReference("Schema", "A").as("X");
  private final TableReference isEqualAliased = new TableReference("Schema", "A").as("X");
  private final TableReference differentSchema = new TableReference("Schema2", "A").as("X");
  private final TableReference differentName = new TableReference("Schema", "B").as("X");
  private final TableReference differentAlias = new TableReference("Schema", "A").as("Y");

  @Test
  public void testHashCode() {
    assertEquals(isEqual.hashCode(), onTest.hashCode());
    assertEquals(isEqualAliased.hashCode(), onTestAliased.hashCode());
  }

  @Test
  public void testEquals() {
    assertEquals(isEqual, onTest);
    assertFalse(onTest.equals(null));
    assertNotEquals(notEqualDueToSchema, onTest);
    assertNotEquals(notEqualDueToName, onTest);
    assertNotEquals(notEqualDueToAlias, onTest);

    assertEquals(isEqualAliased, onTestAliased);
    assertNotEquals(differentSchema, onTestAliased);
    assertNotEquals(differentName, onTestAliased);
    assertNotEquals(differentAlias, onTestAliased);
  }

  @Test
  public void testAliasImmutability() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      assertEquals(isEqual.as("A"), onTest.as("A"));
      assertEquals(isEqual.as("B").as("A"), onTest.as("A"));
      assertNotEquals(isEqual.as("A"), onTest.as("B"));
      assertNotSame(onTest, onTest.as("A"));
    });

    // Should get the same object with immutable builders off
    assertSame(onTest, onTest.as("A"));
  }

  @Test
  public void testDeepCopy() {
    TableReference deepCopy = onTest.deepCopy();
    assertEquals(deepCopy, onTest);
    assertNotSame(deepCopy, onTest);
  }

  @Test
  public void testDeepCopyAliased() {
    TableReference deepCopy = onTestAliased.deepCopy();
    assertEquals(deepCopy, onTestAliased);
    assertNotSame(deepCopy, onTestAliased);
  }
}