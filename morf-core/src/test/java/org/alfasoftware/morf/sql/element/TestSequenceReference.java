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

import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.alfasoftware.morf.sql.SqlUtils.sequenceRef;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

/**
 * Test class for {@link SequenceReference}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestSequenceReference {

  private final SequenceReference onTest = sequenceRef("A");
  private final SequenceReference isEqual = sequenceRef("A");
  private final SequenceReference notEqualDueToName = sequenceRef("B");
  private final SequenceReference notEqualDueToTypeOfOperation = sequenceRef("A").nextValue();
  private final AliasedField notEqualDueToAlias = sequenceRef("A").as("X");

  private final AliasedField onTestAliased = sequenceRef("A").as("X");
  private final AliasedField isEqualAliased = sequenceRef("A").as("X");
  private final AliasedField differentName = sequenceRef("B").as("X");
  private final AliasedField differentAlias = sequenceRef("A").as("Y");
  private final AliasedField differentTypeOfOperation = sequenceRef("A").currentValue().as("Y");

  private final AliasedField onTestTypeOfOperation = sequenceRef("A").nextValue();
  private final AliasedField isEqualTypeOfOperation = sequenceRef("A").nextValue();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void testHashCode() {
    assertEquals(isEqual.hashCode(), onTest.hashCode());
    assertEquals(isEqualAliased.hashCode(), onTestAliased.hashCode());
    assertEquals(isEqualTypeOfOperation.hashCode(), onTestTypeOfOperation.hashCode());
  }


  @Test
  public void testEquals() {
    assertEquals(isEqual, onTest);
    assertFalse(onTest.equals(null));
    assertNotEquals(notEqualDueToName, onTest);
    assertNotEquals(notEqualDueToTypeOfOperation, onTest);
    assertNotEquals(notEqualDueToAlias, onTest);

    assertEquals(isEqualAliased, onTestAliased);
    assertEquals(isEqualTypeOfOperation, onTestTypeOfOperation);
    assertNotEquals(differentName, onTestAliased);
    assertNotEquals(differentAlias, onTestAliased);
    assertNotEquals(differentTypeOfOperation, onTestAliased);
  }


  @Test
  public void testTypeOfOperationImmutability() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      // Should get the same object with immutable builders on
      assertEquals(isEqual.nextValue(), onTest.nextValue());
      assertEquals(isEqual.currentValue().nextValue(), onTest.nextValue());
      assertNotEquals(isEqual.nextValue(), onTest.currentValue());
      assertNotSame(onTest, onTest.nextValue());
    });
  }


  /**
   * Remove this test when immutability is permananently enabled.
   */
  @Test
  public void testTypeOfOperationMutablity() {
    AliasedField.withImmutableBuildersDisabled(() -> {
      // Should get the same object with immutable builders off
      assertSame(onTest, onTest.currentValue());
      assertSame(onTest, onTest.nextValue());
    });
  }


  @Test
  public void testDeepCopy() {
    AliasedField deepCopy = onTest.deepCopy();
    assertEquals(deepCopy, onTest);
    assertNotSame(deepCopy, onTest);
  }


  @Test
  public void testDeepCopyAliased() {
    AliasedField deepCopy = onTestAliased.deepCopy();
    assertEquals(deepCopy, onTestAliased);
    assertNotSame(deepCopy, onTestAliased);
  }

}
