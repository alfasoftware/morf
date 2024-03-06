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


import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for for {@link SequenceBean}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestSequenceBean {

  @Test
  public void testConstructors() {
    Sequence sequence;
    Sequence sequence2 = new SequenceBean("a", 1, true);
    Sequence sequence3 = new SequenceBean("c", null, false);

    sequence = new SequenceBean(sequence2);
    assertEquals("Test 1 name successful",sequence2.getName(), sequence.getName());
    assertTrue("Test 1 knows starts with flag successful", sequence.knowsStartsWith());
    assertEquals("Test 1 starts with successful",sequence2.getStartsWith(), sequence.getStartsWith());
    assertEquals("Test 1 temporary flag successful",sequence2.isTemporary(), sequence.isTemporary());

    assertEquals("toString() correct", "Sequence-a", sequence.toString());

    sequence = new SequenceBean("b", 10, false);
    assertEquals("Test 2 name successful", "b", sequence.getName());
    assertTrue("Test 2 knows starts with flag successful", sequence.knowsStartsWith());
    assertEquals("Test 2 starts with successful", 10, sequence.getStartsWith().intValue());
    assertFalse("Test 2 temporary flag successful", sequence.isTemporary());

    assertEquals("toString() correct", "Sequence-b", sequence.toString());

    sequence = new SequenceBean(sequence3);
    assertEquals("Test 3 name successful", "c", sequence.getName());
    assertFalse("Test 3 knows starts with flag successful", sequence.knowsStartsWith());
    assertNull("Test 3 starts with successful", sequence.getStartsWith());
    assertFalse("Test 3 temporary flag successful", sequence.isTemporary());

    assertEquals("toString() correct", "Sequence-c", sequence.toString());

    sequence = new SequenceBean("d", null, false);
    assertEquals("Test 4 name successful", "d", sequence.getName());
    assertFalse("Test 4 knows starts with flag successful", sequence.knowsStartsWith());
    assertNull("Test 4 starts with successful", sequence.getStartsWith());
    assertFalse("Test 4 temporary flag successful", sequence.isTemporary());

    assertEquals("toString() correct", "Sequence-d", sequence.toString());
  }

}
