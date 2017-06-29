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

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Function.floor;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

import org.alfasoftware.morf.sql.SqlUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests the WindowFunction DSL
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestWindowFunctions {

  private final FieldReference field1 = tableRef("table1").field("field1");
  private final FieldReference field2Asc = tableRef("table1").field("field2").asc();
  private final FieldReference field3Desc = field("field3").desc();
  private final FieldReference field4NoOrder = tableRef("table1").field("field4");


  /**
   * Test exception is thrown when an unsupported function type is used.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFunctionException() {
    SqlUtils.windowFunction(floor(field1)).build();
  }


  /**
   * Test exception is thrown when an empty list of fields is passed as a partition by list.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNullPartitionByException() {
    SqlUtils.windowFunction(sum(field1)).partitionBy().build();
  }


  /**
   * Test exception is thrown when an empty list of fields is passed as a order by list.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNullOrderByException() {
    SqlUtils.windowFunction(sum(field1)).orderBy().build();
  }


  /**
   * Test deep copying does not copy instances.
   */
  @Test
  public void testDeepCopy() {
    WindowFunction windowFunction = (WindowFunction) SqlUtils.windowFunction(sum(field1))
                                    .partitionBy(field2Asc)
                                    .orderBy(field3Desc)
                                    .build()
                                    .as("windowFunction1");

    WindowFunction copy = (WindowFunction) windowFunction.deepCopy();

    assertNotSame(copy.getOrderBys(),windowFunction.getOrderBys());
    assertNotSame(copy.getPartitionBys(),windowFunction.getPartitionBys());
    assertEquals(copy.getOrderBys(),windowFunction.getOrderBys());
    assertEquals(copy.getPartitionBys(),windowFunction.getPartitionBys());
    assertEquals(copy.getAlias(),windowFunction.getAlias());
  }


  /**
   * Test the builder pattern preserves previous calls.
   */
  @Test
  public void testMultipleBuilderCalls() {
    WindowFunction windowFunction = (WindowFunction) SqlUtils.windowFunction(sum(field1))
                                    .partitionBy(field2Asc)
                                    .partitionBy(Lists.newArrayList(field3Desc))
                                    .orderBy(field3Desc)
                                    .orderBy(Lists.newArrayList(field4NoOrder))
                                    .build();


    assertThat(windowFunction.getOrderBys(),hasSize(2));
    assertThat(windowFunction.getPartitionBys(),hasSize(2));

  }


  /**
   * Tests order by direction defaulting.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testOrderDefaulting() {

    field4NoOrder.setDirection(Direction.NONE);

    WindowFunction windowFunction = (WindowFunction) SqlUtils.windowFunction(sum(field1))
                                    .orderBy(field2Asc,field3Desc,field4NoOrder)
                                    .build();


    assertThat(windowFunction.getOrderBys(),hasSize(3));
    assertThat(windowFunction.getOrderBys(),contains(fieldReferenceWithNameAndDirection("field2",Direction.ASCENDING),
                                                    fieldReferenceWithNameAndDirection("field3",Direction.DESCENDING),
                                                    fieldReferenceWithNameAndDirection("field4",Direction.ASCENDING)));
  }


  private Matcher<AliasedField> fieldReferenceWithNameAndDirection(final String name,final Direction direction){
    return new TypeSafeMatcher<AliasedField>() {

      @Override
      public void describeTo(Description description) {
        description.appendText("Name:").appendValue(name).appendText(" Direction:").appendValue(direction);
      }


      @Override
      protected boolean matchesSafely(AliasedField item) {
        return item instanceof FieldReference && ((FieldReference) item).getDirection() == direction && name.equals(((FieldReference) item).getName()) ;
      }
    };
  }
}