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
import static org.alfasoftware.morf.sql.element.Direction.ASCENDING;
import static org.alfasoftware.morf.sql.element.Direction.DESCENDING;
import static org.alfasoftware.morf.sql.element.FieldReference.field;
import static org.alfasoftware.morf.sql.element.NullValueHandling.FIRST;
import static org.alfasoftware.morf.sql.element.NullValueHandling.LAST;
import static org.alfasoftware.morf.sql.element.NullValueHandling.NONE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;

/**
 * Tests for field literals
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@RunWith(Parameterized.class)
public class TestFieldReference extends AbstractAliasedFieldTest<FieldReference> {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    TableReference tableRef1 = mockTableReference();
    TableReference tableRef2 = mockTableReference();
    return ImmutableList.of(
      testCase(
          "Deprecated constructors",
          () -> new FieldReference(tableRef1, "date", ASCENDING, FIRST),
          () -> new FieldReference(tableRef1, "date", ASCENDING, LAST),
          () -> new FieldReference(tableRef1, "date", ASCENDING, NONE),
          () -> new FieldReference(tableRef1, "date", DESCENDING, FIRST),
          () -> new FieldReference(tableRef1, "notDate", ASCENDING, FIRST),
          () -> new FieldReference(tableRef2, "date", ASCENDING, FIRST)
      ),
      testCase(
          "Builders",
          () -> field(tableRef1, "date").asc().nullsFirst().build(),
          () -> field(tableRef1, "date").asc().nullsLast().build(),
          () -> field(tableRef1, "date").asc().build(),
          () -> field(tableRef1, "date").desc().nullsFirst().build(),
          () -> field(tableRef1, "notDate").asc().nullsFirst().build(),
          () -> field(tableRef2, "date").asc().nullsFirst().build()
      ),
      testCase(
          "No table reference",
          () -> field("date").asc().nullsFirst().build(),
          () -> field("date").asc().nullsLast().build(),
          () -> field("date").asc().build(),
          () -> field("date").desc().nullsFirst().build(),
          () -> field("notDate").asc().nullsFirst().build(),
          () -> field(tableRef1, "date").asc().nullsFirst().build()
      )
    );
  }


  /**
   * Verify that deep copy works as expected with respect to the individual fields.
   */
  @Test
  public void testDeepCopyDetail() {
    FieldReference fr = (FieldReference) FieldReference.field(mockTableReference(), "TEST1").build().as("testName");
    FieldReference frCopy = (FieldReference)fr.deepCopy();
    assertEquals(fr.getTable(), frCopy.getTable());
    assertEquals(fr.getName(), frCopy.getName());
    assertEquals(fr.getAlias(), frCopy.getAlias());
    assertEquals(fr.getDirection(), frCopy.getDirection());
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    FieldReference onTest = new FieldReference(tableRef("table1"), "x");

    //when
    onTest.accept(res);

    //then
    verify(res).visit(onTest);
  }
}
