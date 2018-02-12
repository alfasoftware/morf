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

package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Test {@link SelectFirstStatement} element generation
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
@RunWith(MockitoJUnitRunner.class)
public class TestSelectFirstElementGeneration {

  @Mock
  private AliasedField field;

  @Mock
  private TableReference table;

  @Mock
  private Criterion criterion;


  /**
   * Test if all the fields get populated.
   */
  @Test
  public void testConstruction() {
    SelectFirstStatement selectFirst = selectFirst(field).from(table).where(criterion).orderBy(field);

    assertEquals("should be one field selected",1,selectFirst.getFields().size());
    assertEquals("should be field set",field,selectFirst.getFields().get(0));
    assertEquals("table should be set",table,selectFirst.getTable());
    assertEquals("criterion should be set",criterion,selectFirst.getWhereCriterion());
  }


  /**
   * Test deep copy
   */
  @Test
  public void testDeepCopy() {
    SelectFirstStatement selectFirst = selectFirst(SqlUtils.field("field1")).from(tableRef("table")).where(criterion).orderBy(SqlUtils.field("field1"));

    when(criterion.deepCopy(any(DeepCopyTransformation.class))).thenReturn(TempTransitionalBuilderWrapper.wrapper(criterion));

    SelectFirstStatement deepCopy = selectFirst.deepCopy();

    assertEquals("should be one field selected",1,deepCopy.getFields().size());
    assertEquals("should be field set","field1",deepCopy.getFields().get(0).getImpliedName());
    assertFalse("getFields should return new list",deepCopy.getFields()==selectFirst.getFields());
    assertEquals("table should be set","table",deepCopy.getTable().getName());
    assertEquals("criterion should be set",criterion,deepCopy.getWhereCriterion());
  }


  /**
   * We have an immutable and mutable mode - make sure this behaves correctly.
   */
  @Test
  public void testFieldsMutable() {
    FieldLiteral field1 = literal(1);
    FieldLiteral field2 = literal(2);
    SelectFirstStatement select = selectFirst(field1);
    select.getFields().clear();
    select.getFields().add(field2);
    assertThat(select.getFields(), contains(field2));
  }


  /**
   * We have an immutable and mutable mode - make sure this behaves correctly.
   */
  @Test
  public void testFieldsImmutable() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      FieldLiteral field1 = literal(1);
      FieldLiteral field2 = literal(2);
      FieldLiteral field3 = literal(3);

      // Can't mutate
      SelectFirstStatement select = selectFirst(field1);
      assertUnsupported(() -> select.getFields().add(field2));

      // But we can make a copy and extend that, and the copy is also unmodifiable.
      SelectFirstStatement copy = select.shallowCopy().fields(field2).build();
      assertThat(copy.getFields(), contains(field1, field2));
      assertUnsupported(() -> copy.getFields().add(field3));
    });
  }


  private void assertUnsupported(Runnable runnable) {
    try {
      runnable.run();
    } catch (UnsupportedOperationException w) {
      return;
    }
    fail("Did not catch UnsupportedOperationException");
  }
}