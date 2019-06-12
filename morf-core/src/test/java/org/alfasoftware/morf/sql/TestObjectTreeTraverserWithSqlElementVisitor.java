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

import static org.alfasoftware.morf.sql.SqlUtils.bracket;
import static org.alfasoftware.morf.sql.SqlUtils.caseStatement;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.concat;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.merge;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.truncate;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.SqlUtils.when;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.eq;
import static org.alfasoftware.morf.sql.element.Criterion.exists;
import static org.alfasoftware.morf.sql.element.Criterion.not;
import static org.alfasoftware.morf.sql.element.Function.min;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Optional;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.junit.Test;

/**
 * Tests {@link ObjectTreeTraverser}.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestObjectTreeTraverserWithSqlElementVisitor {

  private final SqlElementCallback callback = mock(SqlElementCallback.class);
  private final ObjectTreeTraverser traverser = ObjectTreeTraverser.forCallback(callback);

  private final TableReference two = tableRef("two");
  private final TableReference three = tableRef("three");
  private final TableReference four = tableRef("four");
  private final TableReference five = tableRef("five");
  private final TableReference six = tableRef("six");
  private final TableReference seven = tableRef("seven");
  private final TableReference nine = tableRef("nine");
  private final TableReference ten = tableRef("ten");
  private final TableReference eleven = tableRef("eleven");
  private final TableReference twelve = tableRef("twelve");
  private final TableReference thirteen = tableRef("thirteen");


  /**
   * Tests simple join criteria, along with a criterion containing a subquery.
   */
  @Test
  public void testSimpleJoinCriterionWithSubquery() {
    SelectStatement select2 = select(field("b")).from(eleven);
    FieldFromSelect selectsAsField = (FieldFromSelect) select2.asField();
    Criterion crit1 = eq(seven.field("a"), ten.field("b"));
    Criterion crit2 = eq(seven.field("c"), selectsAsField);

    SelectStatement select1 = select().from(two).innerJoin(seven, crit1).innerJoin(seven, crit2);

    traverser.dispatch(select1);

    verify(callback,times(2)).visit(any(Join.class));
    verify(callback).visit(two);
    verify(callback,times(4)).visit(seven);
    verify(callback).visit(eleven);
    verify(callback).visit(ten);
    verify(callback).visit(crit1);
    verify(callback).visit(crit1.getField());
    verify(callback).visit(crit1.getValue());
    verify(callback).visit(crit2);
    verify(callback).visit(crit2.getField());
    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verify(callback).visit(selectsAsField);
    verify(callback).visit(select2.getFields().get(0));
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests group bys and havings.
   */
  @Test
  public void testGroupByAndHavingInNestedFunctions() {
    AliasedField field1 = field("a");
    AliasedField field2 = field("b");
    AliasedField field3 = field("c");
    AliasedField field4 = field("d");
    AliasedField field5 = field("e");
    AliasedField field6 = field("f");
    AliasedField field7 = field("h");
    AliasedField field8 = field("i");
    AliasedField field9 = field("j");

    SelectStatement select = select().from(eleven).groupBy(concat(field1, field2)).having(and(
      sum(
        caseStatement(
          when(literal(1).eq(field3)).then(bracket(new MathsField(cast(field5).asString(5), MathsOperator.DIVIDE, field6)))
        )
        .otherwise(min(field4))
      ).eq(field7),
      field1.in(field8, field9)
    ));

    traverser.dispatch(select);

    verify(callback, times(2)).visit(field1);
    verify(callback).visit(field2);
    verify(callback).visit(field3);
    verify(callback).visit(field4);
    verify(callback).visit(field5);
    verify(callback).visit(field6);
    verify(callback).visit(field7);
    verify(callback).visit(field8);
    verify(callback).visit(field9);
  }


  /**
   * Tests a subquery in the FROM part of the select statement.
   */
  @Test
  public void testFromSubquery() {
    SelectStatement select2 = select().from(three);

    SelectStatement select1 = select().from(select2);

    traverser.dispatch(select1);

    verify(callback).visit(three);
    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests a subquery in the WHERE part of the select statement.
   */
  @Test
  public void testWhereSubquery() {
    SelectStatement select5 = select(field("a")).from(thirteen);
    SelectStatement select4 = select(field("b")).from(twelve);
    SelectStatement select3 = select(field("c")).from(four);
    SelectStatement select2 = select(field("d")).from(select3);

    FieldFromSelect select4AsField = (FieldFromSelect) select4.asField();
    FieldFromSelect select5AsField = (FieldFromSelect) select5.asField();

    Criterion crit1 = eq(select4AsField, select5AsField);


    SelectStatement select1 = select(field("e")).from(select2).where(crit1);

    traverser.dispatch(select1);

    verify(callback).visit(select1);
    verify(callback).visit(select1.getFields().get(0));
    verify(callback).visit(select2);
    verify(callback).visit(select2.getFields().get(0));
    verify(callback).visit(select3);
    verify(callback).visit(select3.getFields().get(0));
    verify(callback).visit(select4);
    verify(callback).visit(select4.getFields().get(0));
    verify(callback).visit(select5);
    verify(callback).visit(select5.getFields().get(0));
    verify(callback).visit(select4AsField);
    verify(callback).visit(select5AsField);
    verify(callback).visit(crit1);
    verify(callback).visit(thirteen);
    verify(callback).visit(twelve);
    verify(callback).visit(four);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests a subquery in the SELECT part of the select statement.
   */
  @Test
  public void testFieldSubquery() {
    SelectStatement select3 = select().from(five);
    SelectStatement select2 = select().from(six).alias("thing");
    Criterion crit1 = eq(nine.field("a"), ten.field("b"));

    FieldFromSelect select2AsField = (FieldFromSelect) select2.asField();
    SelectStatement select1 = select(select2AsField).from(select3).where(crit1);

    traverser.dispatch(select1);

    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verify(callback).visit(select3);
    verify(callback).visit(select2AsField);
    verify(callback).visit(crit1);
    verify(callback).visit(crit1.getField());
    verify(callback).visit(crit1.getValue());
    verify(callback).visit(five);
    verify(callback).visit(six);
    verify(callback).visit(nine);
    verify(callback).visit(ten);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Ensures traversal of nested criteria.
   */
  @Test
  public void testNestedCriteria() {
    SelectStatement select5 = select().from(three);
    SelectStatement select4 = select().from(four);
    SelectStatement select3 = select().from(five);
    SelectStatement select2 = select().from(six);

    FieldFromSelect select4AsField = (FieldFromSelect) select4.asField();
    FieldFromSelect select5AsField = (FieldFromSelect) select5.asField();
    FieldFromSelect select2AsField = (FieldFromSelect) select2.asField();
    FieldFromSelect select3AsField = (FieldFromSelect) select3.asField();

    Criterion crit3 = eq(select4AsField, select5AsField);
    Criterion crit2 = eq(select2AsField, select3AsField);
    Criterion crit1 = and(crit2, crit3);

    SelectStatement select1 = select().from(two).where(crit1);

    traverser.dispatch(select1);

    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verify(callback).visit(select3);
    verify(callback).visit(select4);
    verify(callback).visit(select5);
    verify(callback).visit(select4AsField);
    verify(callback).visit(select5AsField);
    verify(callback).visit(select2AsField);
    verify(callback).visit(select3AsField);
    verify(callback).visit(crit1);
    verify(callback).visit(crit2);
    verify(callback).visit(crit3);
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(four);
    verify(callback).visit(five);
    verify(callback).visit(six);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests delegated traversal.
   */
  @Test
  public void testDriver() {
    final SelectStatement select2 = select().from(two);

    ObjectTreeTraverser.Driver driver = new ObjectTreeTraverser.Driver() {
      @Override
      public void drive(ObjectTreeTraverser traverser) {
        traverser.dispatch(select2);
      }
    };

    traverser.dispatch(driver);

    verify(callback).visit(driver);
    verify(callback).visit(select2);
    verify(callback).visit(two);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of a merge statement.
   */
  @Test
  public void testMerge() {
    final SelectStatement select = select(literal(1), literal(2)).from(two);
    final MergeStatement merge = merge().into(three).from(select);

    traverser.dispatch(merge);

    verify(callback).visit(merge);
    verify(callback).visit(select);
    verify(callback).visit(select.getFields().get(0));
    verify(callback).visit(select.getFields().get(1));
    verify(callback).visit(two);
    verify(callback).visit(three);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of a delete statement
   */
  @Test
  public void testDelete() {
    final SelectStatement select = select().from(two);
    final Criterion crit2 = exists(select);
    final Criterion crit1 = not(crit2);
    int limit = 1000;
    final DeleteStatement delete = DeleteStatement.delete(three).where(crit1).limit(limit).build();

    traverser.dispatch(delete);

    verify(callback).visit(delete);
    verify(callback).visit(crit1);
    verify(callback).visit(crit2);
    verify(callback).visit(select);
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(Optional.of(limit));
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of an update statement
   */
  @Test
  public void testUpdate() {
    final SelectStatement select2 = select(literal(1)).from(four);
    final SelectStatement select1 = select().from(two);
    final Criterion crit = exists(select1);
    final FieldFromSelect select2AsField = (FieldFromSelect) select2.asField().as("target");
    final UpdateStatement update = update(three).set(select2AsField).where(crit);

    traverser.dispatch(update);

    verify(callback).visit(update);
    verify(callback).visit(crit);
    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verify(callback).visit(select2.getFields().get(0));
    verify(callback).visit(select2AsField);
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(four);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of an insert statement inserting from a select
   */
  @Test
  public void testInsertFromSubSelect() {
    final SelectStatement select2 = select(literal(1)).from(four);
    final SelectStatement select1 = select().from(two);
    final FieldFromSelect select2AsField = (FieldFromSelect) select2.asField().as("target");
    final InsertStatement insert = insert().into(three).fields(select2AsField).from(select1);

    traverser.dispatch(insert);

    verify(callback).visit(insert);
    verify(callback).visit(select1);
    verify(callback).visit(select2);
    verify(callback).visit(select2AsField);
    verify(callback).visit(select2.getFields().get(0));
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(four);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of an insert statement inserting fixed values
   */
  @Test
  public void testInsertFromFieldsAndValues() {
    final SelectStatement select1 = select(literal(1)).from(two);
    final SelectStatement select2 = select(literal(2)).from(four);

    final FieldFromSelect select1AsField = (FieldFromSelect) select1.asField().as("a");
    final FieldFromSelect select2AsField = (FieldFromSelect) select2.asField().as("b");

    final InsertStatement insert = insert()
        .into(three)
        .fields(field("a"), field("b"))
        .values(select1AsField)
        .withDefaults(select2AsField);

    traverser.dispatch(insert);

    verify(callback).visit(insert);
    verify(callback).visit(select1);
    verify(callback).visit(select1.getFields().get(0));
    verify(callback).visit(select2);
    verify(callback).visit(select2.getFields().get(0));
    verify(callback).visit(select1AsField);
    verify(callback).visit(select2AsField);
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(four);
    verify(callback).visit(insert.getFields().get(0));
    verify(callback).visit(insert.getFields().get(1));
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of an insert statement inserting fixed values (method 2)
   */
  @Test
  public void testInsertFromFields() {
    final SelectStatement select1 = select(literal(1)).from(two);
    final SelectStatement select2 = select(literal(2)).from(four);

    final FieldFromSelect select1AsField = (FieldFromSelect) select1.asField().as("a");
    final FieldFromSelect select2AsField = (FieldFromSelect) select2.asField().as("b");

    final InsertStatement insert = insert()
        .into(three)
        .fields(select1AsField, select2AsField);

    traverser.dispatch(insert);

    verify(callback).visit(insert);
    verify(callback).visit(select1);
    verify(callback).visit(select1.getFields().get(0));
    verify(callback).visit(select2);
    verify(callback).visit(select2.getFields().get(0));
    verify(callback).visit(select1AsField);
    verify(callback).visit(select2AsField);
    verify(callback).visit(two);
    verify(callback).visit(three);
    verify(callback).visit(four);
    verifyNoMoreInteractions(callback);
  }


  /**
   * Tests traversal of a truncate statement.
   */
  @Test
  public void testTruncate() {
    final TableReference table = tableRef("Foo");
    final TruncateStatement truncate = truncate(table);

    traverser.dispatch(truncate);

    verify(callback).visit(table);
    verify(callback).visit(truncate);
    verifyNoMoreInteractions(callback);
  }
}
