package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.MergeStatement.InputField.inputField;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.alfasoftware.morf.sql.MergeMatchClause.MatchAction;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link MergeMatchClause}
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class TestMergeMatchClause {

  @Mock
  private Criterion criterion;

  @Mock
  private Criterion copiedCriterion;

  @Mock
  private DeepCopyTransformation transformer;

  @Mock
  private ObjectTreeTraverser traverser;

  @Mock
  private SchemaAndDataChangeVisitor visitor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void testUpdateWithoutWhereClause() {
    // when
    MergeMatchClause clause = MergeMatchClause.update().build();

    // then
    assertEquals(MatchAction.UPDATE, clause.getAction());
    assertFalse(clause.getWhereClause().isPresent());
  }


  @Test
  public void testUpdateWithWhereClause() {
    // when
    MergeMatchClause clause = MergeMatchClause.update()
        .onlyWhere(criterion)
        .build();

    // then
    assertEquals(MatchAction.UPDATE, clause.getAction());
    assertTrue(clause.getWhereClause().isPresent());
    assertSame(criterion, clause.getWhereClause().get());
  }


  @Test
  public void testDeepCopy() {
    // given
    MergeMatchClause original = MergeMatchClause.update().build();

    // when
    MergeMatchClause copy = original.deepCopy(transformer).build();

    // then
    assertNotSame(original, copy);
    assertEquals(original.getAction(), copy.getAction());
    assertFalse(copy.getWhereClause().isPresent());
    verifyNoInteractions(transformer);
  }


  @Test
  public void testShallowCopy() {
    // given
    MergeMatchClause original = MergeMatchClause.update()
        .onlyWhere(criterion)
        .build();

    // when
    MergeMatchClause copy = original.shallowCopy().build();

    // then
    assertNotSame(original, copy);
    assertEquals(original.getAction(), copy.getAction());
    assertTrue(copy.getWhereClause().isPresent());
    assertSame(criterion, copy.getWhereClause().get()); // shallow copy shares the same criterion
  }


  @Test
  public void testDriveWithoutWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update().build();

    // when
    clause.drive(traverser);

    // then
    verifyNoInteractions(traverser);
  }


  @Test
  public void testDriveWithWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update()
        .onlyWhere(criterion)
        .build();

    // when
    clause.drive(traverser);

    // then
    verify(traverser).dispatch(criterion);
  }


  @Test
  public void testEqualsAndHashCode() {
    // given
    MergeMatchClause clause1 = MergeMatchClause.update().onlyWhere(criterion).build();
    MergeMatchClause clause2 = MergeMatchClause.update().onlyWhere(criterion).build();
    MergeMatchClause clause3 = MergeMatchClause.update().build();
    MergeMatchClause clause4 = MergeMatchClause.update().onlyWhere(copiedCriterion).build();

    // then
    assertEquals(clause1, clause1);
    assertEquals(clause1, clause2);
    assertEquals(clause2, clause1);
    assertEquals(clause1.hashCode(), clause2.hashCode());

    assertNotEquals(clause1, clause3);
    assertNotEquals(clause1, clause4);
    assertNotEquals(clause1, null);
    assertNotEquals(clause1, new Object());
  }


  @Test
  public void testToStringWithoutWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update().build();

    // when
    String result = clause.toString();

    // then
    assertEquals("UPDATE", result);
  }


  @Test
  public void testToStringWithWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update()
        .onlyWhere(field("testField").neq(inputField("rate")))
        .build();

    // when
    String result = clause.toString();

    // then
    assertEquals("UPDATE WHERE testField <> newValue(rate)", result);
  }


  @Test
  public void testAcceptVisitorWithoutWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update().build();

    // when
    clause.accept(visitor);

    // then
    verify(visitor).visit(clause);
  }


  @Test
  public void testAcceptVisitorWithWhereClause() {
    // given
    MergeMatchClause clause = MergeMatchClause.update()
        .onlyWhere(criterion)
        .build();

    // when
    clause.accept(visitor);

    // then
    verify(visitor).visit(clause);
    verify(criterion).accept(visitor);
  }

}