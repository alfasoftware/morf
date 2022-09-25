package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link MinusSetOperator}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestMinusSetOperator {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if the {@code parentSelect} reference
   * is null.
   */
  @Test
  public void testUnionSetOperatorWithNullParentSelect() {
    // Given
    SelectStatement parentSelect = null;
    SelectStatement childSelect = new SelectStatement();

    // When
    assertThrows(IllegalArgumentException.class, () -> new MinusSetOperator(parentSelect, childSelect));

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if the {@code childSelect} reference
   * is null.
   */
  @Test
  public void testConstructorWithNullChildSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement();
    SelectStatement childSelect = null;

    // When
    assertThrows(IllegalArgumentException.class, () -> new MinusSetOperator(parentSelect, childSelect));

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if {@code parentSelect} and
   * {@code childSelect} number of fields don't match.
   */
  @Test
  public void testConstructorWithDifferentNumberFields() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"), new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF"));

    // When
    assertThrows(IllegalArgumentException.class, () -> new MinusSetOperator(parentSelect, childSelect));

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if {@code parentSelect} and
   * {@code childSelect} number of fields don't match.
   */
  @Test
  public void testConstructorWithSameNumberOfFields() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF"));

    // When
    new MinusSetOperator(parentSelect, childSelect);

    // Then
    // no exceptions
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor does not throw an
   * exception when the {@code childSelect} parameter contains an order-by
   * statement.
   */
  @Test
  public void testConstructorWithSortedChildSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF")).orderBy(new FieldReference("XYZ"));

    // When
    new MinusSetOperator(parentSelect, childSelect);

    // Then
    // no exceptions
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator} constructor allows the
   * creation of a union where the {@code parentSelect} contains an order-by
   * statement.
   */
  @Test
  public void testConstructorWithSortedParentSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement().orderBy(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement();

    // When
    new MinusSetOperator(parentSelect, childSelect);

    // Then
    // no exceptions
  }


  /**
   * Verifies that the {@linkplain MinusSetOperator#deepCopy()} method returns a
   * new instance of the original object, but with different objects.
   */
  @Test
  public void testDeepCopy() {
    // Given
    SelectStatement parentSelect = new SelectStatement();
    SelectStatement childSelect = new SelectStatement();
    MinusSetOperator original = new MinusSetOperator(parentSelect, childSelect);

    // When
    MinusSetOperator copy = (MinusSetOperator) original.deepCopy(noTransformation()).build();

    // Then
    assertNotSame(original, copy);
    assertNotSame(childSelect, copy.getSelectStatement());
  }


  /**
   * Verifies that UpgradeTableResolutionVisitor interacts with the
   * MinusSetOperator as expected.
   */
  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    SelectStatement parentSelect = mock(SelectStatement.class);
    SelectStatement childSelect = mock(SelectStatement.class);
    MinusSetOperator original = new MinusSetOperator(parentSelect, childSelect);

    //when
    original.accept(res);

    //then
    verify(res).visit(original);
    verify(childSelect).accept(res);
  }

}