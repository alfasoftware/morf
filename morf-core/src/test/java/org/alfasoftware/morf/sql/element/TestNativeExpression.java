package org.alfasoftware.morf.sql.element;

import java.util.List;

import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;

import static org.alfasoftware.morf.sql.SqlUtils.CaseStatementBuilder.nativeSql;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link NativeExpression}s
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2026
 */
@RunWith(Parameterized.class)
public class TestNativeExpression extends AbstractAliasedFieldTest<NativeExpression> {

  @Mock
  private UpgradeTableResolutionVisitor res;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> data() {
    return ImmutableList.of(
        testCase(
            "Test 1",
            () -> nativeSql("some native sql"),
            () -> nativeSql("0"),
            () -> nativeSql("true")
        )
    );
  }


  /**
   * Verify that deep copy actually copies the individual properties.
   */
  @Test
  public void testDeepCopyDetail() {
    NativeExpression ne1 = (NativeExpression) onTestAliased;
    NativeExpression neCopy = (NativeExpression)onTestAliased.deepCopy();

    assertEquals("Native expression matches", ne1.getExpression(), neCopy.getExpression());
    assertEquals("Native expression alias matches", ne1.getAlias(), neCopy.getAlias());
  }
}
