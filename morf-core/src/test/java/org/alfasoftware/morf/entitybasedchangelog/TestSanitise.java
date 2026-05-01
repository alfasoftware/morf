package org.alfasoftware.morf.entitybasedchangelog;

import org.alfasoftware.morf.upgrade.HumanReadableStatementProducerUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestSanitise {

  @Test
  public void testSanitise() {
    // Given
    String version = "v5.7.24.r";
    // When
    String result = new HumanReadableStatementProducerUtils().sanitise(version);

    // Then
    Assert.assertEquals("5.7.24", result);
  }
}
