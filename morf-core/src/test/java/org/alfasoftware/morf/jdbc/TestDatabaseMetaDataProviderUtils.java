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

package org.alfasoftware.morf.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link DatabaseMetaDataProviderUtils}.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestDatabaseMetaDataProviderUtils {

  @Test
  public void testGetAutoIncrementStartValue() {
    assertEquals("Failed to fetch correct value", 1234, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue("AUTONUMSTART:[1234]"));
    assertEquals("Failed to fetch correct value from large string", 1234, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue(" dfdfg AUTONUMSTART:[1234] dsfdfg"));
    assertEquals("Failed to fetch correct value when multiple values exist", 1234, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue("AUTONUMSTART:[1234]/AUTONUMSTART:[2345]"));
    assertEquals("Failed to fetch unset value", -1, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue("AUTONUMSTART:[1234"));
    assertEquals("Failed to fetch missing value", -1, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue("AUTONUMSTART:[]"));
    assertEquals("Failed to fetch malformed value", -1, DatabaseMetaDataProviderUtils.getAutoIncrementStartValue("AUTONUMSTART:[ABC]"));
  }

  @Test
  public void shouldIgnoreIndex() {
    assertFalse(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_1"));
    assertFalse(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Fred"));
    assertFalse(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_PRF")); // missing digit
    assertFalse(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_xPRF")); // missing underscore before PRF
    assertFalse(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_PRF1X")); // Random character at the end

    assertTrue(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_PRF0"));
    assertTrue(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Schedule_PRF1"));
    assertTrue(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Fred_PRF2"));
    assertTrue(DatabaseMetaDataProviderUtils.shouldIgnoreIndex("Fred_prf2")); // everything has to be case-insensitive
  }
}
