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

package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests {@link UpgradePath}.
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class TestUpgradePath {

  /**
   * Tests that upgrade script additions are appended to the upgrade path SQL.
   */
  @Test
  public void testUpgradeScriptAdditions() {
    Set<UpgradeScriptAddition> scriptAdditions = Sets.<UpgradeScriptAddition>newLinkedHashSet();

    scriptAdditions.add(createScriptAddition("ABC", "DEF"));
    scriptAdditions.add(createScriptAddition("GHI"));

    UpgradePath path = new UpgradePath(scriptAdditions, mock(SqlDialect.class));

    path.writeSql(ImmutableList.of("A", "B", "C"));

    assertEquals("SQL", ImmutableList.of("A", "B", "C", "ABC", "DEF", "GHI"), path.getSql());
  }


  private UpgradeScriptAddition createScriptAddition(String... sql) {
    UpgradeScriptAddition scriptAddition = mock(UpgradeScriptAddition.class);
    when(scriptAddition.sql()).thenReturn(Lists.newArrayList(sql));
    return scriptAddition;
  }
}
