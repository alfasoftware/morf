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

package org.alfasoftware.morf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.guicesupport.MorfModule;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.TableContribution;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.junit.Rule;
import org.junit.Test;

import com.google.inject.Inject;

/**
 * Tests expected behaviour of Guice bindings defined in {@link MorfModule}
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestMorfModule {
  @Rule
  public InjectMembersRule injectMembersRule = new InjectMembersRule(new MorfModule(), new TestingDataSourceModule());

  @Inject
  private Set<UpgradeScriptAddition> upgradeScriptAdditions;

  @Inject
  private Set<TableContribution> tableContributions;

  @Test
  public void testUpgradeScriptAdditionSetBinding() {
    assertNotNull("UpgradeScriptAddition set not null", upgradeScriptAdditions);
    assertTrue("UpgradeScriptAddition set is empty", upgradeScriptAdditions.isEmpty());
  }

  @Test
  public void testTableContributionSetBinding() {
    assertNotNull("TableContribution set not null", tableContributions);
    assertFalse("TableContribution set is not empty", tableContributions.isEmpty());
    assertEquals("Just one class is bound to TableContribution set", 1, tableContributions.size());
  }
}
