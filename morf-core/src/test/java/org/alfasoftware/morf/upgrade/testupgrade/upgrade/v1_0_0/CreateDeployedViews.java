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

package org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.TestUpgrade;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * A simple upgrade step to create {@code DeployedViews}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
@Sequence(1354537217)
@UUID("328e3a46-a0db-4544-9568-4b2286c93366")
public final class CreateDeployedViews implements UpgradeStep {
  @Override public String getJiraId() { return "WEB-18348"; }
  @Override public String getDescription() { return "Foo"; }
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addTable(TestUpgrade.deployedViews());
  }
}