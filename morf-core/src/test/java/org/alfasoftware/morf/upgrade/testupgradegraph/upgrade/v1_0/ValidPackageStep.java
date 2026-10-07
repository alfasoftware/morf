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

package org.alfasoftware.morf.upgrade.testupgradegraph.upgrade.v1_0;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Test upgrade step with valid package-based version (v1_0).
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
@Sequence(2000)
public class ValidPackageStep implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "TEST-PKG-1-0";
  }

  @Override
  public String getDescription() {
    return "Valid package step for v1.0";
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // No-op
  }
}
