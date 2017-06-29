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

package org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/***/
@Sequence(2)
@UUID("5f91f501-6b2d-4e2f-9462-ae0955bfa491") // This is the UUID of WidenIndustryCodeUpgrade
public final class MockWidenIndustryCodeUpgrade extends UpgradeStepTest {

  /**
   * @see UpgradeStep#execute(SchemaEditor, DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
  }
}