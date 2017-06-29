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

import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Base test class to implement functionality we are not testing.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
abstract class UpgradeStepTest implements UpgradeStep {
  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
   */
  @Override
  public String getDescription() {
    return "Test upgrade step";
  }

  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
   */
  @Override
  public String getJiraId() {
    return "SAMPLE-1";
  }
}