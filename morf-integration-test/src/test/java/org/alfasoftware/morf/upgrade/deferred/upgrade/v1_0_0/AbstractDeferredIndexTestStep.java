/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0;

import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Base class for deferred-index integration test upgrade steps.
 */
abstract class AbstractDeferredIndexTestStep implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "DEFERRED-000";
  }


  @Override
  public String getDescription() {
    return "";
  }
}
