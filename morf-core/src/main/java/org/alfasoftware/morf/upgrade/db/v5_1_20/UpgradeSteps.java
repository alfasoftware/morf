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

package org.alfasoftware.morf.upgrade.db.v5_1_20;

import java.util.List;

import org.alfasoftware.morf.upgrade.UpgradeStep;

import com.google.common.collect.ImmutableList;

/**
 * Provide access to the package-visible upgrade steps in this package.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class UpgradeSteps {

  /**
   * Upgrade steps.
   */
  public static final List<Class<? extends UpgradeStep>> LIST = ImmutableList.<Class<? extends UpgradeStep>> of(

    /*
     * The UpgradeAudit table was modified in 5.1.20 however it does not make sense to include
     * it in the upgrade steps because any installation preceding v5.1.20 will
     * not be able to use this step as the UpgradeAudit table has to be manually changed to match
     * the trunk in order to run <em>any</em> upgrade steps. See WEB-20215.

      RefactorRunDateToAppliedTimeInUpgradeAudit.class
     */
  );
}
