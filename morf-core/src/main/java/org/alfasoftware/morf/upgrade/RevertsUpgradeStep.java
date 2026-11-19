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

package org.alfasoftware.morf.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Restricts an upgrade step to run only if the referenced upgrade step was already
 * applied before the current upgrade began.
 *
 * <p>For example, {@code @RevertsUpgradeStep("9823a150-2578-11e0-ac64-0800200c9a66")}
 * skips the annotated step unless that UUID is present in the applied upgrade history.
 * The referenced step need not still be available in the application. If both the
 * original and the annotated reversal are pending in the same upgrade, they cancel
 * each other and neither is executed. This also prevents their {@link OnlyWith}
 * dependants from running.</p>
 *
 * <p>This annotation only controls eligibility. The annotated step must implement the
 * schema or data changes which undo the original step. Normal ordering, already-applied
 * checks and any {@link OnlyWith} restriction still apply.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RevertsUpgradeStep {

  /**
   * @return the string representation of the UUID of the previously applied step to revert;
   *         must be a valid, non-blank UUID
   */
  String value();
}
