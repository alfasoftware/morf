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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Puts a restriction on an upgrade step so that it can only be executed along with another upgrade step.
 *
 * <p>
 * The annotated step can come either earlier or later in the sequence of upgrade steps than the referenced
 * upgrade step. The ordering of upgrade steps is fully respected, there are no restrictions
 * on sequence numbers of upgrade steps liked together with this annotation.</p>
 *
 * <p>
 * Examples:
 *</p>
 *  <ul>
 *   <li>given two steps, unapplied A and unapplied B, and step B annotated with <code>OnlyWith("UUID_of_step_A")</code>,
 *   when one runs the database upgrade, then both steps will be applied</li>
 *   <li>given two steps, applied A and unapplied B, and step B annotated with <code>OnlyWith("UUID_of_step_A")</code>,
 *   when one runs the database upgrade, none of the steps will be applied</li>
 *  </ul>
 *
 * The safe uses of the {@link OnlyWith}:
 *  <ul>
 *   <li>making a retrospective migration of the optional table, a migration that uses temporary or only-then existing tables
 *    - use it on the new optional (located in optional module) upgrade step to extend the behaviour of the other upgrade step</li>
 *   <li>moving tables from core to optional modules
 *    - split the existing core upgrade step into its core and optional parts, annotate the optional part to be executed only with its core part.</li>
 *  </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OnlyWith {

  /**
   * @return the UUID of the other upgrade step; if the upgrade step with the given UUID does not exist, the annotated upgrade step will fail
   */
  String value();

}