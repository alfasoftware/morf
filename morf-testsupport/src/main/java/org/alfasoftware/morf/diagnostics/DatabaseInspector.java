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

package org.alfasoftware.morf.diagnostics;

import com.google.inject.ImplementedBy;

/**
 * An inspection facility for viewing and querying the database during testing.
 *
 * <p>Declare and inspector in your test with {@code &amp;Inject DatabaseInspector inspector;}</p>
 *
 * <p>Use by adding {@code inspector.inspect()} and a breakpoint afterwards
 * (the inspector being open will not pause execution).</p>
 *
 * <p>Do not commit tests with {@code inspector.inspect()} calls in!</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@ImplementedBy(H2DatabaseInspector.class)
public interface DatabaseInspector {

  /**
   * Launch the inspector to view the database.
   */
  public void inspect();
}
