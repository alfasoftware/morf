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

package org.alfasoftware.morf.metadata;

/**
 * Provides database meta data.
 *
 * <p>Like other database resources sche can implement a
 * {@link #close()} operation. Consumers of this resource are obliged to
 * call {@link #close()} to guarantee all resources are released.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface SchemaResource extends Schema {

  /**
   * Closes the resource.
   */
  public void close();

}
