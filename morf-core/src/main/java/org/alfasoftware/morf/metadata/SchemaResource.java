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

import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.TableCollectionSupplier;

/**
 * Provides database meta data.
 *
 * <p>Like other database resources, schema can implement a
 * {@link #close()} operation. Consumers of this resource are obliged to
 * call {@link #close()} to guarantee all resources are released,
 * or use try-with-resources.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface SchemaResource extends Schema, AutoCloseable {

  /**
   * Closes the resource.
   *
   * <p>No specific checked exception is thrown by implementers.</p>
   */
  @Override
  void close();


  /**
   * Returns the underlying {@link DatabaseMetaDataProvider}, if there is one.
   *
   * @return the underlying {@link DatabaseMetaDataProvider}, if there is one.
   */
  default Optional<DatabaseMetaDataProvider> getDatabaseMetaDataProvider() {
    return Optional.empty();
  }

  /**
   * Introduced to allow access to meta-data when using auto-healing with Oracle. See MORF-98.
   * The schema should not normally be accessed directly.
   * @return The delegate schema.
   */
  default Optional<TableCollectionSupplier> getTableCollectionSupplier() {
    return Optional.empty();
  }
}
