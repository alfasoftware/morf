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

package org.alfasoftware.morf.dataset;

import org.alfasoftware.morf.metadata.Schema;

/**
 * Defines a data set source.
 *
 * <p>The {@link #close()} method must be called when the data set is no longer required in
 * order to release resources.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 * @see DataSetConsumer
 */
public interface DataSetProducer {

  /**
   * Opens the data set for reading. Implementations should expect to receive
   * exactly one call to this method at the start of processing.
   */
  public void open();


  /**
   * Closes the data set once reading is complete. Implementations should expect to receive
   * exactly one call to this method at the end of processing.
   */
  public void close();


  /**
   * Provides meta data for the source data set.
   *
   * @return the meta data for the table currently being read.
   */
  public Schema getSchema();


  /**
   * Access the data for the table <var>tableName</var>.
   *
   * <p><strong>Important:</strong> The {@link Record} objects returned by the iterator may be re-used by the provider
   * implementation for performance. Consumers should not hold references to them once next() has been called.</p>
   *
   * @param tableName The table to get records from.
   * @return The records.
   */
  public Iterable<Record> records(String tableName);


  /**
   * @param tableName The table to check
   * @return Whether the specified table is empty.
   */
  public boolean isTableEmpty(String tableName);
}
