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

import org.alfasoftware.morf.metadata.Table;

/**
 * Defines the contract for transmitting a data set.
 *
 * <p>Methods are expected to fire in the following order:</p>
 * <blockquote><pre><code>open();
 * table(...);
 * table(...);
 * table(...);
 * close();</code></pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface DataSetConsumer {

  /**
   * Indicates the state of the producing system at the time of the close.
   */
  // Consider API design of CloseState enum
  public enum CloseState {
    /**
     * The dataset completed successfully
     */
    COMPLETE,
    /**
     * The dataset did not completed successfully
     */
    INCOMPLETE
  }

  /**
   * Opens the consumer.
   */
  public void open();


  /**
   * Closes the consumer.
   *
   * <p>This method is invoked only once and it will be the last method invoked in this interface. It should
   * <em>always</em> be called, even if something went wrong. This allows the constumer to clean up resources.</p>
   *
   * <p>If the dataset wasn't completed successfully, indicate this with the closeState parameter.</p>
   *
   * @param closeState The state when dataset was closed - indicates whether all tables were imported.
   */
  public void close(CloseState closeState);


  /**
   * Receives a table and its data.
   *
   * @param table Meta data for the table.
   * @param records The data.
   */
  public void table(Table table, Iterable<Record> records);

}
