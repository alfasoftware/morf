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

package org.alfasoftware.morf.directory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

/**
 * Provides streams for accessing XML data sets.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface DirectoryStreamProvider {

  /**
   * Opens or creates any resources required to provide content handlers.
   */
  void open();

  /**
   * Closes, releases or finalises any resources.
   */
  void close();

  /**
   * Provides input streams for reading data sets from XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  interface DirectoryInputStreamProvider extends DirectoryStreamProvider {

    /**
     * Provides an input stream to read XML for a specific table.
     *
     * @param tableName The table for which a content handler is requried.
     * @return An input stream from which the table XML can be read.
     */
    InputStream openInputStreamForTable(String tableName);

    /**
     * @return A collection of stream names that can be provided by calling {@link #openInputStreamForTable(String)}.
     */
    Collection<String> availableStreamNames();

    /**
     * Determines if a table exists.
     *
     * @param name The table name to be checked. The case of the name should be ignored.
     * @return True if a table <var>name</var> exists. False otherwise.
     */
    boolean tableExists(String name);
  }


  /**
   * Provides output streams for writing XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  interface DirectoryOutputStreamProvider extends DirectoryStreamProvider {

    /**
     * Provides an output stream to write XML for a specific table.
     *
     * <p><strong>Streams provided by this method must be closed in the normal
     * way by the caller.</strong></p>
     *
     * @param tableName The table for which a content handler is requried.
     * @return An output stream to which the table XML can be read.
     */
    OutputStream openOutputStreamForTable(String tableName);

    /**
     * Clear the destination of any previous output. Called after open().
     */
    void clearDestination();
  }
}
