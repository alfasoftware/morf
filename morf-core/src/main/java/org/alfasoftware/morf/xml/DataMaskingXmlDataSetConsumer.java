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

package org.alfasoftware.morf.xml;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlOutputStreamProvider;

/**
 * Implementation of {@linkplain DataSetConsumer} which discards specific fields when consuming
 * a data set.
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class DataMaskingXmlDataSetConsumer extends XmlDataSetConsumer {

  private final Map<String, Set<String>> tableColumnsToMask;

  /**
   * Creates a data set consumer that will pipe the data set to the file system location
   * specified by <var>file</var>.
   *
   * <p>The serialised output can be written to a single archive or multiple data files:</p>
   * <ul>
   * <li>If <var>file</var> identifies a directory then each table in the data set is
   * serialised to a separate XML file within that directory.</li>
   * <li>If <var>file</var> identifies a file name then the file will be created or replaced with
   * a zip archive containing one XML file per table in the data set.</li>
   * </ul>
   *
   * @param file The file system location to receive the data set.
   * @param tableColumnsToMask the table columns to mask when consuming a dataset
   */
  public DataMaskingXmlDataSetConsumer(File file, Map<String, Set<String>> tableColumnsToMask) {
    super(file);
    this.tableColumnsToMask = tableColumnsToMask;
  }


  /**
   * Creates a data set consumer that will pipe the data set to the streams
   * returned from <var>xmlStreamProvider</var>.
   *
   * @param xmlOutputStreamProvider Provides streams to receive the XML content
   * for the data in the data set.
   * @param tableColumnsToMask the table columns to mask when consuming a dataset
   */
  public DataMaskingXmlDataSetConsumer(XmlOutputStreamProvider xmlOutputStreamProvider, Map<String, Set<String>> tableColumnsToMask) {
    super(xmlOutputStreamProvider);
    this.tableColumnsToMask = tableColumnsToMask;
  }



  /**
   * If the provided column should be masked then return null, otherwise return the value.
   */
  @Override
  protected String getValue(Record record, Column column, String table) {
    if (tableColumnsToMask.containsKey(table) && tableColumnsToMask.get(table).contains(column.getName())) {
      return null;
    }
    return super.getValue(record, column, table);
  }
}
