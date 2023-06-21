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

/**
 * Set of constant fields required for XML serialisation / de-serialisation
 * of data sets.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class XmlDataSetNode {

  /**
   * Name space URI used by XML data sets.
   */
  public static final String URI = "";

  /**
   * Node name that contains meta data.
   */
  public static final String METADATA_NODE = "metadata";

  /**
   * Node name for the table meta data node.
   */
  public static final String TABLE_NODE = "table";

  /**
   * Node name that defines a column.
   */
  public static final String COLUMN_NODE = "column";

  /**
   * Attribute name for the name property.
   */
  public static final String NAME_ATTRIBUTE = "name";

  /**
   * Attribute name for the version property.
   */
  public static final String VERSION_ATTRIBUTE    = "version";

  /**
   * Attribute name for the data type property.
   */
  public static final String TYPE_ATTRIBUTE = "type";

  /**
   * Attribute name for the width property.
   */
  public static final String WIDTH_ATTRIBUTE = "width";

  /**
   * Attribute name for the scale property.
   */
  public static final String SCALE_ATTRIBUTE = "scale";

  /**
   * Attribute name for the nullable property.
   */
  public static final String NULLABLE_ATTRIBUTE = "nullable";

  /**
   * Attribute name for the default property.
   */
  public static final String DEFAULT_ATTRIBUTE    = "default";

  /**
   * Attribute name for the primary key property.
   */
  public static final String PRIMARYKEY_ATTRIBUTE = "primaryKey";

  /**
   * Attribute name for the unique property of an index.
   */
  public static final String UNIQUE_ATTRIBUTE = "unique";

  /**
   * Attribute name for the columns property of an index.
   */
  public static final String COLUMNS_ATTRIBUTE = "columns";

  /**
   * Node name for the data node.
   */
  public static final String DATA_NODE = "data";

  /**
   * Node name used for a record.
   */
  public static final String RECORD_NODE = "record";

  /**
   * Data type name for string attributes.
   */
  public static final String STRING_TYPE = "string";

  /**
   * Node name for indexes
   */
  public static final String INDEX_NODE = "index";

  /**
   * Node name for autonumbering
   */
  public static final String AUTONUMBER_ATTRIBUTE = "autoNum";

}
