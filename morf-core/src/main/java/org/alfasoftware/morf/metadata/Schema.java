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

import java.util.Collection;

import org.alfasoftware.morf.jdbc.AdditionalMetadata;


/**
 * Provides database schema meta data.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface Schema extends AdditionalMetadata {

  /**
   * Determines if a database exists in the meta data source.
   *
   * @return True if there are no database tables available.
   */
  public boolean isEmptyDatabase();

  /**
   * Determines if a table exists on the database.
   *
   * @param name The table name to be checked. The case of the name should be ignored.
   * @return True if a table <var>name</var> exists in the database. False otherwise.
   */
  public boolean tableExists(String name);

  /**
   * Retrieves the meta data for the table <var>name</var>.
   *
   * <p>Implementations should never return null. A {@link RuntimeException} may
   * be thrown if the table is unknown.</p>
   *
   * @param name The table name for which meta data is required. The case of the name should be ignored.
   * @return The table meta data.
   */
  public Table getTable(String name);

  /**
   * Provides the names of all tables in the database. Note that the order of
   * the tables in the result is not specified. The case of the
   * table names may be preserved when logging progress, but should not be relied on for schema
   * processing.
   *
   * @return A collection of all table names available in the database.
   */
  public Collection<String> tableNames();

  /**
   * @return the tables in in the schema represented by this metadata
   */
  public Collection<Table> tables();

  /**
   * Determines if a view exists on the database.
   *
   * @param name The view name to be checked. The case of the name should be ignored.
   * @return True if a view <var>name</var> exists in the database. False otherwise.
   */
  public boolean viewExists(String name);

  /**
   * Retrieves the meta data for the view <var>name</var>.
   *
   * <p>Implementations should never return null. A {@link RuntimeException} may
   * be thrown if the view is unknown.</p>
   *
   * @param name The view name for which meta data is required. The case of the name should be ignored.
   * @return The view meta data.
   */
  public View getView(String name);

  /**
   * Provides the names of all views in the database. Note that the order of
   * the views in the result is not specified. The case of the
   * view names may be preserved when logging progress, but should not be relied on for schema
   * processing.
   *
   * @return A collection of all view names available in the database.
   */
  public Collection<String> viewNames();

  /**
   * @return the views in the schema represented by this metadata
   */
  public Collection<View> views();


  /**
   * Determines if a sequence exists on the database.
   *
   * @param name The sequence name to be checked. The case of the name should be ignored.
   * @return True if a sequence <var>name</var> exists in the database. False otherwise.
   */
  public boolean sequenceExists(String name);


  /**
   * Retrieves the meta data for the sequence <var>name</var>.
   *
   * <p>Implementations should never return null. A {@link RuntimeException} may
   * be thrown if the sequence is unknown.</p>
   *
   * @param name The sequence name for which meta data is required. The case of the name should be ignored.
   * @return The sequence meta data.
   */
  public Sequence getSequence(String name);


  /**
   * Provides the names of all sequences in the database. Note that the order of
   * the sequences in the result is not specified. The case of the
   * sequence names may be preserved when logging progress, but should not be relied on for schema
   * processing.
   *
   * @return A collection of all view names available in the database.
   */
  public Collection<String> sequenceNames();


  /**
   * @return the sequences in the schema represented by this metadata
   */
  public Collection<Sequence> sequences();
}
