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

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;


/**
 * A unit of metadata change which can be applied to a snapshot of database metadata
 * in a forwards or backwards direction returning a new snapshot of database metadata
 * with the change applied or un-applied.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface SchemaChange {

  /**
   * Apply the {@link SchemaChange} in a forward direction (i.e. apply
   * the change to the specified <var>metadata</var>.
   *
   * @param schema {@link Schema} to apply the change against resulting in new metadata.
   *
   * @return MetaData with {@link SchemaChange} applied.
   */
  public Schema apply(Schema schema);


  /**
   * Apply the {@link SchemaChange} in a backward direction (i.e. un-apply
   * the change against the specified <var>metadata</var>.
   *
   * @param schema {@link Schema} to un-apply the change against resulting in new metadata.
   *
   * @return MetaData with {@link SchemaChange} un-applied.
   */
  public Schema reverse(Schema schema);



  /**
   * Determine if the {@link SchemaChange} is already applied to the specified <var>schema</var>.
   *
   * @param schema Schema to test against.
   * @param database The database to check for applied steps in.
   * @return True if the change is already present in the schema.
   */
  public boolean isApplied(Schema schema, ConnectionResources database);


  /**
   * Visits the schema change.
   *
   * <p>Any upgrade strategy must be able to handle all the types of schema changes.
   * This {@link SchemaChangeVisitor} interface identifies those elementary changes and
   * allows different upgrade approach implementations to operate over the same set
   * of schema changes.
   *
   * @param visitor Visitor to provide specific upgrade implementations.
   */
  public void accept(SchemaChangeVisitor visitor);
}
