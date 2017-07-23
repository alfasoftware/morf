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

package org.alfasoftware.morf.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Class which encapsulates the generation of an UPDATE SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   new UpdateStatement([table])                        = UPDATE TABLE
 *             |----&gt; .set([field], ...)              = UPDATE TABLE SET ([field], ...)
 *             |----&gt; .where([criterion])             = UPDATE TABLE SET ([field], ...) WHERE [criterion] </pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */

public class UpdateStatement implements Statement ,  DeepCopyableWithTransformation<UpdateStatement,Builder<UpdateStatement>>,Driver {

  /**
   * The table to update
   */
  private final TableReference table;

  /**
   * The fields to select from the table
   */
  private final List<AliasedField> fields   = new ArrayList<>();

  /**
   * The selection criteria for selecting from the database
   */
  private Criterion whereCriterion;


  @Override
  public String toString() {
    return "SQL UPDATE [" + table + "] SET " + fields;
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link UpdateStatement} to create a deep copy from.
   */
  private UpdateStatement(UpdateStatement sourceStatement, DeepCopyTransformation transformation) {
    super();
    this.fields.addAll(DeepCopyTransformations.transformIterable(sourceStatement.fields, transformation));
    this.table = transformation.deepCopy(sourceStatement.table);
    this.whereCriterion = transformation.deepCopy(sourceStatement.whereCriterion);
  }

  /**
   * Constructs an Update Statement.
   *
   * @param table the database table to update
   */
  public UpdateStatement(TableReference table) {
    super();

    this.table = table;
  }


  /**
   * Gets the table being inserted into
   *
   * @return the table being inserted into
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Specifies the fields to set
   *
   * @param destinationFields the fields to update in the database table
   * @return the updated UpdateStatement (this will not be a new object)
   */
  public UpdateStatement set(AliasedField... destinationFields) {
    this.fields.addAll(Arrays.asList(destinationFields));
    return this;
  }


  /**
   * Specifies the where criteria
   *
   * <blockquote><pre>
   *    new UpdateStatement([table]).set([fields])
   *                         .where([criteria]);</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return the updated SelectStatement (this will not be a new object)
   */
  public UpdateStatement where(Criterion criterion) {
    if (criterion == null)
      throw new IllegalArgumentException("Criterion was null in where clause");

    whereCriterion = criterion;

    return this;
  }


  /**
   * Gets the where criteria.
   *
   * @return the where criteria
   */
  public Criterion getWhereCriterion() {
    return whereCriterion;
  }


  /**
   * Gets the list of fields
   *
   * @return the fields
   */
  public List<AliasedField> getFields() {
    return fields;
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  @Deprecated
  public UpdateStatement deepCopy() {
    return deepCopy(DeepCopyTransformations.noTransformation()).build();
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<UpdateStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new UpdateStatement(this,transformer));
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTable())
      .dispatch(getWhereCriterion())
      .dispatch(getFields());
  }
}
