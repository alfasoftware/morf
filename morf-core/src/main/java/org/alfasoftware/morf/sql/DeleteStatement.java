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

import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Class which encapsulates the generation of a DELETE SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   new DeleteStatement()
 *        |----&gt; .from([table])                               = DELETE FROM [table]
 *                |----&gt; .where([criterion])                  = DELETE FROM [table] WHERE [criterion]
 *  </pre></blockquote>
 *
 * <p>This class does not accept string references to table names. Instead, you must provide
 * the methods with a {@link TableReference} reference.</p>
 *
 * <p>Each method of this class will return an instance of the {@link DeleteStatement} class. However, this will always
 * be the same instance rather than a new instance of the class.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class DeleteStatement  implements Statement, DeepCopyableWithTransformation<DeleteStatement, Builder<DeleteStatement>>, Driver {

  /**
   * The table to update
   */
  private final TableReference table;


  /**
   * The selection criteria for selecting from the database
   */
  private Criterion whereCriterion;


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link DeleteStatement} to create a deep copy from.
   * @param transformer A transformation that can intercept a deep copy operation.
   */
  private DeleteStatement(DeleteStatement sourceStatement, DeepCopyTransformation transformer) {
    this.table = transformer.deepCopy(sourceStatement.table);
    this.whereCriterion = transformer.deepCopy(sourceStatement.whereCriterion);
  }

  /**
   * Constructs a Delete Statement.
   *
   * @param table the database table to delete from.
   */
  public DeleteStatement(TableReference table) {
    super();

    this.table = table;
  }


  /**
   * Gets the table being deleted from.
   *
   * @return the table being inserted into
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Specifies the where criteria
   *
   * <blockquote><pre>
   *    new DeleteStatement([table])
   *                         .where([criteria]);</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return the updated DeleteStatement (this will not be a new object)
   */
  public DeleteStatement where(Criterion criterion) {
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
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public DeleteStatement deepCopy() {
    return new DeleteStatement(this, DeepCopyTransformations.noTransformation());
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTable())
      .dispatch(getWhereCriterion());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<DeleteStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new DeleteStatement(this,transformer));
  }
}
