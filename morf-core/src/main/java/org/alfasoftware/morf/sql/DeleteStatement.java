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

import java.util.function.Function;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;

/**
 * Class which encapsulates the generation of a DELETE SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   SqlUtils.delete()
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
public class DeleteStatement implements Statement,
                                        DeepCopyableWithTransformation<DeleteStatement, DeleteStatementBuilder>,
                                        ShallowCopyable<DeleteStatement, DeleteStatementBuilder>,
                                        Driver {

  /**
   * The table to update
   */
  private final TableReference table;


  /**
   * The selection criteria for selecting from the database.
   *
   * TODO make final
   */
  private Criterion whereCriterion;


  /**
   * Builder constructor.
   *
   * @param deleteStatementBuilder The builder.
   */
  DeleteStatement(DeleteStatementBuilder deleteStatementBuilder) {
    this.table = deleteStatementBuilder.getTable();
    this.whereCriterion = deleteStatementBuilder.getWhereCriterion();
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
   * <blockquote><pre>delete([table])
   *    .where([criteria]);</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return a statement with the change applied.
   */
  public DeleteStatement where(Criterion criterion) {
    return copyOnWriteOrMutate(
        b -> b.where(criterion),
        () -> {
          if (criterion == null)
            throw new IllegalArgumentException("Criterion was null in where clause");
          whereCriterion = criterion;
        }
    );
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
    return deepCopy(DeepCopyTransformations.noTransformation()).build();
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
  public DeleteStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new DeleteStatementBuilder(this, transformer);
  }


  /**
   * Either shallow copies and mutates the result, returning it,
   * or mutates the statement directly, depending on
   * {@link AliasedField#immutableDslEnabled()}.
   *
   * TODO for removal along with mutable behaviour.
   *
   * @param transform A transform which modifies the shallow copy builder.
   * @param mutator Code which applies the local changes instead.
   * @return The result (which may be {@code this}).
   */
  private DeleteStatement copyOnWriteOrMutate(Function<DeleteStatementBuilder, DeleteStatementBuilder> transform, Runnable mutator) {
    if (AliasedField.immutableDslEnabled()) {
      return transform.apply(shallowCopy()).build();
    } else {
      mutator.run();
      return this;
    }
  }


  /**
   * Performs a shallow copy to a builder, allowing a duplicate
   * to be created and modified.
   *
   * @return A builder, initialised as a duplicate of this statement.
   */
  @Override
  public DeleteStatementBuilder shallowCopy() {
    return new DeleteStatementBuilder(this);
  }


  @Override
  public String toString() {
    return "SQL DELETE FROM [" + table + "] " +
    (whereCriterion == null ? "" : ("WHERE " + whereCriterion));
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    result = prime * result + ((whereCriterion == null) ? 0 : whereCriterion.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DeleteStatement other = (DeleteStatement) obj;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (whereCriterion == null) {
      if (other.whereCriterion != null)
        return false;
    } else if (!whereCriterion.equals(other.whereCriterion))
      return false;
    return true;
  }
}