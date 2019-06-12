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

import java.util.Objects;
import java.util.Optional;
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
 * <p>The class structure imitates the end SQL and is constructed using a builder as follows:</p>
 *
 * <blockquote><pre>
 *  DeleteStatement.delete()
 *    .from([table])             = DELETE FROM [table]
 *    .where([criterion])        = DELETE FROM [table] WHERE [criterion]
 *    .build()</pre></blockquote>
 *
 * <p>It is also possible to create instances directly using the constructors or the factory
 * methods on {@link SqlUtils}.  Both are discouraged and will be deprecated in the future.</p>
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
   * The limit.
   */
  private final Optional<Integer> limit;


  /**
   * The selection criteria for selecting from the database.
   *
   * TODO make final
   */
  private Criterion whereCriterion;


  /**
   * Constructs a Delete Statement. See class-level documentation for usage instructions.
   *
   * @param table the database table to delete from.
   * @return A builder.
   */
  public static DeleteStatementBuilder delete(TableReference table) {
    return new DeleteStatementBuilder(table);
  }


  /**
   * Builder constructor.
   *
   * @param deleteStatementBuilder The builder.
   */
  DeleteStatement(DeleteStatementBuilder deleteStatementBuilder) {
    this.table = deleteStatementBuilder.getTable();
    this.whereCriterion = deleteStatementBuilder.getWhereCriterion();
    this.limit = deleteStatementBuilder.getLimit();
  }


  /**
   * Constructs a Delete Statement.
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #delete(TableReference)} for preference.</p>
   *
   * @param table the database table to delete from.
   */
  public DeleteStatement(TableReference table) {
    super();
    this.table = table;
    this.limit = Optional.empty();
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
   * Gets the limit.
   *
   * @return the limit on the number of deleted records.
   */
  public Optional<Integer> getLimit() {
    return limit;
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
      .dispatch(getWhereCriterion())
      .dispatch(getLimit());
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
      (whereCriterion == null ? "" : ("WHERE " + whereCriterion)) +
      (limit.isPresent() ? " LIMIT(" + limit.get() + ")" : "");
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    result = prime * result + ((whereCriterion == null) ? 0 : whereCriterion.hashCode());
    result = prime * result + ((!limit.isPresent()) ? 0 : limit.get().hashCode());
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
    if (!Objects.equals(limit, other.limit))
      return false;
    return true;
  }
}