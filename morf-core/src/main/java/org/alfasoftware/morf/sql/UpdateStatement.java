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

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Class which encapsulates the generation of an UPDATE SQL statement.
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 * UpdateStatement.update([table])   = UPDATE TABLE
 *   .set([field], ...)              = UPDATE TABLE SET ([field], ...)
 *   .where([criterion])             = UPDATE TABLE SET ([field], ...) WHERE [criterion]
 *   .build()</pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */

public class UpdateStatement implements Statement,
                            DeepCopyableWithTransformation<UpdateStatement, UpdateStatementBuilder>,
                            ShallowCopyable<UpdateStatement, UpdateStatementBuilder>,
                            Driver {

  /**
   * The table to update
   */
  private final TableReference table;

  /**
   * The fields to select from the table
   */
  private final List<AliasedField> fields;

  /**
   * Lists the declared hints in the order they were declared.
   */
  private final List<Hint> hints;

  /**
   * The selection criteria for selecting from the database
   *
   * TODO make final
   */
  private Criterion whereCriterion;


  /**
   * Constructs an update statement.
   *
   * @param tableReference the database table to update
   * @return Builder.
   */
  public static UpdateStatementBuilder update(TableReference tableReference) {
    return new UpdateStatementBuilder(tableReference);
  }


  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("SQL UPDATE [");
    if (!hints.isEmpty()) {
      builder.append(hints);
    }
    builder.append(table).append("] SET ").append(fields);
    if (whereCriterion != null) {
      builder.append(" WHERE [" + whereCriterion + "]");
    }
    return builder.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (fields == null ? 0 : fields.hashCode());
    result = prime * result + (hints == null ? 0 : hints.hashCode());
    result = prime * result + (table == null ? 0 : table.hashCode());
    result = prime * result + (whereCriterion == null ? 0 : whereCriterion.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    UpdateStatement other = (UpdateStatement) obj;
    if (fields == null) {
      if (other.fields != null) return false;
    } else if (!fields.equals(other.fields)) return false;
    if (hints == null) {
      if (other.hints != null) return false;
    } else if (!hints.equals(other.hints)) return false;
    if (table == null) {
      if (other.table != null) return false;
    } else if (!table.equals(other.table)) return false;
    if (whereCriterion == null) {
      if (other.whereCriterion != null) return false;
    } else if (!whereCriterion.equals(other.whereCriterion)) return false;
    return true;
  }


  /**
   * Constructor for the builder.
   *
   * @param sourceStatement {@link UpdateStatement} to create a deep copy from.
   */
  UpdateStatement(UpdateStatementBuilder builder) {
    super();
    this.fields = AliasedField.immutableDslEnabled()
        ? ImmutableList.copyOf(builder.getFields())
        : Lists.newArrayList(builder.getFields());
    this.hints = AliasedField.immutableDslEnabled()
        ? ImmutableList.copyOf(builder.getHints())
        : Lists.newArrayList(builder.getHints());
    this.table = builder.getTable();
    this.whereCriterion = builder.getWhereCriterion();
  }


  /**
   * Constructs an Update Statement.
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #update(TableReference)} for preference.</p>
   *
   * @param table the database table to update
   */
  public UpdateStatement(TableReference table) {
    super();
    this.fields = AliasedField.immutableDslEnabled()
        ? ImmutableList.of()
        : Lists.newArrayList();
    this.hints = AliasedField.immutableDslEnabled()
            ? ImmutableList.of()
            : Lists.newArrayList();
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
   * Specifies the fields to set.
   *
   * @param destinationFields the fields to update in the database table
   * @return a statement with the changes applied.
   */
  public UpdateStatement set(AliasedField... destinationFields) {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().set(destinationFields).build();
    } else {
      this.fields.addAll(Arrays.asList(destinationFields));
      return this;
    }
  }


  /**
   * Specifies the where criteria.
   *
   * <blockquote><pre>
   * update([table])
   *   .set([fields])
   *   .where([criteria]);</pre></blockquote>
   *
   * @param criterion the criteria to filter the results by
   * @return a statement with the changes applied.
   */
  public UpdateStatement where(Criterion criterion) {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().where(criterion).build();
    } else {
      if (criterion == null)
        throw new IllegalArgumentException("Criterion was null in where clause");
      whereCriterion = criterion;
      return this;
    }
  }


  /**
   * Request that this statement is executed with a parallel execution plan for data manipulation language (DML). This request will have no effect unless the database implementation supports it and the feature is enabled.
   *
   * <p>For statement that will affect a high percentage or rows in the table, a parallel execution plan may reduce the execution time, although the exact effect depends on
   * the underlying database, the nature of the data and the nature of the query.</p>
   *
   * <p>Note that the use of parallel DML comes with restrictions, in particular, a table may not be accessed in the same transaction following a parallel DML execution. Please consult the Oracle manual section <em>Restrictions on Parallel DML</em> to check whether this hint is suitable.</p>
   *    
   * @return this, for method chaining.
   */
  public UpdateStatement useParallelDml() {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().useParallelDml().build();
    } else {
      hints.add(new UseParallelDml());
      return this;
    }
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
   * @return all hints in the order they were declared.
   */
  public List<Hint> getHints() {
    return hints;
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   * @deprecated use deepCopy({@link DeepCopyTransformation}) which returns a builder.
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
  public UpdateStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new UpdateStatementBuilder(this, transformer);
  }


  /**
   * Performs a shallow copy to a builder, allowing a duplicate
   * to be created and modified.
   *
   * @return A builder, initialised as a duplicate of this statement.
   */
  @Override
  public UpdateStatementBuilder shallowCopy() {
    return new UpdateStatementBuilder(this);
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTable())
      .dispatch(getWhereCriterion())
      .dispatch(getFields())
      .dispatch(getHints());
  }
}