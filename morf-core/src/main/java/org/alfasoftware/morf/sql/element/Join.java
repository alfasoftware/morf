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

package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.sql.AbstractSelectStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * A class which represents an SQL join to a table. It only represents the
 * destination table and not the source table.
 *
 * <p>Joins are created using join methods such as
 * {@link AbstractSelectStatement#innerJoin(SelectStatement)}. Example:</p>
 *
 * <blockquote><pre>
 *    Statement stmt = SqlUtils.select
 *                          .from(tableRef("agreement"))
 *                          .innerJoin(tableRef("schedule"),
 *                                     tableRef("agreement").field("agreementnumber")
 *                                       .eq(tableRef("schedule").field("agreementnumber"))
 *                                    );</pre></blockquote>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class Join implements Driver, DeepCopyableWithTransformation<Join,Builder<Join>> {

  /**
   * The type of join to use
   */
  private final JoinType type;

  /**
   * The table to join to
   */
  private final TableReference table;

  private final SelectStatement subSelect;

  /**
   * The criteria to match records in the joined table with.
   * TODO should be final
   */
  private Criterion criterion;


  /**
   * Constructor used to create deep copy of a join
   *
   * @param sourceJoin the source {@link Join} to create the deep copy from
   */
  private Join(Join sourceJoin, DeepCopyTransformation transformer) {
    super();
    this.criterion =  sourceJoin.criterion == null ? null : transformer.deepCopy(sourceJoin.criterion);
    this.table = sourceJoin.table == null ? null : transformer.deepCopy(sourceJoin.table);
    this.subSelect = sourceJoin.subSelect == null ? null : transformer.deepCopy(sourceJoin.subSelect);
    this.type = sourceJoin.type;
  }


  /**
   * Construct a new Join object to a specified table on the specified criteria.
   *
   * @param type the type of join
   * @param table the table to join to
   * @deprecated Use {@link #Join(JoinType, TableReference, Criterion)}.  This constructor
   *     relies on setting the {@link Criterion} after construction, which it won't be able
   *     to do once {@link Join} is immutable.
   */
  @Deprecated
  public Join(JoinType type, TableReference table) {
    super();
    this.type = type;
    this.table = table;
    this.subSelect = null;
  }


  /**
   * Construct a new Join object to a specified table on the specified criteria.
   *
   * @param type The type of join.
   * @param table The table to join to.
   * @param criterion The join criterion.
   */
  public Join(JoinType type, TableReference table, Criterion criterion) {
    super();
    this.type = type;
    this.table = table;
    this.criterion = criterion;
    this.subSelect = null;
  }


  /**
   * Construct a new Join onto a sub select statment. This statement must have an alias
   *
   * @param type the type of join
   * @param subSelect the select to join onto
   * @deprecated Use {@link #Join(JoinType, SelectStatement, Criterion)}.  This constructor
   *     relies on setting the {@link Criterion} after construction, which it won't be able
   *     to do once {@link Join} is immutable.
   */
  @Deprecated
  public Join(JoinType type, SelectStatement subSelect) {
    this.type = type;
    this.subSelect = subSelect;
    this.table = null;
  }


  /**
   * Construct a new Join onto a sub select statment. This statement must have an alias
   *
   * @param type The type of join
   * @param subSelect The select to join onto
   * @param criterion The join criterion.
   */
  public Join(JoinType type, SelectStatement subSelect, Criterion criterion) {
    this.type = type;
    this.subSelect = subSelect;
    this.criterion = criterion;
    this.table = null;
  }


  /**
   * Get the type of join.
   *
   * @return the type
   */
  public JoinType getType() {
    return type;
  }

  /**
   * Get the table to join to.
   *
   * @return the table
   */
  public TableReference getTable() {
    return table;
  }

  /**
   * Get the criteria used in the join.
   *
   * @return the criteria
   */
  public Criterion getCriterion() {
    return criterion;
  }


  /**
   * @return the subSelect
   */
  public SelectStatement getSubSelect() {
    return subSelect;
  }


  /**
   * Add ON criteria for the join.
   *
   * @param onCondition the criteria
   * @return this
   * @deprecated Do not use. Will be removed shortly - {@link Join} should be immutable.
   */
  @Deprecated
  public Join on(Criterion onCondition) {
    AliasedField.assetImmutableDslDisabled();
    this.criterion = onCondition;
    return this;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTable())
      .dispatch(getSubSelect())
      .dispatch(getCriterion());
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (criterion == null ? 0 : criterion.hashCode());
    result = prime * result + (subSelect == null ? 0 : subSelect.hashCode());
    result = prime * result + (table == null ? 0 : table.hashCode());
    result = prime * result + (type == null ? 0 : type.hashCode());
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
    Join other = (Join) obj;
    if (criterion == null) {
      if (other.criterion != null)
        return false;
    } else if (!criterion.equals(other.criterion))
      return false;
    if (subSelect == null) {
      if (other.subSelect != null)
        return false;
    } else if (!subSelect.equals(other.subSelect))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (type != other.type)
      return false;
    return true;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if (table != null) {
      return type.toString() + " " + table.toString() + " ON " + criterion;
    } else {
      return type.toString() + " (" + subSelect.toString() + ") ON " + criterion;
    }
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<Join> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new Join(this,transformer));
  }
}