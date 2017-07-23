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

import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;

/**
 * Class which encapsulates the generation of a UNION set operator. It combines
 * the result from multiple {@link SelectStatement}s into a single result set.
 * <p>
 * This class cannot be instantiated directly. Instead, use the
 * {@linkplain SelectStatement#union(SelectStatement)} or
 * {@linkplain SelectStatement#unionAll(SelectStatement)} which encapsulate this
 * class existence.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class UnionSetOperator implements SetOperator{

  /**
   * Identifies the duplicate row elimination strategy for UNION statements.
   *
   * @author Copyright (c) Alfa Financial Software 2012
   */
  public enum UnionStrategy {

    /**
     * Returns all rows into the result set, even if the row exists in more than
     * one of the {@link SelectStatement}.
     */
    ALL,

    /**
     * Removes the duplicate rows from the result set.
     */
    DISTINCT;
  }

  private final SelectStatement selectStatement;

  private final UnionStrategy   unionStrategy;



  /**
   * Creates a new UNION set operation between the {@code parentSelect} and
   * {@code childSelect}, either removing or including duplicate records.
   * <p>
   * A reference to the {@code parentSelect} is not maintained as the only way
   * to construct this class is via the parent select itself. Its reference is
   * necessary only for validation purposes.
   * </p>
   */
  UnionSetOperator(UnionStrategy unionStrategy, SelectStatement parentSelect, SelectStatement childSelect) {
    validateNotNull(parentSelect, childSelect);
    validateFields(parentSelect, childSelect);
    validateOrderBy(childSelect);

    this.selectStatement = childSelect;
    this.unionStrategy = unionStrategy;
  }


  /**
   * Constructor used to create a deep copy of a union statement.
   *
   * @param unionStrategy the union strategy to use
   * @param childSelect the second part of the UNION statement
   */
  private UnionSetOperator(UnionStrategy unionStrategy, SelectStatement childSelect) {
    this.selectStatement = childSelect;
    this.unionStrategy = unionStrategy;
  }


  /**
   * Don't allow {@code null} references to {@linkplain SelectStatement}.
   *
   * @param parentSelect the select statement to be validated.
   * @param childSelect the select statement to be validated.
   */
  private void validateNotNull(SelectStatement parentSelect, SelectStatement childSelect) throws IllegalArgumentException {
    if (parentSelect == null || childSelect == null) {
      throw new IllegalArgumentException("Select statements cannot be null");
    }
  }


  /**
   * Don't allow {@code childSelect} have a different number of fields from
   * {@code parentSelect}.
   * <p>
   * The column names from the parent select statement are used as the column
   * names for the results returned. Selected columns listed in corresponding
   * positions of each SELECT statement should have the same data type.
   * </p>
   *
   * @param parentSelect the select statement to be compared against.
   * @param childSelect the select statement to be validated.
   */
  private void validateFields(SelectStatement parentSelect, SelectStatement childSelect) throws IllegalArgumentException {
    if (parentSelect.getFields().size() != childSelect.getFields().size()) {
      throw new IllegalArgumentException("Union statement requires selecting the same number of fields on both select statements");
    }
  }


  /**
   * Don't allow sub-select statements to have ORDER BY statements, as this is
   * an invalid construct in in SQL-92.
   *
   * @param selectStatement the select statement to be validated.
   */
  private void validateOrderBy(SelectStatement selectStatement) throws IllegalArgumentException {
    if (!selectStatement.getOrderBys().isEmpty()) {
      throw new IllegalArgumentException("Only the parent select statement can contain an order by statement");
    }
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.sql.SetOperator#getSelectStatement()
   */
  @Override
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * @return The duplicate row elimination strategy.
   * @see UnionStrategy
   */
  public UnionStrategy getUnionStrategy() {
    return unionStrategy;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "UNION " + unionStrategy + " " + selectStatement;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getSelectStatement());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<SetOperator> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.<SetOperator>wrapper(new UnionSetOperator(getUnionStrategy(),transformer.deepCopy(getSelectStatement())));
  }
}