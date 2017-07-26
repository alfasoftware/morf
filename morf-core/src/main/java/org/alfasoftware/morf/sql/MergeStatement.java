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

import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * <p>Class which encapsulates the generation of an MERGE SQL statement.</p>
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *   new MergeStatement()
 *        |----&gt; .into([table])
 *                |----&gt; .tableUniqueKey([field]...)
 *                |----&gt; .fromSelect([SelectStatement])
 * </pre></blockquote>
 *
 * <p>A Merge statement takes a target table and merges (INSERTS or UPDATES) data from a source table.
 * If a record exists with the same unique key in the target table, the record is updated, otherwise the record
 * is inserted</p>
 *
 * <p>In order to ensure compatibility across database platforms, in particular MySQL,
 * the fields used as the keys within the Merge must be the table's primary keys or the keys in a unique index.
 * Although other databases can support any fields being the matching keys, MySQL does not.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class MergeStatement implements Statement,DeepCopyableWithTransformation<MergeStatement,Builder<MergeStatement>>,Driver {

  /**
   * The key fields upon which to check if record exists.
   */
  private final List<AliasedField>  tableUniqueKey = new ArrayList<>();

  /**
   * The primary table to merge into.
   */
  private TableReference            table;

  /**
   * The select statement to source the data from.
   */
  private SelectStatement           selectStatement;


  /**
   * Constructs an Merge Statement.
   */
  public MergeStatement() {
    super();
  }


  /**
   * Constructor to create a deep copy.
   *
   * @param sourceStatement {@link MergeStatement} to create a deep copy from.
   */
  private MergeStatement(MergeStatement sourceStatement,DeepCopyTransformation transformer) {
    super();

    this.table = transformer.deepCopy(sourceStatement.table);

    for (AliasedField currentField : sourceStatement.tableUniqueKey) {
      this.tableUniqueKey.add(transformer.deepCopy(currentField.deepCopy()));
    }

    this.selectStatement = transformer.deepCopy(sourceStatement.selectStatement);
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public MergeStatement deepCopy() {
    return new MergeStatement(this,noTransformation());
  }


  /**
   * Merges into a specific table.
   *
   * <blockquote><pre>
   *    new MergeStatement().into(new TableReference("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to merge into.
   * @return the updated MergeStatement (this will not be a new object).
   */
  public MergeStatement into(TableReference intoTable) {
    this.table = intoTable;
    return this;
  }


  /**
   * <p>
   * Specifies the fields which make up a unique index or primary key on the
   * target table.
   * </p>
   * <p>
   * These <em>must</em> fully match a unique index or primary key, otherwise
   * this statement will fail on MySQL.  Note also potential issues around having
   * two unique indexes or a primary key and unique index, as detailed
   * <a href="http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html">here</a>.
   * </p>
   *
   * @param keyFields the key fields.
   * @return the updated MergeStatement (this will not be a new object).
   */
  public MergeStatement tableUniqueKey(AliasedField... keyFields) {
    return tableUniqueKey(Arrays.asList(keyFields));
  }


  /**
   * <p>
   * Specifies the fields which make up a unique index or primary key on the
   * target table.
   * </p>
   * <p>
   * These <em>must</em> fully match a unique index or primary key, otherwise
   * this statement will fail on MySQL.  Note also potential issues around having
   * two unique indexes or a primary key and unique index, as detailed
   * <a href="http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html">here</a>.
   * </p>
   *
   * @param keyFields the key fields.
   * @return the updated MergeStatement (this will not be a new object).
   */
  public MergeStatement tableUniqueKey(List<AliasedField> keyFields) {
    this.tableUniqueKey.addAll(keyFields);
    return this;
  }


  /**
   * Specifies the select statement to use as a source of the data.
   *
   * @param statement the source statement.
   * @return the updated MergeStatement (this will not be a new object).
   */
  public MergeStatement from(SelectStatement statement) {
    if (statement.getOrderBys().size() != 0) {
      throw new IllegalArgumentException("ORDER BY is not permitted in the SELECT part of a merge statement (SQL Server limitation)");
    }
    this.selectStatement = statement;
    return this;
  }


  /**
   * Gets the table to merge the data into.
   *
   * @return the table.
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * Gets the select statement that selects the values to merge
   * into the table.
   *
   * @return the select statement.
   */
  public SelectStatement getSelectStatement() {
    return selectStatement;
  }


  /**
   * Gets a list of the fields used as the key upon which to match.
   *
   * @return the table unique key.
   */
  public List<AliasedField> getTableUniqueKey() {
    return tableUniqueKey;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SQL MERGE INTO [" + table + "] USING [" + selectStatement + "] KEY " + tableUniqueKey;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTableUniqueKey())
      .dispatch(getTable())
      .dispatch(getSelectStatement());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */

  @Override
  public Builder<MergeStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new MergeStatement(this, transformer));
  }
}