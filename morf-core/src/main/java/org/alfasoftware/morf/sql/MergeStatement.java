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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * <p>Class which encapsulates the generation of an MERGE SQL statement.</p>
 *
 * <p>The class structure imitates the end SQL and is structured as follows:</p>
 *
 * <blockquote><pre>
 *  MergeStatement.merge()
 *    .into([table])
 *    .tableUniqueKey([field]...)
 *    .ifUpdating([updateExpression]...)
 *    .whenMatched([MergeMatchClause])
 *    .fromSelect([SelectStatement])
 *    .build()</pre></blockquote>
 *
 * <p>A Merge statement takes a target table and merges (INSERTS or UPDATES) data from a source table.
 * If a record exists with the same unique key in the target table, the record is updated, otherwise the record
 * is inserted.</p>
 *
 * <p>By default, when records match, all non-key fields are updated with values from the source.
 * This behavior can be customized using {@link MergeStatementBuilder#whenMatched(MergeMatchClause)} to specify
 * conditions for when updates should occur (e.g., only update when specific fields differ).</p>
 *
 * <p>In order to ensure compatibility across database platforms, in particular MySQL,
 * the fields used as the keys within the Merge must be the table's primary keys or the keys in a unique index.
 * Although other databases can support any fields being the matching keys, MySQL does not.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class MergeStatement implements Statement,
                         DeepCopyableWithTransformation<MergeStatement, MergeStatementBuilder>,
                         ShallowCopyable<MergeStatement, MergeStatementBuilder>,
                         Driver {

  /**
   * The key fields upon which to check if record exists.
   */
  private final List<AliasedField>  tableUniqueKey;

  /**
   * The primary table to merge into.
   *
   * TODO make final
   */
  private TableReference            table;

  /**
   * The select statement to source the data from.
   *
   * TODO make final
   */
  private SelectStatement           selectStatement;

  /**
   * Expressions to be used for updating existing records.
   */
  private final List<AliasedField>  ifUpdating;

  /**
   * Optional action specification for when records match.
   */
  private final Optional<MergeMatchClause> whenMatchedAction;

  /**
   * Constructs a Merge Statement which either inserts or updates
   * a record into a table depending on whether a condition exists in
   * the table.
   *
   * @return Statement builder.
   */
  public static MergeStatementBuilder merge() {
    return new MergeStatementBuilder();
  }


  /**
   * For updating existing records, references the new field value being merged, i.e. the value provided by the select.
   * This internally implements the {@link MergeStatementBuilder.UpdateValues#input(String)} reference.
   */
  public static class InputField extends AliasedField {

    private final String name;

    public static InputField inputField(String name) {
      return new InputField(name);
    }

    public InputField(String name) {
      super("");
      this.name = name;
    }


    public String getName() {
      return name;
    }


    @Override
    public String getImpliedName() {
      return StringUtils.isBlank(super.getImpliedName()) ? getName() : super.getImpliedName();
    }


    @Override
    protected AliasedFieldBuilder deepCopyInternal(DeepCopyTransformation transformer) {
      return new InputField(name);
    }


    @Override
    public String toString() {
      return "newValue(" + name + ")" + super.toString();
    }


    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .appendSuper(super.hashCode())
          .append(name)
          .toHashCode();
    }


    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null) return false;
      if (obj.getClass() != this.getClass()) return false;
      InputField that = (InputField) obj;
      return super.equals(that)
          && this.name.equals(that.name);
    }


    @Override
    public void accept(SchemaAndDataChangeVisitor visitor) {
      visitor.visit(this);
    }
  }


  /**
   * Constructs an Merge Statement.
   *
   * <p>Usage is discouraged; this method will be deprecated at some point. Use
   * {@link #merge()} for preference.</p>
   */
  public MergeStatement() {
    super();
    this.tableUniqueKey = AliasedField.immutableDslEnabled() ? ImmutableList.of() : Lists.newArrayList();
    this.ifUpdating = ImmutableList.of(); // use MergeStatementBuilder to specify ifUpdating() expressions
    this.whenMatchedAction = Optional.empty();
  }


  /**
   * Builder constructor.
   *
   * @param builder The builder
   */
  MergeStatement(MergeStatementBuilder builder) {
    super();
    this.table = builder.getTable();
    this.tableUniqueKey = AliasedField.immutableDslEnabled()
        ? ImmutableList.copyOf(builder.getTableUniqueKey())
        : Lists.newArrayList(builder.getTableUniqueKey());
    this.selectStatement = builder.getSelectStatement();
    this.ifUpdating = ImmutableList.copyOf(builder.getIfUpdating());
    this.whenMatchedAction = builder.getWhenMatchedAction();
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public MergeStatement deepCopy() {
    return deepCopy(noTransformation()).build();
  }


  /**
   * Merges into a specific table.
   *
   * <blockquote><pre>merge().into(tableRef("agreement"));</pre></blockquote>
   *
   * @param intoTable the table to merge into.
   * @return a statement with the changes applied.
   */
  public MergeStatement into(TableReference intoTable) {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().into(intoTable).build();
    } else {
      this.table = intoTable;
      return this;
    }
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
   * @return a statement with the changes applied.
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
   * @return a statement with the changes applied.
   */
  public MergeStatement tableUniqueKey(List<AliasedField> keyFields) {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().tableUniqueKey(keyFields).build();
    } else {
      this.tableUniqueKey.addAll(keyFields);
      return this;
    }
  }


  /**
   * Specifies the select statement to use as a source of the data.
   *
   * @param statement the source statement.
   * @return a statement with the changes applied.
   */
  public MergeStatement from(SelectStatement statement) {
    if (AliasedField.immutableDslEnabled()) {
      return shallowCopy().from(statement).build();
    } else {
      if (statement.getOrderBys().size() != 0) {
        throw new IllegalArgumentException("ORDER BY is not permitted in the SELECT part of a merge statement (SQL Server limitation)");
      }
      this.selectStatement = statement;
      return this;
    }
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
   * Gets a list of the expressions to be used upon existing records.
   *
   * @return the update expressions.
   */
  public List<AliasedField> getIfUpdating() {
    return ifUpdating;
  }


  /**
   * Gets the action to take when records match.
   *
   * @return the match clause, or Optional.empty() if default behavior should be used
   */
  public Optional<MergeMatchClause> getWhenMatchedAction() {
    return whenMatchedAction;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SQL MERGE INTO [" + table + "] USING [" + selectStatement + "] KEY [" + tableUniqueKey + "] IF UPDATING [" + ifUpdating + "]" +
           (whenMatchedAction.isPresent() ? whenMatchedAction.get() : "");
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (selectStatement == null ? 0 : selectStatement.hashCode());
    result = prime * result + (table == null ? 0 : table.hashCode());
    result = prime * result + (tableUniqueKey == null ? 0 : tableUniqueKey.hashCode());
    result = prime * result + (ifUpdating == null ? 0 : ifUpdating.hashCode());
    result = prime * result + (whenMatchedAction.isEmpty() ? 0 : whenMatchedAction.get().hashCode());
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
    MergeStatement other = (MergeStatement) obj;
    if (selectStatement == null) {
      if (other.selectStatement != null)
        return false;
    } else if (!selectStatement.equals(other.selectStatement))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (tableUniqueKey == null) {
      if (other.tableUniqueKey != null)
        return false;
    } else if (!tableUniqueKey.equals(other.tableUniqueKey))
      return false;
    if (ifUpdating == null) {
      if (other.ifUpdating != null)
        return false;
    } else if (!ifUpdating.equals(other.ifUpdating))
      return false;
    if (!whenMatchedAction.equals(other.whenMatchedAction))
        return false;
    return true;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser
      .dispatch(getTableUniqueKey())
      .dispatch(getIfUpdating())
      .dispatch(getTable())
      .dispatch(getSelectStatement());
    getWhenMatchedAction().ifPresent(traverser::dispatch);
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public MergeStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new MergeStatementBuilder(this, transformer);
  }


  /**
   * Performs a shallow copy to a builder, allowing a duplicate
   * to be created and modified.
   *
   * @return A builder, initialised as a duplicate of this statement.
   */
  @Override
  public MergeStatementBuilder shallowCopy() {
    return new MergeStatementBuilder(this);
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);

    if(selectStatement != null) {
      selectStatement.accept(visitor);
    }
    if(tableUniqueKey != null) {
      tableUniqueKey.stream().forEach(tuk -> tuk.accept(visitor));
    }
    if(ifUpdating != null) {
      ifUpdating.stream().forEach(iu -> iu.accept(visitor));
    }
    if (whenMatchedAction.isPresent()) {
      whenMatchedAction.get().accept(visitor);
    }
  }
}