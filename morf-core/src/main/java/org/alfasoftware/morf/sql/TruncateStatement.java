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

import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;

/**
 * Generates a statements suitable for truncating a table.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TruncateStatement implements Statement,
                                          DeepCopyableWithTransformation<TruncateStatement, Builder<TruncateStatement>>,
                                          Driver {

  /**
   * The table to update
   */
  private final TableReference table;


  /**
   * Constructs a Truncate Statement.
   *
   * @param table The table to truncate.
   * @return Builder.
   */
  public static Builder<TruncateStatement> truncate(TableReference table) {
    return () -> new TruncateStatement(table);
  }


  /**
   * <p>Usage is discouraged; this constructor will be deprecated at some point. Use
   * {@link #truncate(TableReference)} for preference.</p>
   *
   * @param table The table to truncate.
   */
  public TruncateStatement(TableReference table) {
    this.table = table;
  }


  /**
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public TruncateStatement deepCopy() {
    return deepCopy(DeepCopyTransformations.noTransformation()).build();
  }


  /**
   * @return the table
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SQL TRUNCATE TABLE [" + table + "]";
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (table == null ? 0 : table.hashCode());
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
    TruncateStatement other = (TruncateStatement) obj;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getTable());
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public Builder<TruncateStatement> deepCopy(DeepCopyTransformation transformer) {
    return TempTransitionalBuilderWrapper.wrapper(new TruncateStatement(transformer.deepCopy(table)));
  }


  @Override
  public void accept(SchemaAndDataChangeVisitor visitor) {
    visitor.visit(this);
  }
}