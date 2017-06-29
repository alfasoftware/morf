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

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Callback;

/**
 * Implementation of the Callback interface which type switches on SQL statements and elements.
 * To use, extend this class and pass to {@link ObjectTreeTraverser}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@SuppressWarnings("unused")
public class SqlElementCallback implements Callback {

  /**
   * Invoked on each element of the SQL statement that is being traversed.
   * This similar to the Visitor pattern but ObjectTreeTraverser has the
   * calling responsibility.
   *
   * @param object the object to invoke the callback on.
   */
  @Override
  public final void visit(Object object) {
    if (object instanceof Join) {
      visit((Join) object);
    } else if (object instanceof SelectStatement) {
      visit((SelectStatement) object);
    } else if (object instanceof Criterion) {
      visit((Criterion) object);
    } else if (object instanceof TableReference) {
      visit((TableReference) object);
    } else if (object instanceof SelectFirstStatement) {
      visit((SelectFirstStatement) object);
    } else if (object instanceof AliasedField) {
      visit((AliasedField) object);
    } else if (object instanceof UpdateStatement) {
      visit((UpdateStatement) object);
    } else if (object instanceof TruncateStatement) {
      visit((TruncateStatement) object);
    } else if (object instanceof DeleteStatement) {
      visit((DeleteStatement) object);
    } else if (object instanceof SetOperator) {
      visit((SetOperator) object);
    } else if (object instanceof WhenCondition) {
      visit((WhenCondition) object);
    } else if (object instanceof InsertStatement) {
      visit((InsertStatement) object);
    } else if (object instanceof MergeStatement) {
      visit((MergeStatement) object);
    } else {
      visitUnknown(object);
    }
  }

  protected void visitUnknown(Object object) { }
  protected void visit(Join join) {}
  protected void visit(SelectStatement selectStatement) {}
  protected void visit(Criterion criterion) { }
  protected void visit(TableReference tableReference) {}
  protected void visit(SelectFirstStatement statement) {}
  protected void visit(AliasedField field) {}
  protected void visit(UpdateStatement updateStatement) {}
  protected void visit(TruncateStatement truncateStatement) {}
  protected void visit(DeleteStatement deleteStatement) {}
  protected void visit(SetOperator setOperator) {}
  protected void visit(WhenCondition whenCondition) {}
  protected void visit(InsertStatement insertStatement) {}
  protected void visit(MergeStatement mergeStatement) {}
}
