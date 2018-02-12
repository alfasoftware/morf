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

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.FieldFromSelectFirst;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.alfasoftware.morf.util.ShallowCopyable;

/**
 * Class which encapsulates the generation of an SQL statement for first aggregation.
 *
 * The implementation varies in different database platforms.
 *
 * <p>The class structure imitates the end SQL (in Oracle flavour) and is structured as follows:</p>
 *
 * <blockquote><pre>
 *  SelectFirstStatement.selectFirst([field])    = SELECT [fields]
 *    .from([table])                               FROM [table]
 *    .where([criterion])                          WHERE [criterion]
 *    .orderBy([fields])                           ORDER BY [fields]
 *    .build()</pre></blockquote>
 *
 * <p>This class accepts only one {@link FieldReference} reference in the constructor.</p>
 *
 * <p>This class does not accept string references to field or table names. Instead, you must provide
 * the methods with a {@link TableReference} or {@link FieldReference} reference.</p>
 *
 * <p>There must be a {@link #orderBy(org.alfasoftware.morf.sql.element.AliasedField...)} expression on the statement</p>
 *
 * <p>The first row encountered in the table with the given order by will be returned. If there is an ambiguous first row the result is undocumented. One of the rows will be returned.</p>
 *
 * <p>The statement generated can be used standalone, or in a sub-select within an outer expression</p>
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class SelectFirstStatement extends AbstractSelectStatement<SelectFirstStatement>
                               implements DeepCopyableWithTransformation<SelectFirstStatement, SelectFirstStatementBuilder>,
                                          ShallowCopyable<SelectFirstStatement, SelectFirstStatementBuilder>,
                                          Driver {

  private int hashCode;


  /**
   * Constructs a Select First Statement.
   *
   * @param field the field that should be selected
   * @return Builder.
   */
  public static final SelectFirstStatementBuilder selectFirst(AliasedFieldBuilder field) {
    return new SelectFirstStatementBuilder(field);
  }


  /**
   * <p>Usage is discouraged; this constructor will be deprecated at some point. Use
   * {@link #selectFirst(AliasedFieldBuilder)} for preference.</p>
   *
   * @param field The field in the select statemenr
   */
  public SelectFirstStatement(AliasedFieldBuilder field) {
    super(field);
  }

  /**
   * Builder constructor.
   *
   * @param builder The builder.
   */
  SelectFirstStatement(SelectFirstStatementBuilder builder) {
    super(builder);
  }


  @Override
  public SelectFirstStatementBuilder shallowCopy() {
    return new SelectFirstStatementBuilder(this);
  }


  /**
   *
   * @see org.alfasoftware.morf.sql.Statement#deepCopy()
   */
  @Override
  public SelectFirstStatement deepCopy() {
    return deepCopy(noTransformation()).build();
  }


  /**
   * @see org.alfasoftware.morf.sql.AbstractSelectStatement#asField()
   */
  @Override
  public AliasedField asField() {
    return new FieldFromSelectFirst(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.AbstractSelectStatement#toString()
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("SQL SELECT FIRST ");
    result.append(super.toString());
    return result.toString();
  }


  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = super.hashCode();
    }
    return hashCode;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    if (hashCode() != obj.hashCode())
      return false;
    return super.equals(obj);
  }


  /**
   * @see org.alfasoftware.morf.util.DeepCopyableWithTransformation#deepCopy(org.alfasoftware.morf.util.DeepCopyTransformation)
   */
  @Override
  public SelectFirstStatementBuilder deepCopy(DeepCopyTransformation transformer) {
    return new SelectFirstStatementBuilder(this, transformer);
  }
}