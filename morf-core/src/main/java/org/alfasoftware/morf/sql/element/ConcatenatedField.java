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

import java.util.List;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * A field which is made up of the concatenation of other
 * {@linkplain AliasedField}s.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class ConcatenatedField extends AliasedField implements Driver {

  /**
   * The fields to be concatenated.
   */
  private final ImmutableList<AliasedField> fields;


  /**
   * Returns an expression concatenating all the passed expressions.
   *
   * @param fields the expressions to concatenate.
   * @return the expression concatenating the passed expressions.
   */
  public static ConcatenatedField concat(AliasedField... fields) {
    return new ConcatenatedField(fields);
  }


  /**
   * Constructs a ConcatenatedField.
   *
   * @param fields the fields to be concatenated
   * @deprecated Use {@link #concat(AliasedField...)}
   */
  @Deprecated
  public ConcatenatedField(AliasedField... fields) {
    super();
    // We need at least two fields to concatenate
    if (fields.length < 2) {
      throw new IllegalArgumentException("A concatenated field requires at least two fields to concatenate.");
    }
    this.fields = ImmutableList.copyOf(fields);
  }


  private ConcatenatedField(String alias, ImmutableList<AliasedField> fields) {
    super(alias);
    this.fields = fields;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new ConcatenatedField(getAlias(), FluentIterable.from(fields).transform(transformer::deepCopy).toList());
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new ConcatenatedField(aliasName, fields);
  }


  /**
   * Get the fields to be concatenated
   *
   * @return the fields to be concatenated
   */
  public List<AliasedField> getConcatenationFields() {
    return fields;
  }


  /**
   * @see org.alfasoftware.morf.util.ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)
   */
  @Override
  public void drive(ObjectTreeTraverser traverser) {
    traverser.dispatch(getConcatenationFields());
  }


  /**
   *
   * @see org.alfasoftware.morf.sql.element.AliasedField#toString()
   */
  @Override
  public String toString() {
    return "CONCAT(" + StringUtils.join(fields, ", ") + ")" + super.toString();
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    ConcatenatedField other = (ConcatenatedField) obj;
    if (fields == null) {
      if (other.fields != null)
        return false;
    } else if (!fields.equals(other.fields))
      return false;
    return true;
  }
}