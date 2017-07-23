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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang.StringUtils;

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
  private final List<AliasedField> fields;


  /**
   * Constructs a ConcatenatedField.
   *
   * @param fields the fields to be concatenated
   */
  public ConcatenatedField(AliasedField... fields) {
    super();
    // We need at least two fields to concatenate
    if (fields.length < 2) {
      throw new IllegalArgumentException("A concatenated field requires at least two fields to concatenate.");
    }
    this.fields = Arrays.asList(fields);
  }


  /**
   * Private constructor used to carry out a deep copy.
   *
   * @param concatenatedField the concatenated field to deep copy
   * @param transformer The transformation to be executed during the copy
   */
  private ConcatenatedField(ConcatenatedField concatenatedField,DeepCopyTransformation transformer) {
    super();

    this.fields = new ArrayList<>();

    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      this.fields.add(transformer.deepCopy(field));
    }
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new ConcatenatedField(this,transformer);
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
}