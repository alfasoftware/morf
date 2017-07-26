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

import java.math.BigDecimal;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.joda.time.LocalDate;


/**
 * Provides a representation of a literal field value to be used in a {@link Statement}
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class FieldLiteral extends AliasedField {

  /**
   * The literal value for the field
   */
  private final String value;

  /**
   * The type of the literal field
   */
  private final DataType dataType;


  /**
   * Constructor used to create the deep copy of this field literal
   *
   * @param sourceField the field literal to create the copy from
   */
  private FieldLiteral(FieldLiteral sourceField) {
    super();

    this.value = sourceField.value;
    this.dataType = sourceField.dataType;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as string source.
   *
   * @param stringValue the literal value to use
   */
  public FieldLiteral(String stringValue) {
    super();

    this.value = stringValue;
    this.dataType = DataType.STRING;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Double source.
   *
   * @param doubleValue the literal value to use
   */
  public FieldLiteral(Double doubleValue) {
    super();

    this.value = doubleValue != null ? doubleValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a BigDecimal source.
   *
   * @param bigDecimalValue the literal value to use
   */
  public FieldLiteral(BigDecimal bigDecimalValue) {
    super();

    this.value = bigDecimalValue != null ? bigDecimalValue.toPlainString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with an Integer source.
   *
   * @param integerValue the literal value to use
   */
  public FieldLiteral(Integer integerValue) {
    super();

    this.value = integerValue != null ? integerValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a {@link LocalDate} source.
   *
   * @param localDateValue the literal value to use
   */
  public FieldLiteral(LocalDate localDateValue) {
    super();
    this.value = localDateValue != null ? localDateValue.toString("yyyy-MM-dd") : null;
    this.dataType = DataType.DATE;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a Long source.
   *
   * @param longValue the literal value to use
   */
  public FieldLiteral(Long longValue) {
    super();

    this.value = longValue != null ? longValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Character source.
   *
   * @param charValue the literal value to use
   */
  public FieldLiteral(Character charValue) {
    super();

    this.value = charValue != null ? charValue.toString() : null;
    this.dataType = DataType.STRING;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Boolean source.
   *
   * @param booleanValue the literal value to use
   */
  public FieldLiteral(Boolean booleanValue) {
    super();

    this.value = booleanValue != null ? booleanValue.toString() : null;
    this.dataType = DataType.BOOLEAN;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a Null source.
   *
   */
  protected FieldLiteral() {
    super();

    this.value = null;
    this.dataType = DataType.NULL;
  }


  /**
   * Constructor specifying data type.
   *
   * @param value The value of the field.
   * @param dataType The data type of the field.
   */
  public FieldLiteral(String value, DataType dataType) {
    super();
    this.value = value;
    this.dataType = dataType;
  }


  /**
   * Constructs a {@linkplain FieldLiteral} from a specified object.
   *
   * @param object the object to construct the {@linkplain FieldLiteral} from
   * @return the new {@linkplain FieldLiteral}
   */
  public static FieldLiteral fromObject(Object object) {
    if (object instanceof String) {
      return new FieldLiteral((String) object);
    }

    if (object instanceof Double) {
      return new FieldLiteral((Double) object);
    }

    if (object instanceof Integer) {
      return new FieldLiteral((Integer) object);
    }

    if (object instanceof Character) {
      return new FieldLiteral((Character) object);
    }

    return new FieldLiteral(object.toString());
  }


  /**
   * @return the value
   */
  public String getValue() {
    return value;
  }


  /**
   * @return the dataType
   */
  public DataType getDataType() {
    return dataType;
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#deepCopyInternal(DeepCopyTransformation)
   */
  @Override
  protected AliasedField deepCopyInternal(DeepCopyTransformation transformer) {
    return new FieldLiteral(this);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#as(java.lang.String)
   */
  @Override
  public FieldLiteral as(String aliasName) {
    super.as(aliasName);
    return this;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return dataType.equals(DataType.STRING) ? "\"" + value + "\"" : value;
  }
}