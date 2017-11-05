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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
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
   * @return A literal representing NULL.
   */
  public static FieldLiteral nullLiteral() {
    return new FieldLiteral("", null, DataType.NULL);
  }


  /**
   * @param alias The alias for the NULL.
   * @return A literal representing NULL.
   */
  public static FieldLiteral nullLiteral(String alias) {
    return new FieldLiteral(alias, null, DataType.NULL);
  }


  /**
   * @param stringValue Any string.
   * @return A string literal.
   */
  public static FieldLiteral literal(final String stringValue) {
    return new FieldLiteral("", stringValue, DataType.STRING);
  }


  /**
   * @param value A stringified value.
   * @param dataType The data type represented by the stringified value.
   * @return An appropriately typed literal.
   */
  public static FieldLiteral literal(final String value, final DataType dataType) {
    return new FieldLiteral("", value, dataType);
  }


  /**
   * @param doubleValue Any double.
   * @return A numeric literal.
   */
  public static FieldLiteral literal(final Double doubleValue) {
    return new FieldLiteral("", doubleValue != null ? doubleValue.toString() : null, DataType.DECIMAL);
  }


  /**
   * @param bigDecimalValue Any decimal.
   * @return A numeric literal.
   */
  public static FieldLiteral literal(final BigDecimal bigDecimalValue) {
    return new FieldLiteral("", bigDecimalValue != null ? bigDecimalValue.toPlainString() : null, DataType.DECIMAL);
  }


  /**
   * @param integerValue Any integer.
   * @return An integer literal.
   */
  public static FieldLiteral literal(final Integer integerValue) {
    return new FieldLiteral("", integerValue != null ? integerValue.toString() : null, DataType.DECIMAL);
  }


  /**
   * @param localDateValue Any date.
   * @return A date literal.
   */
  public static FieldLiteral literal(final LocalDate localDateValue) {
    return new FieldLiteral("", localDateValue != null ? localDateValue.toString("yyyy-MM-dd") : null, DataType.DATE);
  }


  /**
   * @param longValue Any long.
   * @return A big integer literal.
   */
  public static FieldLiteral literal(final Long longValue) {
    return new FieldLiteral("", longValue != null ? longValue.toString() : null, DataType.DECIMAL);
  }


  /**
   * @param charValue Any character.
   * @return A string literal.
   */
  public static FieldLiteral literal(final Character charValue) {
    return new FieldLiteral("", charValue != null ? charValue.toString() : null, DataType.STRING);
  }


  /**
   * @param booleanValue Any boolean..
   * @return A boolean literal.
   */
  public static FieldLiteral literal(final Boolean booleanValue) {
    return new FieldLiteral("", booleanValue != null ? booleanValue.toString() : null, DataType.BOOLEAN);
  }


  /**
   * TODO make private when {@link NullFieldLiteral} is removed
   *
   * @param alias The alias.
   * @param value The value.
   * @param dataType The data type.
   */
  protected FieldLiteral(final String alias, final String value, final DataType dataType) {
    super(alias);
    this.value = value;
    this.dataType = dataType;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as string source.
   *
   * @param stringValue the literal value to use
   * @deprecated Use {@link #literal(String)}
   */
  @Deprecated
  public FieldLiteral(final String stringValue) {
    super();

    this.value = stringValue;
    this.dataType = DataType.STRING;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Double source.
   *
   * @param doubleValue the literal value to use
   * @deprecated Use {@link #literal(Double)}
   */
  @Deprecated
  public FieldLiteral(final Double doubleValue) {
    super();

    this.value = doubleValue != null ? doubleValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a BigDecimal source.
   *
   * @param bigDecimalValue the literal value to use
   * @deprecated Use {@link #literal(BigDecimal)}
   */
  @Deprecated
  public FieldLiteral(final BigDecimal bigDecimalValue) {
    super();

    this.value = bigDecimalValue != null ? bigDecimalValue.toPlainString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with an Integer source.
   *
   * @param integerValue the literal value to use
   * @deprecated Use {@link #literal(Integer)}
   */
  @Deprecated
  public FieldLiteral(final Integer integerValue) {
    super();

    this.value = integerValue != null ? integerValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a {@link LocalDate} source.
   *
   * @param localDateValue the literal value to use
   * @deprecated Use {@link #literal(LocalDate)}
   */
  @Deprecated
  public FieldLiteral(final LocalDate localDateValue) {
    super();
    this.value = localDateValue != null ? localDateValue.toString("yyyy-MM-dd") : null;
    this.dataType = DataType.DATE;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a Long source.
   *
   * @param longValue the literal value to use
   * @deprecated Use {@link #literal(Long)}
   */
  @Deprecated
  public FieldLiteral(final Long longValue) {
    super();

    this.value = longValue != null ? longValue.toString() : null;
    this.dataType = DataType.DECIMAL;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Character source.
   *
   * @param charValue the literal value to use
   * @deprecated Use {@link #literal(Character)}
   */
  @Deprecated
  public FieldLiteral(final Character charValue) {
    super();

    this.value = charValue != null ? charValue.toString() : null;
    this.dataType = DataType.STRING;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with as Boolean source.
   *
   * @param booleanValue the literal value to use
   * @deprecated Use {@link #literal(Boolean)}
   */
  @Deprecated
  public FieldLiteral(final Boolean booleanValue) {
    super();

    this.value = booleanValue != null ? booleanValue.toString() : null;
    this.dataType = DataType.BOOLEAN;
  }


  /**
   * Constructs a new {@linkplain FieldLiteral} with a Null source.
   *
   */
  @Deprecated
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
   * @deprecated Use {@link #literal(String)} and specify the data type using the builder.
   */
  @Deprecated
  public FieldLiteral(final String value, final DataType dataType) {
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
      return literal((String) object);
    }

    if (object instanceof Double) {
      return literal((Double) object);
    }

    if (object instanceof Integer) {
      return literal((Integer) object);
    }

    if (object instanceof Character) {
      return literal((Character) object);
    }

    return literal(object.toString());
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
  protected FieldLiteral deepCopyInternal(final DeepCopyTransformation transformer) {
    return new FieldLiteral(this.getAlias(), this.value, this.dataType);
  }


  @Override
  protected AliasedField shallowCopy(String aliasName) {
    return new FieldLiteral(aliasName, this.value, this.dataType);
  }


  /**
   * @see org.alfasoftware.morf.sql.element.AliasedField#as(java.lang.String)
   */
  @Override
  public FieldLiteral as(String aliasName) {
    return (FieldLiteral) super.as(aliasName);
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FieldLiteral other = (FieldLiteral) obj;
    return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(this.value, other.value)
        .append(this.dataType, other.dataType)
        .isEquals();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(this.value)
        .append(this.dataType)
        .toHashCode();
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return dataType.equals(DataType.STRING)
        ? "\"" + value + "\""
        : value == null ? "NULL" : value;
  }
}