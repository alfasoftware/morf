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

package org.alfasoftware.morf.metadata;

import java.math.BigDecimal;

import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.joda.time.LocalDate;

/**
 * Implements {@link RecordBuilder}.
 */
class RecordBuilderImpl extends DataValueLookupBuilderImpl implements RecordBuilder {

  @Override
  public RecordBuilder withInitialColumnCount(int count) {
    return (RecordBuilder) super.withInitialColumnCount(count);
  }

  @Override
  public RecordBuilder value(String columnName, String value) {
    return (RecordBuilder) super.value(columnName, value);
  }

  @Override
  public RecordBuilder setBigDecimal(String columnName, BigDecimal value) {
    return (RecordBuilder) super.setBigDecimal(columnName, value);
  }

  @Override
  public RecordBuilder setBoolean(String columnName, Boolean value) {
    return (RecordBuilder) super.setBoolean(columnName, value);
  }

  @Override
  public RecordBuilder setDate(String columnName, java.sql.Date value) {
    return (RecordBuilder) super.setDate(columnName, value);
  }

  @Override
  public RecordBuilder setDouble(String columnName, Double value) {
    return (RecordBuilder) super.setDouble(columnName, value);
  }

  @Override
  public RecordBuilder setInteger(String columnName, Integer value) {
    return (RecordBuilder) super.setInteger(columnName, value);
  }

  @Override
  public RecordBuilder setLocalDate(String columnName, LocalDate value) {
    return (RecordBuilder) super.setLocalDate(columnName, value);
  }

  @Override
  public RecordBuilder setLong(String columnName, Long value) {
    return (RecordBuilder) super.setLong(columnName, value);
  }

  @Override
  public RecordBuilder setObject(String columnName, Object value) {
    return (RecordBuilder) super.setObject(columnName, value);
  }

  @Override
  public RecordBuilder setString(String columnName, String value) {
    return (RecordBuilder) super.setString(columnName, value);
  }

  @Override
  public RecordBuilder setByteArray(String columnName, byte[] value) {
    return (RecordBuilder) super.setByteArray(columnName, value);
  }
}
