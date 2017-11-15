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

import org.alfasoftware.morf.metadata.DataSetUtils.StatementParametersBuilder;
import org.joda.time.LocalDate;

/**
 * Implements {@link StatementParametersBuilder}.
 */
class StatementParametersBuilderImpl extends DataValueLookupBuilderImpl implements StatementParametersBuilder {

  @Override
  public StatementParametersBuilder withInitialColumnCount(int count) {
    return (StatementParametersBuilder) super.withInitialColumnCount(count);
  }

  @Override
  public StatementParametersBuilder value(String columnName, String value) {
    return (StatementParametersBuilder) super.value(columnName, value);
  }

  @Override
  public StatementParametersBuilder setBigDecimal(String columnName, BigDecimal value) {
    return (StatementParametersBuilder) super.setBigDecimal(columnName, value);
  }

  @Override
  public StatementParametersBuilder setBoolean(String columnName, Boolean value) {
    return (StatementParametersBuilder) super.setBoolean(columnName, value);
  }

  @Override
  public StatementParametersBuilder setDate(String columnName, java.sql.Date value) {
    return (StatementParametersBuilder) super.setDate(columnName, value);
  }

  @Override
  public StatementParametersBuilder setDouble(String columnName, Double value) {
    return (StatementParametersBuilder) super.setDouble(columnName, value);
  }

  @Override
  public StatementParametersBuilder setInteger(String columnName, Integer value) {
    return (StatementParametersBuilder) super.setInteger(columnName, value);
  }

  @Override
  public StatementParametersBuilder setLocalDate(String columnName, LocalDate value) {
    return (StatementParametersBuilder) super.setLocalDate(columnName, value);
  }

  @Override
  public StatementParametersBuilder setLong(String columnName, Long value) {
    return (StatementParametersBuilder) super.setLong(columnName, value);
  }

  @Override
  public StatementParametersBuilder setObject(String columnName, Object value) {
    return (StatementParametersBuilder) super.setObject(columnName, value);
  }

  @Override
  public StatementParametersBuilder setString(String columnName, String value) {
    return (StatementParametersBuilder) super.setString(columnName, value);
  }

  @Override
  public StatementParametersBuilder setByteArray(String columnName, byte[] value) {
    return (StatementParametersBuilder) super.setByteArray(columnName, value);
  }
}
