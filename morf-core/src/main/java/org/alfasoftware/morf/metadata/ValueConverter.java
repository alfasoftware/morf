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

import org.alfasoftware.morf.metadata.ValueConverters.BooleanValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.DateValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.IntegerConverter;
import org.alfasoftware.morf.metadata.ValueConverters.NullValueConverter;


/**
 * Lightweight means of handling a virtual dispatch type conversion for field values.
 * Minimises runtime dispatch (by using virtual method invocation to fast track
 * to the right type conversion).
 *
 * <p>Also minimises object churn by being <strong>stateless</strong>.  Generally
 * implementations are singletons and paired with the value they represent.
 * Goodbye object-oriented programming - it was nice knowing you.</p>
 *
 * <p>Null values should be paired with {@link NullValueConverter}, so other implementations
 * don't need to implement null checks.</p>
 *
 * <p>See {@link IntegerConverter}, {@link DateValueConverter}, {@link BooleanValueConverter} etc.</p>
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
interface ValueConverter<T> {
  BigDecimal bigDecimalValue(T value);
  Boolean booleanValue(T value);
  byte[] byteArrayValue(T value);
  java.sql.Date dateValue(T value);
  Double doubleValue(T value);
  Integer integerValue(T value);
  org.joda.time.LocalDate localDateValue(T value);
  Long longValue(T value);
  String stringValue(T value);
}