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

package org.alfasoftware.morf.dataset;

import static org.hamcrest.Matchers.equalTo;

import java.util.HashMap;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Matcher for {@link Record}.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
public final class BaseRecordMatcher extends BaseMatcher<Record> {

  private static final Logger log = LoggerFactory.getLogger(BaseRecordMatcher.class);

  // If you find yourself wondering why we store these separately, it's so that we can
  // check the matches using getValue() and getObject().  getObject() will not have been
  // uniformly implemented on mocks in code dating from before the method was created.
  // Until we completely refactor all use of getValue(), this is safest.
  private final Map<String, Matcher<String>> stringValues = new HashMap<>();
  private final Map<Column, Matcher<?>> objectValues = new HashMap<>();


  public static BaseRecordMatcher create() {
    return new BaseRecordMatcher();
  }


  private BaseRecordMatcher() {
  }


  public BaseRecordMatcher withValue(String field, Matcher<String> value) {
    stringValues.put(field, value);
    return this;
  }


  public BaseRecordMatcher withValue(String field, String value) {
    stringValues.put(field, equalTo(value));
    return this;
  }


  public BaseRecordMatcher withObject(Column column, Matcher<Object> value) {
    objectValues.put(column, value);
    return this;
  }


  public BaseRecordMatcher withObject(Column column, Object value) {
    objectValues.put(column, equalTo(value));
    return this;
  }


  @Override
  public boolean matches(Object item) {
    if (!(item instanceof Record)) {
      return false;
    }
    final Record record = (Record)item;

    // Check values using getValue(), because we know they will have been mocked
    // on mock Record implementations dating to before the creation of getObject().
    // When getValue() is removed, we can remove this
    for (Map.Entry<String, Matcher<String>> entry : stringValues.entrySet()) {
      Matcher<String> expected = entry.getValue();
      @SuppressWarnings("deprecation")
      String actual = record.getValue(entry.getKey());
      if (!expected.matches(actual)) {
        log.warn(actual + " mismatches " + expected);
        return false;
      }
    }

    // This bit will stay though.
    for (Map.Entry<Column, Matcher<?>> entry : objectValues.entrySet()) {
      Matcher<?> expected = entry.getValue();
      Object actual = record.getObject(entry.getKey());
      if (!expected.matches(actual)) {
        log.warn(actual + " mismatches " + expected);
        return false;
      }
    }
    return true;
  }


  @Override
  public void describeTo(Description description) {
    description.appendText("is a Record[");
    boolean first = true;
    for (Map.Entry<String, Matcher<String>> matcher : stringValues.entrySet()) {
      if (!first) {
        description.appendText(", ");
      }
      description
        .appendText(matcher.getKey())
        .appendText(" matches ")
        .appendDescriptionOf(matcher.getValue());
      first = false;
    }
    for (Map.Entry<Column, Matcher<?>> matcher : objectValues.entrySet()) {
      if (!first) {
        description.appendText(", ");
      }
      description
        .appendText(matcher.getKey().getName())
        .appendText(" matches ")
        .appendDescriptionOf(matcher.getValue());
      first = false;
    }
    description.appendText("]");
  }
}