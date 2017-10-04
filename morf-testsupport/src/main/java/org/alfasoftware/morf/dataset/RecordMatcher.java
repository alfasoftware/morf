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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;


/**
 * Matcher for {@link Record}.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
public final class RecordMatcher extends BaseMatcher<Record> {

  private final Map<String, Matcher<String>> fieldValues = new HashMap<>();


  public static RecordMatcher create() {
    return new RecordMatcher();
  }


  private RecordMatcher() {
  }


  public RecordMatcher withValue(String field, Matcher<String> value) {
    fieldValues.put(field, value);
    return this;
  }


  public RecordMatcher withValue(String field, String value) {
    fieldValues.put(field, equalTo(value));
    return this;
  }


  @Override
  public boolean matches(Object item) {
    if (!(item instanceof Record)) {
      return false;
    }
    final Record record = (Record)item;
    for (Map.Entry<String, Matcher<String>> matcher : fieldValues.entrySet()) {
      if (!matcher.getValue().matches(record.getValue(matcher.getKey()))) {
        return false;
      }
    }
    return true;
  }


  @Override
  public void describeTo(Description description) {
    description
      .appendText("is a Record");
    for (Map.Entry<String, Matcher<String>> matcher : fieldValues.entrySet()) {
      description.appendText(" with value of [" + matcher.getKey() + "] that ").appendDescriptionOf(matcher.getValue());
    }
  }
}