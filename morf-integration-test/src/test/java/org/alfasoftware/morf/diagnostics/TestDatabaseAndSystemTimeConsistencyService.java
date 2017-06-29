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

package org.alfasoftware.morf.diagnostics;

import org.junit.Rule;
import org.junit.Test;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.testing.TestingDataSourceModule;

import com.google.inject.Inject;

/**
 * Tests of {@link DatabaseAndSystemTimeConsistencyChecker}
 * 
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class TestDatabaseAndSystemTimeConsistencyService {

  @Rule
  public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject
  private DatabaseAndSystemTimeConsistencyChecker databaseTimeService;


  /**
   * Due to the nature of the class there is not much we can test here but the
   * fact if the query is ok and if the integration is right. That's what is
   * done below. See mocked test for more indept testing.
   */
  @Test
  public void testIntegration() {
    databaseTimeService.verifyDatabaseAndSystemTimeConsitency();
  }
}
