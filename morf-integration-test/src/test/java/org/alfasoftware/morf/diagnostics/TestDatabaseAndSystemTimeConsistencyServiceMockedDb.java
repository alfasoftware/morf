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

import static junitparams.JUnitParamsRunner.$;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.alfasoftware.morf.diagnostics.DatabaseAndSystemTimeConsistencyChecker.CurrentSystemTime;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;

/**
 * Unit tests of {@link DatabaseAndSystemTimeConsistencyChecker}
 * 
 * @author Copyright (c) Alfa Financial Software 2015
 */
@RunWith(JUnitParamsRunner.class)
public class TestDatabaseAndSystemTimeConsistencyServiceMockedDb {

  private DatabaseAndSystemTimeConsistencyChecker service;

  @Rule
  public ExpectedException                        expectedException = ExpectedException.none();

  @Mock
  private ConnectionResources                     connectionResources;

  @Mock
  private SqlScriptExecutorProvider               sqlScriptExecutorProvider;

  @Mock
  private CurrentSystemTime                       currentSystemTime;

  @Mock
  private SqlScriptExecutor                       sqlScriptExecutor;

  @Mock
  private SqlDialect                              sqlDialect;

  @Mock
  private AppenderSkeleton                        appenderSkeleton;


  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Logger rootLogger = Logger.getRootLogger();
    rootLogger.removeAllAppenders();
    rootLogger.setLevel(Level.TRACE);
    rootLogger.addAppender(appenderSkeleton);

    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);
    when(sqlScriptExecutorProvider.get()).thenReturn(sqlScriptExecutor);

    service = new DatabaseAndSystemTimeConsistencyChecker(connectionResources, sqlScriptExecutorProvider, currentSystemTime);
  }

  /**
   * LoggingEvent matcher
   * 
   * @author Copyright (c) Alfa Financial Software 2015
   */
  private class LoggingEventMatcher extends ArgumentMatcher<LoggingEvent> {

    private final Level  level;
    private final String message;


    /**
     * @param level
     * @param message
     */
    public LoggingEventMatcher(Level level, String message) {
      super();
      this.level = level;
      this.message = message;
    }


    /**
     * @see org.mockito.ArgumentMatcher#matches(java.lang.Object)
     */
    @Override
    public boolean matches(Object argument) {
      LoggingEvent event = (LoggingEvent) argument;
      if (level.equals(event.getLevel()) && message.equals(event.getMessage()))
        return true;
      return false;
    }

  }


  @SuppressWarnings("unchecked")
  @Test
  public void noSignificantDifference() {
    // given
    when(currentSystemTime.currentSystemTime()).thenReturn(1L).thenReturn(10L);
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(5L);

    // when
    service.verifyDatabaseAndSystemTimeConsitency();

    // then
    verify(appenderSkeleton, times(1)).doAppend(
      argThat(new LoggingEventMatcher(Level.DEBUG,
          "system-time-before-query: [1], database-time: [5], system-time-after-query: [10]")));
  }


  @SuppressWarnings("unused")
  private Object[] parametersForLookForSignificantTimeDifferenceProblemsTest() {
    return $(
      $(DatabaseAndSystemTimeConsistencyChecker.WARN_DIFF_VALUE, false, Level.WARN, false));
  }


  @SuppressWarnings("unchecked")
  @Test
  @Parameters
  public void lookForSignificantTimeDifferenceProblemsTest(long baseSystemTimeDiff, boolean suppressExceptionJavaPropValue,
      Level messageLevel, boolean exceptionExpected) {
    // given
    when(currentSystemTime.currentSystemTime()).thenReturn(1L).thenReturn(
      baseSystemTimeDiff + 50L);
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(5L);
    if (exceptionExpected) {
      expectedException.expect(IllegalStateException.class);
    }
    System.setProperty(DatabaseAndSystemTimeConsistencyChecker.SUPPRESS_EXCEPTION_JAVA_PROPERTY,
      Boolean.toString(suppressExceptionJavaPropValue));

    // when
    service.verifyDatabaseAndSystemTimeConsitency();

    // then
    verify(appenderSkeleton, times(1)).doAppend(
      argThat(new LoggingEventMatcher(Level.DEBUG,
          "system-time-before-query: [1], database-time: [5], system-time-after-query: ["
              + (baseSystemTimeDiff + 50L) + "]")));

    verify(appenderSkeleton, times(1)).doAppend(
      argThat(new LoggingEventMatcher(messageLevel,
          "Database and system times are different by: ["
              + (baseSystemTimeDiff + 45L)
              + "] system-time-before-query: [1], database-time: [5], system-time-after-query: ["
              + (baseSystemTimeDiff + 50L) + "]")));
  }
}
