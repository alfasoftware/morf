package org.alfasoftware.morf.jdbc;

import org.hamcrest.Matchers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Copyright (c) Alfa Financial Software 2022
 */
public abstract class AbstractSqlDialectTestWithNoSupportForWindowFunctions extends AbstractSqlDialectTest{

    protected final void verifyExceptionForNoOrderByClause(Exception e) {
        verifyWindowFunctionNotSupported(e);
    }

    @Override
    protected void verifyNoExceptionForWithoutPartitionClause(String result) {
        fail("Window function not supported. UnsupportedOperationException should be thrown");
    }

    @Override
    protected void verifyException(Exception e) {
        verifyWindowFunctionNotSupported(e);
    }

    @Override
    protected void verifyNoExceptionForWithPartitionClause(String result) {
        fail("Window function not supported. UnsupportedOperationException should be thrown");
    }

    @Override
    protected void verifyNoExceptionForWithoutOrderByClause() {
        fail("Window function not supported. UnsupportedOperationException should be thrown");
    }

    private void verifyWindowFunctionNotSupported(Exception e){
        assertThat(e, Matchers.instanceOf(UnsupportedOperationException.class));
        assertThat(e.getMessage(), Matchers.containsString("does not support window functions"));
    }
}
