package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.sql.SqlUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.when;

public class TestViewChangesDeploymentHelper {

    @Mock
    private SqlDialect dialect;

    private ViewChangesDeploymentHelper changesDeploymentHelper;
    private View testView;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Table testTable     = table("Foo").columns(column("name", DataType.STRING, 32));
        Table deployedViews = table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME).columns(column("name", DataType.STRING, 30), column("hash", DataType.STRING, 64));
        testView      = view("FooView", select(field("name")).from(tableRef("Foo")));

        when(dialect.tableDeploymentStatements(same(testTable))).thenReturn(ImmutableList.of("A"));
        when(dialect.tableDeploymentStatements(same(deployedViews))).thenReturn(ImmutableList.of("B"));
        when(dialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("C"));
        when(dialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("D"));
        when(dialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("E");
        when(dialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("G");
        when(dialect.viewDeploymentStatementsAsLiteral(same(testView))).thenReturn(literal("F"));

        changesDeploymentHelper = new ViewChangesDeploymentHelper(dialect);
    }

    @Test
    public void testDeprecatedCreateView() {
        List<String> results = changesDeploymentHelper.createView(testView);

        Assert.assertTrue(results.size() == 2);
    }

    @Test
    public void testDeprecatedDropViewIfExistsWithUpdate() {
        List<String> results = changesDeploymentHelper.dropViewIfExists(testView, true);

        Assert.assertTrue(results.size() == 1);
    }

    @Test
    public void testDeprecatedDropViewIfExists() {
        List<String> results = changesDeploymentHelper.dropViewIfExists(testView);

        Assert.assertTrue(results.size() == 1);
    }

    @Test
    public void testDeprecatedDeregisterViewIfExists() {
        List<String> results = changesDeploymentHelper.deregisterViewIfExists(testView, true);

        Assert.assertTrue(results.size() == 1);
    }

    @Test
    public void testDeprecatedDeregisterAllViews() {
        List<String> results = changesDeploymentHelper.deregisterAllViews();

        Assert.assertTrue(results.size() == 1);
    }

}