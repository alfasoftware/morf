package org.alfasoftware.morf.jdbc;

import com.google.inject.Inject;
import net.jcip.annotations.NotThreadSafe;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider.createRealName;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link DatabaseMetaDataProvider.UnsupportedDataTypeColumn}.
 *
 * @author Copyright (c) Alfa Financial Software 2023
 */
@NotThreadSafe
public class TestUnsupportedDataTypeColumn {


    @Rule
    public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

    @Inject
    private DatabaseSchemaManager schemaManager;

    @Inject
    private ConnectionResources database;

    private DatabaseMetaDataProvider.UnsupportedDataTypeColumn  column;
    private final ResultSet resultSet = mock(ResultSet.class);
    private final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

    @Before
    public void setUp() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(0);
        column = new DatabaseMetaDataProvider.UnsupportedDataTypeColumn(createRealName("columnName", "test"), "typeName", -123, 0, 0, resultSet);

    }


    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testGetWidth() {
        column.getWidth();
    }

    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testGetScale() {
        column.getScale();
    }

    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testIsNullable() {
        column.isNullable();
    }

    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testIsPrimaryKey() {
        column.isPrimaryKey();
    }

    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testIsAutonumbered() {
        column.isAutoNumbered();
    }


    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testGetAutoNumberStart() {
        column.getAutoNumberStart();
    }

    @Test(expected = DatabaseMetaDataProvider.UnexpectedDataTypeException.class)
    public void testGetDefaultValue() {
        column.getDefaultValue();
    }

    @Test
    public void testDefaultValue() {
        assertSame(column, column.defaultValue("Any String"));
    }

    @Test
    public void testNullable() {
        assertSame(column, column.nullable());
    }


    @Test
    public void testPrimaryKey() {
        assertSame(column, column.primaryKey());
    }

    @Test
    public void testNotPrimaryKey() {
        assertSame(column, column.notPrimaryKey());
    }


    @Test
    public void testAutoNumbered() {
        assertSame(column, column.autoNumbered(0));
    }

    @Test
    public void testDataType() {
        assertSame(column, column.dataType(DataType.BOOLEAN));
    }
}
