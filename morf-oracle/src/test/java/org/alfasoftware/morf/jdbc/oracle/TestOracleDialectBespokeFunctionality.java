package org.alfasoftware.morf.jdbc.oracle;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.alfasoftware.morf.sql.SqlUtils.*;
import static org.junit.Assert.assertEquals;

public class TestOracleDialectBespokeFunctionality {



    @Test
    public void testConvertStatementToSQL() {
        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3),
                        column("nullField", DataType.STRING, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select(field("someField"), field("otherField"), nullLiteral()).from(tableRef("OtherTable"));

        OracleDialect oracleDialect = new OracleDialect("TEST");
        String result = oracleDialect.convertStatementToSQL(selectStatement, table);

        String expectedResult = "SELECT " +
                "CAST(someField AS NVARCHAR2(3)) AS someField, " +
                "CAST(otherField AS DECIMAL(3,0)) AS otherField, " +
                "CAST(null AS NVARCHAR2(3)) AS nullField " +
                "FROM TEST.OtherTable";
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDistinctStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select(field("someField"), field("otherField")).from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().distinct().build();

        String result = oracleDialect.convertStatementToSQL(distinctSelectStatement, table);

        String expectedResult = "SELECT DISTINCT " +
                "CAST(someField AS NVARCHAR2(3)) AS someField, " +
                "CAST(otherField AS DECIMAL(3,0)) AS otherField " +
                "FROM TEST.OtherTable";
        assertEquals(expectedResult, result);
    }


    @Test
    public void testAllStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().distinct().build();

        String result = oracleDialect.convertStatementToSQL(distinctSelectStatement, table);

        String expectedResult = "SELECT DISTINCT * FROM TEST.OtherTable";
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpdateStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().forUpdate().build();

        String result = oracleDialect.convertStatementToSQL(distinctSelectStatement, table);

        String expectedResult = "SELECT * FROM TEST.OtherTable FOR UPDATE";
        assertEquals(expectedResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateForDistinctStatement() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        SelectStatement selectStatement = select().from(tableRef("OtherTable"));
        SelectStatement distinctSelectStatement = selectStatement.shallowCopy().forUpdate().distinct().build();

        String result = oracleDialect.convertStatementToSQL(distinctSelectStatement, table);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExeptionIsThrown() {
        OracleDialect oracleDialect = new OracleDialect("TEST");

        Table table = table("SomeTable")
                .columns(
                        column("someField", DataType.STRING, 3).primaryKey(),
                        column("otherField", DataType.DECIMAL, 3),
                        column("nullField", DataType.STRING, 3)
                ).indexes(
                        index("SomeTable_1").columns("otherField")
                );

        oracleDialect.convertStatementToSQL(null, table);
    }


}
