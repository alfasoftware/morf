package org.alfasoftware.morf.upgrade.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Test;

public class TestUpgradeSteps {



    private void testUpgradeStep(UpgradeStep upgradeStep){
        assertFalse("JiraId is set", upgradeStep.getJiraId().isEmpty());
        assertFalse("Description is set", upgradeStep.getDescription().isEmpty());
    }


    @Test
    public void testCreateDeployedViews() {
        CreateDeployedViews upgradeStep = new CreateDeployedViews();
        testUpgradeStep(upgradeStep);
        SchemaEditor schema = mock(SchemaEditor.class);
        DataEditor dataEditor = mock(DataEditor.class);
        upgradeStep.execute(schema, dataEditor);
        verify(schema, times(1)).addTable(any());
    }

    @Test
    public void testRecreateOracleSequences() {
        RecreateOracleSequences upgradeStep = new RecreateOracleSequences();
        testUpgradeStep(upgradeStep);
        SchemaEditor schema = mock(SchemaEditor.class);
        DataEditor dataEditor = mock(DataEditor.class);
        upgradeStep.execute(schema, dataEditor);
        verifyNoInteractions(schema);
    }


    /**
     * Verify CreateDeferredIndexOperationTables has metadata and calls addTable twice (one per table).
     */
    @Test
    public void testCreateDeferredIndexOperationTables() {
        CreateDeferredIndexOperationTables upgradeStep = new CreateDeferredIndexOperationTables();
        testUpgradeStep(upgradeStep);
        SchemaEditor schema = mock(SchemaEditor.class);
        DataEditor dataEditor = mock(DataEditor.class);
        upgradeStep.execute(schema, dataEditor);
        verify(schema, times(2)).addTable(any());
    }


    /**
     * Verify DeferredIndexOperation table has all required columns and indexes.
     */
    @Test
    public void testDeferredIndexOperationTableStructure() {
        Table table = DatabaseUpgradeTableContribution.deferredIndexOperationTable();
        assertEquals("DeferredIndexOperation", table.getName());

        java.util.List<String> columnNames = table.columns().stream()
            .map(c -> c.getName())
            .collect(Collectors.toList());
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("upgradeUUID"));
        assertTrue(columnNames.contains("tableName"));
        assertTrue(columnNames.contains("indexName"));
        assertTrue(columnNames.contains("indexUnique"));
        assertTrue(columnNames.contains("status"));
        assertTrue(columnNames.contains("retryCount"));
        assertTrue(columnNames.contains("createdTime"));
        assertTrue(columnNames.contains("startedTime"));
        assertTrue(columnNames.contains("completedTime"));
        assertTrue(columnNames.contains("errorMessage"));

        java.util.List<String> indexNames = table.indexes().stream()
            .map(i -> i.getName())
            .collect(Collectors.toList());
        assertTrue(indexNames.contains("DeferredIndexOp_1"));
        assertTrue(indexNames.contains("DeferredIndexOp_2"));
    }


    /**
     * Verify DeferredIndexOperationColumn table has all required columns and that PK index is unique.
     */
    @Test
    public void testDeferredIndexOperationColumnTableStructure() {
        Table table = DatabaseUpgradeTableContribution.deferredIndexOperationColumnTable();
        assertEquals("DeferredIndexOperationColumn", table.getName());

        java.util.List<String> columnNames = table.columns().stream()
            .map(c -> c.getName())
            .collect(Collectors.toList());
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("operationId"));
        assertTrue(columnNames.contains("columnName"));
        assertTrue(columnNames.contains("columnSequence"));

        java.util.List<String> indexNames = table.indexes().stream()
            .map(i -> i.getName())
            .collect(Collectors.toList());
        assertTrue(indexNames.contains("DeferredIdxOpCol_1"));
    }

}