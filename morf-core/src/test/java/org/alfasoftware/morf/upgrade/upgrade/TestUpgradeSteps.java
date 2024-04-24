package org.alfasoftware.morf.upgrade.upgrade;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.junit.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

}