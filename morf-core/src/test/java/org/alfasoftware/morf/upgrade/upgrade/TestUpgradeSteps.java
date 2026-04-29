/* Copyright 2026 Alfa Financial Software
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