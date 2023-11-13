package org.alfasoftware.morf.guicesupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.upgrade.DatabaseUpgradePathValidationService;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeBuilder.GraphBasedUpgradeBuilderFactory;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradeStatusTableService;
import org.alfasoftware.morf.upgrade.ViewChangesDeploymentHelper;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link MorfModule}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestMorfModule {

  @Mock ConnectionResources connectionResources;
  @Mock UpgradePathFactory factory;
  @Mock UpgradeStatusTableService upgradeStatusTableService;
  @Mock ViewChangesDeploymentHelper viewChangesDeploymentHelper;
  @Mock ViewDeploymentValidator viewDeploymentValidator;
  @Mock GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory;
  @Mock
  DatabaseUpgradePathValidationService databaseUpgradePathValidationService;

  private MorfModule module;


  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    module = new MorfModule();
  }


  /**
   * Test that the module can provide an instance of {@link Upgrade}.
   */
  @Test
  public void testProvideUpgrade() {
    Upgrade upgrade = module.provideUpgrade(connectionResources, factory, upgradeStatusTableService,
      viewChangesDeploymentHelper, viewDeploymentValidator, graphBasedUpgradeBuilderFactory, databaseUpgradePathValidationService);

    assertNotNull("Instance of Upgrade should not be null", upgrade);
    assertThat("Instance of Upgrade", upgrade, IsInstanceOf.instanceOf(Upgrade.class));
  }
}

