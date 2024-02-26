package org.alfasoftware.morf.upgrade;

import junit.framework.TestCase;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.View;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.*;

public class TestViewDeploymentValidator {

    ViewDeploymentValidator viewDeploymentValidator;
    View testView;
    UpgradeSchemas upgradeSchemas;

    ViewDeploymentValidator.Factory.AlwaysValidateFactory alwaysValidateFactory = new ViewDeploymentValidator.Factory.AlwaysValidateFactory();

    @Mock
    ConnectionResources connectionResources;

    @Before
    public void setup() {
        viewDeploymentValidator = alwaysValidateFactory.createViewDeploymentValidator(connectionResources);
        upgradeSchemas = new UpgradeSchemas(schema(), schema());
        testView      = view("FooView", select(field("name")).from(tableRef("Foo")));
    }

    @Test
    public void testValidateExistingView() {
        Assert.assertTrue(viewDeploymentValidator.validateExistingView(testView, upgradeSchemas));
    }

    @Test
    public void testValidateMissingView() {
        Assert.assertTrue(viewDeploymentValidator.validateMissingView(testView, upgradeSchemas));
    }

    @Test
    public void testDeprecatedValidateExistingView() {
        Assert.assertTrue(viewDeploymentValidator.validateExistingView(testView));
    }

    @Test
    public void testDeprecatedValidateMissingView() {
        Assert.assertTrue(viewDeploymentValidator.validateMissingView(testView));
    }
}