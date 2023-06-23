package org.alfasoftware.morf.upgrade;

import junit.framework.TestCase;
import org.alfasoftware.morf.metadata.View;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.*;

public class TestViewDeploymentValidator {

    ViewDeploymentValidator viewDeploymentValidator;
    View testView;

    @Before
    public void setup() {
        viewDeploymentValidator = new ViewDeploymentValidator.AlwaysValidate();
        testView      = view("FooView", select(field("name")).from(tableRef("Foo")));
    }

    @Test
    public void testValidateExistingView() {
        Assert.assertTrue(viewDeploymentValidator.validateExistingView(testView));
    }

    @Test
    public void testValidateMissingView() {
        Assert.assertTrue(viewDeploymentValidator.validateMissingView(testView));
    }

}