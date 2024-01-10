package org.alfasoftware.morf.util;

import org.junit.Test;

import static org.alfasoftware.morf.util.SchemaValidatorUtil.validateSchemaName;
import static org.junit.Assert.*;

public class TestSchemaValidatorUtil {


    @Test
    public void testSchemaNameWithJustLettersIsValid(){
        String unvalidatedName = "SchemaName";
        String schemaName = validateSchemaName(unvalidatedName);
        assertEquals(schemaName, unvalidatedName);
    }

    @Test
    public void testSchemaNameWithUnderscoreIsValid(){
        String unvalidatedName = "Schema_Name";
        String schemaName = validateSchemaName(unvalidatedName);
        assertEquals(schemaName, unvalidatedName);
    }

    @Test
    public void testSchemaNameWithSpecialCharsIsNotValid(){
        String unvalidatedName = "S;chemaName";
         IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> validateSchemaName(unvalidatedName));
         assertTrue(e.getMessage().contains("schemaName [" + unvalidatedName + "] failed validation check."));
    }
}