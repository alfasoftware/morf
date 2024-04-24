package org.alfasoftware.morf.util;

/**
 * Util class to help validate Schema details
 */
public final class SchemaValidatorUtil {

    private static final String SCHEMA_NAME_VALIDATION_PATTERN = "^[A-Za-z0-9_]*$";

    private SchemaValidatorUtil() {}

    /**
     * throws a IllegalArgumentException when schemaName contains illegal chars.
     * @param schemaName - String value of schemaName.
     * @return - a string containing schemaName.
     * @throws IllegalArgumentException - when the schemaName contains invalid chars.
     */
    public static String validateSchemaName(String schemaName) {
        if(schemaName != null && !schemaName.matches(SCHEMA_NAME_VALIDATION_PATTERN)) {
            throw new IllegalArgumentException("schemaName [" + schemaName + "] failed validation check.");
        }
        return schemaName;
    }

}
