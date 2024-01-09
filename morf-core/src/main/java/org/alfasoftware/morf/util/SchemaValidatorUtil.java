package org.alfasoftware.morf.util;

/**
 * Util class to help validate Schema details
 */
public class SchemaValidatorUtil {

    private static final String pattern = "^[A-Za-z0-9_]*$";

    /**
     * throws a IllegalArgumentException when schemaName contains illegal chars.
     * @param schemaName - String value of schemaName.
     * @return - a string containing schemaName.
     * @throws IllegalArgumentException - when the schemaName contains invalid chars.
     */
    public static String validateSchemaName(String schemaName) {
        if(!schemaName.matches(pattern)) {
            throw new IllegalArgumentException("schemaName has failed validation check.");
        };
        return schemaName;
    }

}
