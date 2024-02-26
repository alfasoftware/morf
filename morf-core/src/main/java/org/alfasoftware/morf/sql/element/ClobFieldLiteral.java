package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.metadata.DataType;

/**
 * Clob field literal.
 */
public class ClobFieldLiteral extends FieldLiteral {

    public ClobFieldLiteral(String string) {
        super(string, DataType.CLOB);
    }
}
