package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.metadata.DataType;
import org.apache.commons.codec.binary.Hex;

/**
 * Binary data literal.
 *
 * <p>Note: {@link ClobFieldLiteral#getValue()} returns the binary data as hex-encoded string.</p>
 */
public class ClobFieldLiteral extends FieldLiteral {

    public ClobFieldLiteral(String string) {
        super(string, DataType.CLOB);
    }
}
