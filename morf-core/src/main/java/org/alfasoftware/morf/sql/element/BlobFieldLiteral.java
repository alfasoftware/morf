package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.metadata.DataType;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Since we store binary data, we will store the values in hex format, since all DBs support inserting
 * blob data using hex values.
 * So {@link FieldLiteral#getValue()} will always return the hex encoded string.
 */
public class BlobFieldLiteral extends FieldLiteral {

    public BlobFieldLiteral(byte[] bytes) {
        super(new BigInteger(bytes).toString(16).toUpperCase(), DataType.BLOB);
    }

    public BlobFieldLiteral(String text) {
        this(text.getBytes(StandardCharsets.UTF_8));
    }
}
