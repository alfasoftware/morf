package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.metadata.DataType;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Since we store binary data, we will store the values in base64 format and do the appropriate conversion based on dialect to the actual value when saving in DB.
 * So {@link FieldLiteral#getValue()} will always return the base64 encoded string.
 */
public class BlobFieldLiteral extends FieldLiteral {

    public BlobFieldLiteral(byte[] bytes) {
        super("", Base64.getEncoder().encodeToString(bytes), DataType.BLOB);
    }

    public BlobFieldLiteral(String text) {
        this(text.getBytes(StandardCharsets.UTF_8));
    }
}
