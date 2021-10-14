package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.metadata.DataType;
import org.apache.commons.codec.binary.Hex;

/**
 * Binary data literal.
 *
 * <p>Note: {@link BlobFieldLiteral#getValue()} returns the binary data as hex-encoded string.</p>
 */
public class BlobFieldLiteral extends FieldLiteral {


    public BlobFieldLiteral(byte[] bytes) {
        super(convertToHex(bytes), DataType.BLOB);
    }


    private static String convertToHex(byte[] bytes) {
      return Hex.encodeHexString(bytes, false);
    }
}
