package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Tiny Blob data type.
 */
public class TinyBlob extends AbstractDataType {

    @Override
    public String getName() {
        return "TINYBLOB";
    }

    @Override
    public Class<?> getDataClass() {
        return byte[].class;
    }
}
