package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Blob data type.
 */
public class Blob extends AbstractDataType {

    @Override
    public String getName() {
        return "BLOB";
    }

    @Override
    public Class<?> getDataClass() {
        return byte[].class;
    }
}

