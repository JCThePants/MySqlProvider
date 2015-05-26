package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Long Blob data type.
 */
public class LongBlob extends AbstractDataType {

    @Override
    public String getName() {
        return "LONGBLOB";
    }

    @Override
    public Class<?> getDataClass() {
        return byte[].class;
    }
}
