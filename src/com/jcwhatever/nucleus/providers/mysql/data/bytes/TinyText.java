package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Tiny Text data type.
 */
public class TinyText extends AbstractDataType {

    @Override
    public String getName() {
        return "TINYTEXT";
    }

    @Override
    public Class<?> getDataClass() {
        return String.class;
    }
}

