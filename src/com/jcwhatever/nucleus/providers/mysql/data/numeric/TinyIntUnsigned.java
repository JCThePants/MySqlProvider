package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 8-bit unsigned integer.
 */
public class TinyIntUnsigned extends AbstractDataType {

    @Override
    public String getName() {
        return "TINYINT UNSIGNED";
    }

    @Override
    public Class<?> getDataClass() {
        return short.class;
    }
}