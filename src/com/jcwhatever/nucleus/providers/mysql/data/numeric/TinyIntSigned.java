package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 8-bit signed integer.
 */
public class TinyIntSigned extends AbstractDataType {

    @Override
    public String getName() {
        return "TINYINT";
    }

    @Override
    public Class<?> getDataClass() {
        return byte.class;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}