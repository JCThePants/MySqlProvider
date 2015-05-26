package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 64 bit signed integer.
 */
public class BigIntSigned extends AbstractDataType {

    @Override
    public String getName() {
        return "BIGINT";
    }

    @Override
    public Class<?> getDataClass() {
        return long.class;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
