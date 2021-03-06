package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 16-bit signed integer.
 */
public class SmallIntSigned extends AbstractDataType {

    @Override
    public String getName() {
        return "SMALLINT";
    }

    @Override
    public Class<?> getDataClass() {
        return short.class;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
