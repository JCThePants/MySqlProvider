package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 24-bit signed integer.
 */
public class MediumIntSigned extends AbstractDataType {

    @Override
    public String getName() {
        return "MEDIUMINT";
    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public Class<?> getDataClass() {
        return int.class;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
