package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * 24-bit unsigned integer.
 */
public class MediumIntUnsigned extends AbstractDataType {

    @Override
    public String getName() {
        return "MEDIUMINT UNSIGNED";
    }

    @Override
    public Class<?> getDataClass() {
        return int.class;
    }
}
