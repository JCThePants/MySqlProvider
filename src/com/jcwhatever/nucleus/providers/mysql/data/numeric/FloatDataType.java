package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Float data type.
 */
public class FloatDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "FLOAT";
    }

    @Override
    public Class<?> getDataClass() {
        return float.class;
    }
}
