package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Boolean data type.
 */
public class Bit extends AbstractDataType {

    @Override
    public String getName() {
        return "BIT";
    }

    @Override
    public Class<?> getDataClass() {
        return boolean.class;
    }
}
