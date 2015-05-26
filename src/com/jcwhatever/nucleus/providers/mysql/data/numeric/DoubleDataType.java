package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class DoubleDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "DOUBLE";
    }

    @Override
    public Class<?> getDataClass() {
        return double.class;
    }
}
