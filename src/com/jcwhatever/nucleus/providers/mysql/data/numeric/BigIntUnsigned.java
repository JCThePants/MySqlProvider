package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class BigIntUnsigned extends AbstractDataType {

    @Override
    public String getName() {
        return "BIGINT UNSIGNED";
    }

    @Override
    public Class<?> getDataClass() {
        return long.class;
    }
}