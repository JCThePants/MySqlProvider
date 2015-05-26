package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
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
