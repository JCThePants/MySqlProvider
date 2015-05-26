package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class SmallIntUnsigned extends AbstractDataType {

    @Override
    public String getName() {
        return "SMALLINT";
    }

    @Override
    public Class<?> getDataClass() {
        return int.class;
    }
}