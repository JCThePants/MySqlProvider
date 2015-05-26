package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class Text extends AbstractDataType {

    @Override
    public String getName() {
        return "TEXT";
    }

    @Override
    public Class<?> getDataClass() {
        return String.class;
    }
}

