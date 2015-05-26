package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class MediumBlob extends AbstractDataType {

    @Override
    public String getName() {
        return "MEDIUMBLOB";
    }

    @Override
    public Class<?> getDataClass() {
        return byte[].class;
    }
}
