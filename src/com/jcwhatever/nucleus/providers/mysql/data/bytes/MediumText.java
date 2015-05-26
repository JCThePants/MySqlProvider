package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class MediumText extends AbstractDataType {

    @Override
    public String getName() {
        return "MEDIUMTEXT";
    }

    @Override
    public Class<?> getDataClass() {
        return String.class;
    }
}
