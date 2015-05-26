package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/**
 * Long Text data type.
 */
public class LongText extends AbstractDataType {

    @Override
    public String getName() {
        return "LONGTEXT";
    }

    @Override
    public Class<?> getDataClass() {
        return String.class;
    }
}
