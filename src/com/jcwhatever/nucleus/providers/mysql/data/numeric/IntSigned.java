package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

/*
 * 
 */
public class IntSigned extends AbstractDataType {

    @Override
    public String getName() {
        return "INT";
    }

    @Override
    public Class<?> getDataClass() {
        return int.class;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
