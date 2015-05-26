package com.jcwhatever.nucleus.providers.mysql.data;

import java.util.Date;

/*
 * 
 */
public class TimeStampDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "TIMESTAMP";
    }

    @Override
    public Class<?> getDataClass() {
        return Date.class;
    }
}
