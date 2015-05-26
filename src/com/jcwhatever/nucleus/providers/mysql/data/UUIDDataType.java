package com.jcwhatever.nucleus.providers.mysql.data;

import java.util.UUID;

/*
 * 
 */
public class UUIDDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "BINARY";
    }

    @Override
    public int size() {
        return 16;
    }

    @Override
    public Class<?> getDataClass() {
        return UUID.class;
    }
}

