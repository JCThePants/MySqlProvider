package com.jcwhatever.nucleus.providers.mysql.data;

import java.util.UUID;

/**
 * UUID data type
 *
 * <p>16 byte binary data.</p>
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

