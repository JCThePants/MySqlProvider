package com.jcwhatever.nucleus.providers.mysql.data;

import java.util.Vector;

/*
 * 
 */
public class VectorDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "BUKKITVECTOR";
    }

    @Override
    public Class<?> getDataClass() {
        return Vector.class;
    }

    @Override
    public boolean isCompound() {
        return true;
    }
}

