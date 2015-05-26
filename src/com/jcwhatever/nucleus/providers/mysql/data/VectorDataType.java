package com.jcwhatever.nucleus.providers.mysql.data;

import org.bukkit.util.Vector;

/**
 * Compound data type for {@link Vector}
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

