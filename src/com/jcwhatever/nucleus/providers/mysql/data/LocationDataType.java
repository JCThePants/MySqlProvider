package com.jcwhatever.nucleus.providers.mysql.data;

import org.bukkit.Location;

/**
 * Compound data type for {@link Location}.
 */
public class LocationDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "BUKKITLOCATION";
    }

    @Override
    public Class<?> getDataClass() {
        return Location.class;
    }

    @Override
    public boolean isCompound() {
        return true;
    }
}

