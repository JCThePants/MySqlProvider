package com.jcwhatever.nucleus.providers.mysql.data;

import org.bukkit.inventory.ItemStack;

/*
 * 
 */
public class ItemStacksDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "BUKKITITEMSTACKS";
    }

    @Override
    public Class<?> getDataClass() {
        return ItemStack[].class;
    }

    @Override
    public boolean isCompound() {
        return true;
    }
}

