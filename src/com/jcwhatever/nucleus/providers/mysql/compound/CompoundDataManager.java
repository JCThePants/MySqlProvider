package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.utils.PreCon;

import org.bukkit.Location;
import org.bukkit.inventory.ItemStack;
import org.bukkit.util.Vector;

import javax.annotation.Nullable;

/*
 * 
 */
public class CompoundDataManager {

    private final ISqlDatabase _database;
    private final LocationHandler _locationHandler;
    private final VectorHandler _vectorHandler;
    private final ItemStackHandler _itemStackHandler;

    public CompoundDataManager(ISqlDatabase database) {
        PreCon.notNull(database);

        _database = database;
        _locationHandler = new LocationHandler(database);
        _vectorHandler = new VectorHandler(database);
        _itemStackHandler = new ItemStackHandler(database);
    }

    @Nullable
    public ICompoundDataHandler getHandler(ISqlDbType dataType) {
        PreCon.notNull(dataType);

        if (dataType.getDataClass() == Location.class)
            return _locationHandler;

        if (dataType.getDataClass() == Vector.class)
            return _vectorHandler;

        if (dataType.getDataClass() == ItemStack.class ||
                dataType.getDataClass() == ItemStack[].class) {
            return _vectorHandler;
        }

        return null;
    }

    public LocationHandler getLocationHandler() {
        return _locationHandler;
    }

    public VectorHandler getVectorHandler() {
        return _vectorHandler;
    }

    public ItemStackHandler getItemStackHandler() {
        return _itemStackHandler;
    }
}
