package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.mixins.ILoadable;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.utils.PreCon;

import org.bukkit.Location;
import org.bukkit.inventory.ItemStack;
import org.bukkit.util.Vector;

import javax.annotation.Nullable;

/**
 * Manages compound data handlers.
 */
public class CompoundDataManager implements ILoadable {

    private final ISqlDatabase _database;
    private final LocationHandler _locationHandler;
    private final VectorHandler _vectorHandler;
    private final ItemStackHandler _itemStackHandler;

    /**
     * Constructor.
     *
     * @param database  The database the manager is for.
     */
    public CompoundDataManager(ISqlDatabase database) {
        PreCon.notNull(database);

        _database = database;
        _locationHandler = new LocationHandler(database);
        _vectorHandler = new VectorHandler(database);
        _itemStackHandler = new ItemStackHandler(database);
    }

    @Override
    public boolean isLoaded() {
        return _locationHandler.isLoaded()
                && _vectorHandler.isLoaded()
                && _itemStackHandler.isLoaded();
    }

    /**
     * Get the data handler for the specified data type.
     *
     * @param dataType  The data type.
     *
     * @return  The data handler or null if one could not be found for the
     * data type.
     */
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

    /**
     * Get the {@link Location} data handler.
     */
    public LocationHandler getLocationHandler() {
        return _locationHandler;
    }

    /**
     * Get the {@link Vector} data handler.
     */
    public VectorHandler getVectorHandler() {
        return _vectorHandler;
    }

    /**
     * Get the {@link ItemStack} data handler.
     */
    public ItemStackHandler getItemStackHandler() {
        return _itemStackHandler;
    }
}
