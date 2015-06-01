package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.SqlDbType;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.coords.SyncLocation;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultSubscriber;
import com.jcwhatever.nucleus.utils.observer.future.Result;
import org.bukkit.Location;
import org.bukkit.World;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link ICompoundDataHandler} for {@link Location}.
 */
public class LocationHandler implements ICompoundDataHandler {

    private static final String TABLE_NAME = "Nucleus_Locations";
    private static final String[] COLUMN_NAMES = new String[] {
            "world",
            "x", "y", "z",
            "yaw", "pitch"
    };

    private ISqlTable _table;
    private boolean _isLoaded;

    /**
     * Constructor.
     *
     * @param database  The owning database.
     */
    public LocationHandler(ISqlDatabase database) {

        ISqlTableDefinition definition =
                database.createTableBuilder().usageReadInsertUpdate().transactional()
                .column("id", SqlDbType.LONG_UNSIGNED).primary().autoIncrement()
                .column("world", SqlDbType.getString(45)).nullable()
                .column("x", SqlDbType.DOUBLE)
                .column("y", SqlDbType.DOUBLE)
                .column("z", SqlDbType.DOUBLE)
                .column("yaw", SqlDbType.FLOAT)
                .column("pitch", SqlDbType.FLOAT)
                .define();

        database.createTable(TABLE_NAME, definition)
                .onSuccess(new FutureResultSubscriber<ISqlTable>() {
                    @Override
                    public void on(Result<ISqlTable> result) {
                        _table = result.getResult();
                        _isLoaded = true;
                    }
                });
    }

    @Override
    public boolean isLoaded() {
        return _isLoaded;
    }

    @Override
    public ISqlTable getTable() {
        if (_table == null)
            throw new IllegalStateException("The database table Nucleus_Locations " +
                    "was not found or is not ready yet.");
        return _table;
    }

    @Nullable
    @Override
    public <T> T getDataFromRow(String alias, ResultSet resultSet) throws SQLException {

        String worldName = resultSet.getString(alias + ".world");
        double x = resultSet.getDouble(alias + ".x");
        double y = resultSet.getDouble(alias + ".y");
        double z = resultSet.getDouble(alias + ".z");
        float yaw = resultSet.getFloat(alias + ".yaw");
        float pitch = resultSet.getFloat(alias + ".pitch");

        @SuppressWarnings("unchecked")
        T result = (T)new SyncLocation(worldName, x, y, z, yaw, pitch);

        return result;
    }

    @Override
    public ICompoundDataIterator dataIterator(Object value) {
        PreCon.isValid(value instanceof Location);

        final Location location = (Location)value;

        return new ICompoundDataIterator() {

            int index = -1;

            @Override
            public boolean next() {
                index++;
                return index < 6;
            }

            @Override
            public String getColumnName() {
                return COLUMN_NAMES[index];
            }

            @Override
            public Object getValue() {

                switch (index) {
                    case 0:
                        if (location instanceof SyncLocation)
                            return ((SyncLocation) location).getWorldName();

                        World world = location.getWorld();
                        return world == null ? null : world.getName();

                    case 1:
                        return location.getX();
                    case 2:
                        return location.getY();
                    case 3:
                        return location.getZ();
                    case 4:
                        return location.getYaw();
                    case 5:
                        return location.getPitch();
                    default:
                        throw new NoSuchElementException();
                }
            }

            @Override
            public int currentIndex() {
                return index;
            }
        };
    }
}
