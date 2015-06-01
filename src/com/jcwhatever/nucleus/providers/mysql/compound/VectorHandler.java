package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.SqlDbType;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultSubscriber;
import com.jcwhatever.nucleus.utils.observer.future.Result;
import org.bukkit.Location;
import org.bukkit.util.Vector;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link Vector} for {@link ICompoundDataHandler}.
 */
public class VectorHandler implements ICompoundDataHandler {

    private static final String TABLE_NAME = "Nucleus_Vectors";
    private static final String[] COLUMN_NAMES = new String[] {
            "x", "y", "z"
    };

    private ISqlTable _table;
    private boolean _isLoaded;

    /**
     * Constructor.
     *
     * @param database  The owning database.
     */
    public VectorHandler(ISqlDatabase database) {

        ISqlTableDefinition definition =
                database.createTableBuilder().usageReadInsertUpdate().transactional()
                        .column("id", SqlDbType.LONG_UNSIGNED).primary().autoIncrement()
                        .column("x", SqlDbType.DOUBLE)
                        .column("y", SqlDbType.DOUBLE)
                        .column("z", SqlDbType.DOUBLE)
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

        double x = resultSet.getDouble(alias + ".x");
        double y = resultSet.getDouble(alias + ".y");
        double z = resultSet.getDouble(alias + ".z");

        @SuppressWarnings("unchecked")
        T result = (T)new Vector(x, y, z);

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
                return index < 3;
            }

            @Override
            public String getColumnName() {
                return COLUMN_NAMES[index];
            }

            @Override
            public Object getValue() {

                switch (index) {
                    case 0:
                        return location.getX();
                    case 1:
                        return location.getY();
                    case 2:
                        return location.getZ();
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

