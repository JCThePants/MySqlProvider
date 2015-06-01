package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.managed.items.serializer.InvalidItemStackStringException;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.SqlDbType;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.items.ItemStackUtils;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultSubscriber;
import com.jcwhatever.nucleus.utils.observer.future.Result;
import org.bukkit.inventory.ItemStack;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link ICompoundDataHandler} for {@link ItemStack}.
 */
public class ItemStackHandler implements ICompoundDataHandler {

    private static final String TABLE_NAME = "Nucleus_Items";
    private static final String[] COLUMN_NAMES = new String[] {
            "serialized"
    };

    private ISqlTable _table;
    private boolean _isLoaded;

    /**
     * Constructor.
     *
     * @param database  The owning database.
     */
    public ItemStackHandler(ISqlDatabase database) {

        ISqlTableDefinition definition =
                database.createTableBuilder().usageReadInsertUpdate().transactional()
                        .column("id", SqlDbType.LONG_UNSIGNED).primary().autoIncrement()
                        .column("serialized", SqlDbType.getString(20000))
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

        String serialized = resultSet.getString(alias + ".serialized");

        try {

            @SuppressWarnings("unchecked")
            T result = (T)ItemStackUtils.parse(serialized);

            return result;

        } catch (InvalidItemStackStringException e) {
            e.printStackTrace();
            throw new SQLException(e);
        }
    }

    @Override
    public ICompoundDataIterator dataIterator(Object value) {
        PreCon.isValid(value instanceof ItemStack || value instanceof ItemStack[]);

        final String serialized = value instanceof ItemStack
                ? ItemStackUtils.serialize((ItemStack)value)
                : ItemStackUtils.serialize((ItemStack[])value);

        return new ICompoundDataIterator() {

            int index = -1;

            @Override
            public boolean next() {
                index++;
                return index < 1;
            }

            @Override
            public String getColumnName() {
                return COLUMN_NAMES[index];
            }

            @Override
            public Object getValue() {

                switch (index) {
                    case 0:
                        return serialized;
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