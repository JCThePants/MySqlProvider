package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * Interface for a compound data handler.
 */
public interface ICompoundDataHandler {

    /**
     * Get the table the handler is for.
     */
    ISqlTable getTable();

    /**
     * Create a new instance of the handlers data type using data from the
     * current row of the specified result set.
     *
     * @param alias      The column prefix.
     * @param resultSet  The result set.
     *
     * @param <T>  The data type.
     *
     * @return  The data type or null if failed to instantiate from data.
     *
     * @throws SQLException
     */
    @Nullable
    <T> T getDataFromRow(String alias, ResultSet resultSet) throws SQLException;

    /**
     * Get compound iterator for a value.
     *
     * @param value  The value which must be an instance of the data type the
     *               handler is for.
     */
    ICompoundDataIterator dataIterator(Object value);
}
