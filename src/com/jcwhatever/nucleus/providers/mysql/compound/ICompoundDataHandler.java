package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nullable;

/*
 * 
 */
public interface ICompoundDataHandler {

    ISqlTable getTable();

    @Nullable
    <T> T getDataFromRow(String alias, ResultSet resultSet) throws SQLException;

    ICompoundDataIterator dataIterator(Object value);
}
