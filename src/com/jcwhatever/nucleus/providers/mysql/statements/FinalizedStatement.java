package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlQueryResult;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.utils.ArrayUtils;
import com.jcwhatever.nucleus.utils.PreCon;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;

/*
 * 
 */
public class FinalizedStatement implements ISqlStatement {

    private final Table _table;
    private final Connection _connection;
    private final String _sql;
    private final Object[] _values;
    private final StatementType _type;
    private final String[] _columns;
    private final boolean _isPrefixed;

    public FinalizedStatement(Connection connection, StatementType type) {

        PreCon.notNull(connection);
        PreCon.isValid(type == StatementType.TRANSACTION_START ||
                type == StatementType.TRANSACTION_COMMIT);

        _table = null;
        _connection = connection;
        _sql = null;
        _values = null;
        _type = type;
        _columns = null;
        _isPrefixed = true;
    }

    public FinalizedStatement(Table table,
                              String sql, Object[] values, String[] columns,
                              StatementType type, boolean isPrefixed) {

        this(table, table.getDatabase().getConnection(), sql, values, columns, type, isPrefixed);
    }

    public FinalizedStatement(Connection connection,
                              String sql, Object[] values, String[] columns,
                              StatementType type, boolean isPrefixed) {

        this(null, connection, sql, values, columns, type, isPrefixed);
    }

    private FinalizedStatement(@Nullable Table table, Connection connection,
                              String sql, Object[] values, String[] columns,
                               StatementType type, boolean isPrefixed) {

        PreCon.notNull(connection);
        PreCon.notNull(sql);
        PreCon.notNull(values);
        PreCon.notNull(columns);
        PreCon.notNull(type);
        PreCon.isValid(type != StatementType.TRANSACTION_START &&
                type != StatementType.TRANSACTION_COMMIT);

        _table = table;
        _connection = connection;
        _sql = sql;
        _values = values;
        _columns = columns;
        _type = type;
        _isPrefixed = isPrefixed;
    }

    @Nullable
    public Table getTable() {
        return _table;
    }

    public Connection getConnection() {
        return _connection;
    }

    public boolean isPrefixed() {
        return _isPrefixed;
    }

    public StatementType getType() {
        return _type;
    }

    public String[] getColumns() {
        if (_columns == null)
            return ArrayUtils.EMPTY_STRING_ARRAY;

        return _columns;
    }

    @Override
    public String getStatement() {
        return _sql;
    }

    @Override
    public Object[] getValues() {
        if (_values == null)
            return ArrayUtils.EMPTY_OBJECT_ARRAY;

        return _values;
    }

    public ISqlQueryResult executeQuery() throws SQLException {

        PreparedStatement prepared = prepareStatement();
        ResultSet resultSet = prepared.executeQuery();
        return new StatementResult(this, resultSet);
    }

    public void execute() throws SQLException {
        PreparedStatement prepared = prepareStatement();
        prepared.execute();
    }

    public PreparedStatement prepareStatement() throws SQLException {

        PreparedStatement statement = getConnection().prepareStatement(_sql);

        int count = 1;
        for (Object value : _values) {

            if (value instanceof String) {
                statement.setString(count, (String) value);
            } else if (value instanceof Boolean) {
                statement.setBoolean(count, (Boolean) value);
            } else if (value instanceof Byte) {
                statement.setByte(count, (Byte) value);
            } else if (value instanceof Short) {
                statement.setShort(count, (Short) value);
            } else if (value instanceof Integer) {
                statement.setInt(count, (Integer) value);
            } else if (value instanceof Long) {
                statement.setLong(count, (Long) value);
            } else if (value instanceof Float) {
                statement.setFloat(count, (Float) value);
            } else if (value instanceof Double) {
                statement.setDouble(count, (Double) value);
            } else if (value instanceof byte[]) {
                statement.setBytes(count, (byte[]) value);
            } else if (value instanceof UUID) {

                UUID uuid = (UUID) value;

                ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                statement.setBytes(count, buffer.array());
            } else if (value instanceof Date) {
                statement.setDate(count, new java.sql.Date(((Date) value).getTime()));
            }
            else {
                statement.setObject(count, value);
            }
            count++;
        }

        return statement;
    }

    @Override
    public String toString() {
        return _sql;
    }
}
