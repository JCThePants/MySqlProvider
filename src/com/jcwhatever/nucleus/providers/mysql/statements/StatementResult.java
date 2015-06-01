package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.compound.ItemStackHandler;
import com.jcwhatever.nucleus.providers.mysql.compound.LocationHandler;
import com.jcwhatever.nucleus.providers.mysql.compound.VectorHandler;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlQueryResult;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.coords.SyncLocation;
import com.jcwhatever.nucleus.utils.text.TextUtils;
import org.bukkit.inventory.ItemStack;
import org.bukkit.util.Vector;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of {@link ISqlQueryResult}.
 */
public class StatementResult implements ISqlQueryResult {

    private final FinalizedStatement _statement;
    private final Table _table;
    private final ResultSet _result;
    private final String[] _columns;
    private boolean _hasCalledNext;

    /**
     * Constructor.
     *
     * @param statement  The finalized statement the result is for.
     * @param result     The result set.
     */
    public StatementResult(FinalizedStatement statement, ResultSet result) {
        PreCon.notNull(statement);
        PreCon.notNull(result);

        _statement = statement;
        _table = statement.getTable();
        _columns = statement.getColumns();
        _result = result;
    }

    /**
     * Get the finalized statement the result is from.
     */
    public FinalizedStatement getFinalizedStatement() {
        return _statement;
    }

    @Override
    public String[] getColumns() {
        return _statement.getColumns();
    }

    @Override
    public UUID getUUID(int columnIndex) throws SQLException {
        assertNextInvoked();

        String columnName = _columns[columnIndex];
        return getUUID(columnName);
    }

    @Nullable
    @Override
    public UUID getUUID(String columnName) throws SQLException {
        assertNextInvoked();

        columnName = getName(columnName);

        byte[] bytes = getBytes(columnName);
        if (bytes == null)
            return null;

        if (bytes.length != 16) {
            throw new SQLException("Failed to read UUID from database. " +
                    "Incorrect data length (" + bytes.length + ')');
        }

        long mostSignificant =
                        ((bytes[0] & 0xFFL) << 56)
                        + ((bytes[1] & 0xFFL) << 48)
                        + ((bytes[2] & 0xFFL) << 40)
                        + ((bytes[3] & 0xFFL) << 32)
                        + ((bytes[4] & 0xFFL) << 24)
                        + ((bytes[5] & 0xFFL) << 16)
                        + ((bytes[6] & 0xFFL) << 8)
                        + (bytes[7] & 0xFFL);

        long leastSignificant =
                        ((bytes[8] & 0xFFL) << 56)
                        + ((bytes[9] & 0xFFL) << 48)
                        + ((bytes[10] & 0xFFL) << 40)
                        + ((bytes[11] & 0xFFL) << 32)
                        + ((bytes[12] & 0xFFL) << 24)
                        + ((bytes[13] & 0xFFL) << 16)
                        + ((bytes[14] & 0xFFL) << 8)
                        + (bytes[15] & 0xFFL);

        return new UUID(mostSignificant, leastSignificant);
    }

    @Override
    public SyncLocation getLocation(int columnIndex) throws SQLException {
        assertNextInvoked();

        String columnName = _columns[columnIndex];
        return getLocation(columnName);
    }

    @Override
    public SyncLocation getLocation(String columnName) throws SQLException {
        assertNextInvoked();

        LocationHandler handler = _table.getDatabase().getCompoundManager().getLocationHandler();

        String[] columnComponents = TextUtils.PATTERN_DOT.split(columnName);

        return handler.getDataFromRow(
                "Nucleus_Locations_" + columnComponents[columnComponents.length - 1], _result);
    }

    @Override
    public Vector getVector(int columnIndex) throws SQLException {
        assertNextInvoked();

        String columnName = _columns[columnIndex];
        return getVector(columnName);
    }

    @Override
    public Vector getVector(String columnName) throws SQLException {
        assertNextInvoked();

        VectorHandler handler = _table.getDatabase().getCompoundManager().getVectorHandler();

        String[] columnComponents = TextUtils.PATTERN_DOT.split(columnName);

        return handler.getDataFromRow(
                "Nucleus_Vectors_" + columnComponents[columnComponents.length - 1], _result);
    }

    @Override
    public ItemStack[] getItemStacks(int columnIndex) throws SQLException {
        assertNextInvoked();

        String columnName = _columns[columnIndex];
        return getItemStacks(columnName);
    }

    @Override
    public ItemStack[] getItemStacks(String columnName) throws SQLException {
        assertNextInvoked();

        ItemStackHandler handler = _table.getDatabase().getCompoundManager().getItemStackHandler();

        String[] columnComponents = TextUtils.PATTERN_DOT.split(columnName);

        return handler.getDataFromRow(
                "Nucleus_Items_" + columnComponents[columnComponents.length - 1], _result);
    }

    @Override
    public boolean next() throws SQLException {
        _hasCalledNext = true;
        return _result.next();
    }

    @Override
    public void close() throws SQLException {
        _result.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        assertNextInvoked();

        return _result.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getDouble(columnIndex);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        assertNextInvoked();

        return _result.getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getAsciiStream(columnIndex);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getDouble(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getAsciiStream(columnLabel);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBinaryStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return _result.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        _result.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return _result.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return _result.getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getObject(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return _result.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return _result.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return _result.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return _result.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        _result.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        _result.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        return _result.first();
    }

    @Override
    public boolean last() throws SQLException {
        return _result.last();
    }

    @Override
    public int getRow() throws SQLException {
        assertNextInvoked();

        return _result.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return _result.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return _result.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return _result.previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        _result.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return _result.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        _result.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return _result.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return _result.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return _result.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return _result.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return _result.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return _result.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        _result.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        _result.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        _result.updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        _result.updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        _result.updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        _result.updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        _result.updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        _result.updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        _result.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        _result.updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        _result.updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        _result.updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        _result.updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        _result.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        _result.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        _result.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        _result.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        _result.updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        _result.updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNull(columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateByte(columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateShort(columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateLong(columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateFloat(columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateDouble(columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateString(columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBytes(columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateDate(columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateTimestamp(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateObject(columnLabel, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateObject(columnLabel, x);
    }

    @Override
    public void insertRow() throws SQLException {
        _result.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        _result.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        _result.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        _result.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        _result.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        _result.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        _result.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return _result.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return _result.getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return _result.getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return _result.getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return _result.getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return _result.getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        assertNextInvoked();

        return _result.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        assertNextInvoked();

        return _result.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        assertNextInvoked();

        return _result.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getURL(columnLabel);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        _result.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateRef(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        _result.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBlob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        _result.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateClob(columnLabel, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        _result.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateArray(columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return _result.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        columnLabel = getName(columnLabel);

        return _result.getRowId(columnLabel);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        _result.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateRowId(columnLabel, x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return _result.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return _result.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        _result.updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        _result.updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNClob(columnLabel, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return _result.getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        columnLabel = getName(columnLabel);

        return _result.getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return _result.getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        columnLabel = getName(columnLabel);

        return _result.getSQLXML(columnLabel);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        _result.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        assertNextInvoked();

        return _result.getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        return _result.getNCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        assertNextInvoked();

        _result.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        assertNextInvoked();

        columnLabel = getName(columnLabel);

        _result.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        _result.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        _result.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        _result.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        _result.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        _result.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        _result.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        _result.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        _result.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        _result.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        _result.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        _result.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        _result.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        _result.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        columnLabel = getName(columnLabel);

        _result.updateNClob(columnLabel, reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return _result.getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        columnLabel = getName(columnLabel);

        return _result.getObject(columnLabel, type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _result.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _result.isWrapperFor(iface);
    }

    private String getName(String columnName) {
        if (_statement.isPrefixed() && columnName.indexOf('.') == -1)
            return _table.getName() + '.' + columnName;

        return columnName;
    }

    private void assertNextInvoked() throws SQLException {
        if (!_hasCalledNext)
            throw new SQLException("'next' method has not been invoked yet.");
    }
}
