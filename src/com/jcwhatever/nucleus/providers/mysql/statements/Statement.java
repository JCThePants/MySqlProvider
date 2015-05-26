package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.utils.PreCon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Sql statement context buffer.
 *
 * <p>Used to construct multiple sql statements for a single context.</p>
 */
public class Statement {

    private final ISqlDatabase _database;
    private final StringBuilder _statement;
    private final List<Object> _values;
    private StatementType _type = StatementType.UPDATE;
    private List<FinalizedStatement> _list;
    private String[] _columns;
    private boolean _isPrefixed = true;

    /**
     * Constructor.
     *
     * @param database       The database the statements will be for.
     * @param statementSize  The initial statement builder buffer size.
     * @param valueSize      The initial capacity of the value buffer.
     */
    public Statement(ISqlDatabase database, int statementSize, int valueSize) {
        PreCon.notNull(database);
        PreCon.positiveNumber(statementSize);
        PreCon.positiveNumber(valueSize);

        _database = database;
        _statement = new StringBuilder(statementSize);
        _values = new ArrayList<>(valueSize);
    }

    /**
     * Get the current length of the current statement.
     */
    public int length() {
        return _statement.length();
    }

    /**
     * Get the column name prefix flag.
     */
    public boolean isPrefixed() {
        return _isPrefixed;
    }

    /**
     * Set the column name prefix flag.
     */
    public void setPrefixed(boolean isPrefixed) {
        _isPrefixed = isPrefixed;
    }

    /**
     * Get the current statement type.
     */
    public StatementType getType() {
        return _type;
    }

    /**
     * Set the current statement type.
     */
    public void setType(StatementType type) {
        _type = type;
    }

    /**
     * Get the current statement affected column names.
     */
    public String[] getColumns() {
        if (_columns == null)
            return new String[0];

        return _columns;
    }

    /**
     * Set the current statement affected columns.
     *
     * @param columns  The column names.
     */
    public void setColumns(String[] columns) {
        _columns = columns;
    }

    /**
     * Get the sql statement buffer.
     */
    public StringBuilder getBuffer() {
        return _statement;
    }

    /**
     * Get the values buffer.
     */
    public List<Object> getValues() {
        return _values;
    }

    /**
     * Get all finalized statements.
     */
    public FinalizedStatements getFinalized() {
        if (_list == null)
            return new FinalizedStatements(_database);

        return new FinalizedStatements(_database, _list);
    }

    /**
     * Prepare all finalized statements.
     *
     * @throws SQLException
     */
    public PreparedStatement[] prepareStatements() throws SQLException {
        if (_list == null)
            return new PreparedStatement[0];

        PreparedStatement[] result = new PreparedStatement[_list.size()];

        for (int i=0; i < result.length; i++) {
            result[i] = _list.get(i).prepareStatement();
        }

        return result;
    }

    /**
     * Finalize the current statement for the specified table and add
     * to the internal list of finalized statements.
     *
     * <p>Resets the {@link Statement} so the next statement can be constructed.</p>
     *
     * @param table  The table the statement is for.
     *
     * @return  The new {@link FinalizedStatement} or null if the buffer is empty.
     */
    @Nullable
    public FinalizedStatement finalizeStatement(Table table) {
        PreCon.notNull(table);

        FinalizedStatement result = null;

        if (_statement.length() != 0) {
            initList();
            result = new FinalizedStatement(
                    table, _statement.toString(), _values.toArray(),
                    getColumns(), _type, _isPrefixed);
            _list.add(result);
        }
        reset();
        return result;
    }

    /**
     * Finalize the current statement for the specified connection and add
     * to the internal list of finalized statements.
     *
     * <p>Resets the {@link Statement} so the next statement can be constructed.</p>
     *
     * @param connection  The connection the statement is for.
     */
    public void finalizeStatement(Connection connection) {
        if (_statement.length() != 0) {
            initList();
            _list.add(new FinalizedStatement(
                    connection, _statement.toString(), _values.toArray(),
                    getColumns(), _type, _isPrefixed));
        }
        reset();
    }

    /**
     * Add a "begin transaction" statement to the internal list of finalized statements.
     *
     * @param connection  The connection the statement is for.
     */
    public void startTransaction(Connection connection) {
        finalizeStatement(connection);
        initList();
        _list.add(new FinalizedStatement(connection, StatementType.TRANSACTION_START));
    }

    /**
     * Adda "commit transaction" statement to the internal list of finalized statements.
     *
     * @param connection  The connection the statement is for.
     */
    public void commitTransaction(Connection connection) {
        finalizeStatement(connection);
        initList();
        _list.add(new FinalizedStatement(connection, StatementType.TRANSACTION_COMMIT));
    }

    @Override
    public String toString() {
        return _statement.toString();
    }

    private void reset() {
        _statement.setLength(0);
        _values.clear();
        _columns = null;
        _isPrefixed = true;
    }

    private void initList() {
        if (_list == null)
            _list = new ArrayList<>(4);
    }
}
