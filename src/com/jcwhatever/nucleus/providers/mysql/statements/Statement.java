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

/*
 * 
 */
public class Statement {

    private final ISqlDatabase _database;
    private final StringBuilder _statement;
    private final List<Object> _values;
    private StatementType _type = StatementType.UPDATE;
    private List<FinalizedStatement> _list;
    private String[] _columns;
    private boolean _isPrefixed = true;

    public Statement(ISqlDatabase database, int statementSize, int valueSize) {
        this(database, statementSize, valueSize, null);
    }

    public Statement(ISqlDatabase database, int statementSize, int valueSize, @Nullable String[] columns) {
        _database = database;
        _statement = new StringBuilder(statementSize);
        _values = new ArrayList<>(valueSize);
        _columns = columns;
    }

    public int length() {
        return _statement.length();
    }

    public boolean isPrefixed() {
        return _isPrefixed;
    }

    public void setPrefixed(boolean isPrefixed) {
        _isPrefixed = isPrefixed;
    }

    public StatementType getType() {
        return _type;
    }

    public void setType(StatementType type) {
        _type = type;
    }

    public String[] getColumns() {
        if (_columns == null)
            return new String[0];

        return _columns;
    }

    public void setColumns(String[] columns) {
        _columns = columns;
    }

    public StringBuilder getBuffer() {
        return _statement;
    }

    public List<Object> getValues() {
        return _values;
    }

    public FinalizedStatements getFinalized() {
        if (_list == null)
            return new FinalizedStatements(_database);

        return new FinalizedStatements(_database, _list);
    }

    public PreparedStatement[] prepareStatements() throws SQLException {
        if (_list == null)
            return new PreparedStatement[0];

        PreparedStatement[] result = new PreparedStatement[_list.size()];

        for (int i=0; i < result.length; i++) {
            result[i] = _list.get(i).prepareStatement();
        }

        return result;
    }

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

    public void finalizeStatement(Connection connection) {
        if (_statement.length() != 0) {
            initList();
            _list.add(new FinalizedStatement(
                    connection, _statement.toString(), _values.toArray(),
                    getColumns(), _type, _isPrefixed));
        }
        reset();
    }

    public void startTransaction(Connection connection) {
        finalizeStatement(connection);
        initList();
        _list.add(new FinalizedStatement(connection, StatementType.TRANSACTION_START));
    }

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
