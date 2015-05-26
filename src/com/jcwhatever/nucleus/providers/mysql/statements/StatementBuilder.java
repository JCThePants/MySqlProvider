package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatementBuilder;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlBuildOrExecute;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.Nullable;

/*
 * 
 */
public class StatementBuilder implements ISqlStatementBuilder,
        ISqlBuildOrExecute {

    private final Table _table;
    private final Statement _statement;
    private final FinalizedStatement _previous;

    public StatementBuilder(Table table, Statement statement, @Nullable FinalizedStatement previous) {
        PreCon.notNull(table);
        PreCon.notNull(statement);

        _table = table;
        _statement = statement;
        _previous = previous;
    }

    @Override
    public StatementBuilder beginTransaction() {
        _statement.startTransaction(_table.getDatabase().getConnection());
        return this;
    }

    @Override
    public Select selectRow(String... columns) {
        return new Select(_table, columns, _statement);
    }

    @Override
    public Select selectRows(String... columns) {
        return new Select(_table, columns, _statement);
    }

    @Override
    public Update updateRow() {
        return new Update(_table, _statement);
    }

    @Override
    public Update updateRows() {
        return new Update(_table, _statement);
    }

    @Override
    public Insert insertRow(String... columns) {
        return new Insert(_table, columns, _statement);
    }

    @Override
    public Insert insertRows(String... columns) {
        return new Insert(_table, columns, _statement);
    }

    @Override
    public Delete deleteRow() {
        return new Delete(_table, _statement);
    }

    @Override
    public Delete deleteRows() {
        return new Delete(_table, _statement);
    }

    @Override
    public IFutureResult<ISqlResult> execute() {
        finalizeStatement();

        return MySqlProvider.getProvider().execute(_statement.getFinalized());
    }

    @Override
    public StatementBuilder endStatement() {
        finalizeStatement();
        return this;
    }

    @Override
    public StatementBuilder setTable(ISqlTable table) {
        PreCon.isValid(table instanceof Table);
        finalizeStatement();

        return new StatementBuilder((Table)table, _statement, _previous);
    }

    @Override
    public StatementBuilder commitTransaction() {
        finalizeStatement();

        _statement.commitTransaction(_table.getDatabase().getConnection());
        return this;
    }

    @Override
    public PreparedStatement[] prepareStatements() throws SQLException {
        finalizeStatement();

        return _statement.prepareStatements();
    }

    @Override
    public ISqlStatement getStatement() {
        return _previous;
    }

    @Override
    public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
        PreCon.notNull(transaction);

        return transaction.append(_previous);
    }

    private void finalizeStatement() {
        _statement.finalizeStatement(_table.getDatabase().getConnection());
    }
}
