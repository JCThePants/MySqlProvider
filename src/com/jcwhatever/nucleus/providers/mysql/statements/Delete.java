package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.Utils;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.delete.ISqlDelete;
import com.jcwhatever.nucleus.providers.sql.statement.delete.ISqlDeleteLogicalOperator;
import com.jcwhatever.nucleus.providers.sql.statement.delete.ISqlDeleteOperator;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlDelete}.
 */
public class Delete implements ISqlDelete {

    private static final StatementSizeTracker _sizeTracker = new StatementSizeTracker(100, 25);

    private final Table _table;
    private final Statement _statement;
    private final BoolOperator _boolOperator;
    private final Operator _operator;

    private List<CompoundValue> _compoundValues;
    private boolean _isFinalized;
    private FinalizedStatement _finalized;

    /**
     * Constructor.
     *
     * @param table      The table the statement is for.
     * @param statement  The current statement context. Null to start a new one.
     */
    public Delete(Table table, @Nullable Statement statement) {
        PreCon.notNull(table);

        _table = table;
        _statement = statement == null
                ? new Statement(table.getDatabase(), _sizeTracker.getSize(), 5)
                : statement;

        _statement.setType(StatementType.UPDATE);

        _boolOperator = new BoolOperator(_statement);
        _operator = new Operator(_statement);

        int totalCompound = table.getDefinition().totalCompoundColumns();
        _compoundValues = totalCompound > 0 ? new ArrayList<CompoundValue>(totalCompound) : null;

        statement()
                .append("DELETE FROM ")
                .append(_table.getName());
    }

    @Override
    public Operator where(String columnName) {
        PreCon.notNullOrEmpty(columnName);
        assertNotFinalized();

        statement()
                .append(" WHERE ")
                .append(columnName);

        _operator.currentColumn = columnName;
        return _operator;
    }

    @Override
    public IFutureResult<ISqlResult> execute() {
        finalizeStatement();

        return MySqlProvider.getProvider().execute(_statement.getFinalized());
    }

    @Override
    public StatementBuilder endStatement() {
        assertNotFinalized();
        finalizeStatement();

        return new StatementBuilder(_table, _statement, _finalized);
    }

    @Override
    public StatementBuilder setTable(ISqlTable table) {
        PreCon.isValid(table instanceof Table);
        assertNotFinalized();
        finalizeStatement();

        return new StatementBuilder((Table)table, _statement, _finalized);
    }

    @Override
    public StatementBuilder commitTransaction() {
        finalizeStatement();

        _statement.commitTransaction(_table.getDatabase().getConnection());
        return new StatementBuilder(_table, _statement, _finalized);
    }

    @Override
    public PreparedStatement[] prepareStatements() throws SQLException {
        finalizeStatement();

        return _statement.prepareStatements();
    }

    @Override
    public ISqlStatement getStatement() {
        return finalizeStatement();
    }

    @Override
    public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
        PreCon.notNull(transaction);

        finalizeStatement();
        return transaction.append(_statement.getFinalized());
    }

    @Override
    public String toString() {
        return _statement.toString();
    }

    private void assertNotFinalized() {
        if (_isFinalized)
            throw new IllegalStateException("The Sql statement is already finalized " +
                    "and cannot be modified.");
    }

    @Nullable
    private FinalizedStatement finalizeStatement() {
        if (_isFinalized)
            return _finalized;

        Utils.appendCompound(_table, _statement, _compoundValues);
        _sizeTracker.registerSize(_statement.length());
        _finalized = _statement.finalizeStatement(_table);
        _isFinalized = true;
        return _finalized;
    }

    private StringBuilder statement() {
        return _statement.getBuffer();
    }

    private void values(Object value) {
        _statement.getValues().add(value);
    }

    private List<Object> values() {
        return _statement.getValues();
    }

    private class Operator extends AbstractOperator<ISqlDeleteLogicalOperator>
            implements ISqlDeleteOperator {

        public Operator(Statement statement) {
            super(statement, _table, _compoundValues);
        }

        @Override
        protected void assertNotFinalized() {
            Delete.this.assertNotFinalized();
        }

        @Override
        protected ISqlDeleteLogicalOperator getConditionOperator() {
            return _boolOperator;
        }
    }

    private class BoolOperator extends AbstractConditionOperator<ISqlDeleteOperator>
            implements ISqlDeleteLogicalOperator {

        public BoolOperator(Statement statement) {
            super(statement);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Delete.this.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return Delete.this.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return Delete.this.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return Delete.this.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Delete.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Delete.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Delete.this.addToTransaction(transaction);
        }

        @Override
        public String toString() {
            return Delete.this.toString();
        }

        @Override
        protected void assertNotFinalized() {
            Delete.this.assertNotFinalized();
        }

        @Override
        protected void setCurrentColumn(String columnName) {
            _operator.currentColumn = columnName;
        }

        @Override
        protected ISqlDeleteOperator getOperator() {
            return _operator;
        }
    }
}
