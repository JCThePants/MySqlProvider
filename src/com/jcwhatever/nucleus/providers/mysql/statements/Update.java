package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.Utils;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdate;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateClause;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateFinal;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateLogicalOperator;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateOperator;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.Rand;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlUpdate}.
 */
public class Update implements ISqlUpdate {

    private static final StatementSizeTracker _sizeTracker = new StatementSizeTracker(100, 25);

    private final Statement _statement;
    private final Table _table;
    private final Operator _operator;
    private final BoolOperator _boolOperator;
    private final Clause _clause = new Clause();
    private final Final _final = new Final();

    private List<CompoundValue> _compoundValues;
    private boolean _isFinalized;
    private FinalizedStatement _finalized;

    /**
     * Constructor.
     *
     * @param table  The table the query is being constructed for.
     */
    public Update(Table table, @Nullable Statement statement) {
        PreCon.notNull(table);

        _table = table;
        _statement = statement == null
                ? new Statement(table.getDatabase(), _sizeTracker.getSize(), 10)
                : statement;

        _statement.setType(StatementType.UPDATE);

        _boolOperator = new BoolOperator(_statement);
        _operator = new Operator(_statement);

        int totalCompound = table.getDefinition().totalCompoundColumns();
        _compoundValues = totalCompound > 0 ? new ArrayList<CompoundValue>(totalCompound) : null;

        statement()
                .append("UPDATE ")
                .append(_table.getName())
                .append(" SET ");
    }

    @Override
    public Final set(String columnName, @Nullable Object value) {
        return _final.set(columnName, value);
    }

    @Override
    public Final setColumn(String columnName, String otherColumnName) {
        return _final.setColumn(columnName, otherColumnName);
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

    private FinalizedStatement finalizeStatement() {
        if (_isFinalized)
            return _finalized;

        Utils.appendCompound(_table, _statement, _compoundValues);
        _sizeTracker.registerSize(_statement.getBuffer().length());
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

    private class Operator extends AbstractOperator<ISqlUpdateLogicalOperator>
            implements ISqlUpdateOperator {

        public Operator(Statement statement) {
            super(statement);
        }

        @Override
        protected void assertNotFinalized() {
            Update.this.assertNotFinalized();
        }

        @Override
        protected ISqlUpdateLogicalOperator getConditionOperator() {
            return _boolOperator;
        }
    }

    private class BoolOperator extends AbstractConditionOperator<ISqlUpdateOperator>
            implements ISqlUpdateLogicalOperator {

        public BoolOperator(Statement statement) {
            super(statement);
        }

        @Override
        public Clause limit(int count) {
            return _clause.limit(count) ;
        }

        @Override
        public Clause orderByAscend(String columnName) {
            return _clause.orderByDescend(columnName);
        }

        @Override
        public Clause orderByDescend(String columnName) {
            return _clause.orderByDescend(columnName);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return _final.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return _final.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return _final.addToTransaction(transaction);
        }

        @Override
        public String toString() {
            return _final.toString();
        }

        @Override
        protected void assertNotFinalized() {
            Update.this.assertNotFinalized();
        }

        @Override
        protected void setCurrentColumn(String columnName) {
            _operator.currentColumn = columnName;
        }

        @Override
        protected ISqlUpdateOperator getOperator() {
            return _operator;
        }
    }

    private class Clause implements ISqlUpdateClause {

        boolean hasLimit;
        boolean hasOrder;

        @Override
        public Clause limit(int count) {
            PreCon.positiveNumber(count);
            PreCon.isValid(!hasLimit, "LIMIT is already set.");
            assertNotFinalized();

            statement()
                    .append(" LIMIT ")
                    .append(count);

            hasLimit = true;
            return this;
        }

        @Override
        public Clause orderByAscend(String columnName) {
            PreCon.notNullOrEmpty(columnName);
            assertNotFinalized();

            appendOrder(columnName);
            statement().append(" ASC");
            return this;
        }

        @Override
        public Clause orderByDescend(String columnName) {
            PreCon.notNullOrEmpty(columnName);
            assertNotFinalized();

            appendOrder(columnName);
            statement().append(" DESC");
            return this;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return _final.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return _final.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return _final.addToTransaction(transaction);
        }

        @Override
        public String toString() {
            return _final.toString();
        }

        private void appendOrder(String columnName) {
            if (hasOrder) {
                statement().append(", ");
            }
            else {
                statement().append(" ORDER BY ");
            }

            statement().append(columnName);
            hasOrder = true;
        }
    }

    private class Final implements ISqlUpdateFinal {

        @Override
        public Operator where(String column) {
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            statement()
                    .append(" WHERE ")
                    .append(column);

            return _operator;
        }

        @Override
        public Final set(String columnName, @Nullable Object value) {
            PreCon.notNullOrEmpty(columnName);
            assertNotFinalized();

            ISqlTableColumn column = _table.getDefinition().getColumn(columnName);
            if (column == null) {
                throw new IllegalArgumentException("A column named" + columnName +
                        " is not defined in table " + _table.getName());
            }

            CompoundValue compoundValue = null;

            if (value != null && _compoundValues != null && column.getDataType().isCompound()) {
                compoundValue = new CompoundValue(Rand.getSafeString(10), column, value);
                _compoundValues.add(compoundValue);
            }

            if (!values().isEmpty())
                statement().append(',');

            statement()
                    .append(columnName)
                    .append('=');

            if (compoundValue != null) {
                statement()
                        .append('@')
                        .append(compoundValue.getVariable());
            }
            else if (value == null) {
                statement().append("DEFAULT");
            }
            else {
                statement().append('?');
                values(value);
            }

            return this;
        }

        @Override
        public Final setColumn(String columnName, String otherColumnName) {
            PreCon.notNullOrEmpty(columnName);
            PreCon.notNullOrEmpty(otherColumnName);
            assertNotFinalized();

            if (!values().isEmpty())
                statement().append(',');

            statement()
                    .append(columnName)
                    .append('=')
                    .append(otherColumnName);

            return this;
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
            assertNotFinalized();
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
    }
}
