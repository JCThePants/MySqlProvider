package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundDataManager;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataHandler;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataIterator;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.generators.IOrderGenerator;
import com.jcwhatever.nucleus.providers.sql.statement.generators.SqlColumnOrder;
import com.jcwhatever.nucleus.providers.sql.SqlOrder;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdate;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateClause;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateFinal;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateJoinClause;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateLogicalOperator;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateOperator;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateSetter;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateSetterOperator;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;
import com.jcwhatever.nucleus.utils.text.TextUtils;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
    private final JoinClause _joinClause = new JoinClause();
    private final SetterOperator _setterOperator = new SetterOperator();

    private List<CompoundValue> _compoundValues;
    private int _setCount = 0;
    private boolean _isFinalized;
    private FinalizedStatement _finalized;
    private boolean _isSetAppended;
    private String _currentJoinTable;

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
                .append("UPDATE `")
                .append(_table.getName())
                .append('`');

        if (totalCompound > 0) {

            ISqlTableColumn[] columns = table.getDefinition().getCompoundColumns();

            for (ISqlTableColumn column : columns) {

                ICompoundDataHandler handler =
                        _table.getDatabase().getCompoundManager()
                                .getHandler(column.getDataType());

                if (handler == null)
                    continue;

                ISqlTable handlerTable = handler.getTable();

                //noinspection ConstantConditions
                statement()
                        .append(" LEFT JOIN `")
                        .append(handlerTable.getName())
                        .append("` `")
                        .append(handlerTable.getName())
                        .append('_')
                        .append(column.getName())
                        .append("` ON `")
                        .append(handlerTable.getName())
                        .append('_')
                        .append(column.getName())
                        .append("`.`")
                        .append(handlerTable.getDefinition().getPrimaryKey().getName())
                        .append("`=`")
                        .append(table.getName())
                        .append("`.`")
                        .append(column.getName())
                        .append('`');
            }
        }
    }

    @Override
    public SetterOperator set(String column) {
        return _final.set(column);
    }

    @Override
    public SetterOperator set(ISqlTable table, String column) {
        return _final.set(table, column);
    }

    @Override
    public JoinClause innerJoin(ISqlTable table) {
        PreCon.notNull(table);
        assertNotFinalized();

        //noinspection ConstantConditions
        statement()
                .append(" INNER JOIN `")
                .append(table.getName())
                .append('`');

        _currentJoinTable = '`' + table.getName() + '`';

        return _joinClause;
    }

    @Override
    public JoinClause leftJoin(ISqlTable table) {
        PreCon.notNull(table);
        assertNotFinalized();

        statement()
                .append(" LEFT JOIN `")
                .append(table.getName())
                .append('`');

        _currentJoinTable = '`' + table.getName() + '`';

        return _joinClause;
    }

    @Override
    public JoinClause rightJoin(ISqlTable table) {
        PreCon.notNull(table);
        assertNotFinalized();

        statement()
                .append(" RIGHT JOIN `")
                .append(table.getName())
                .append('`');

        _currentJoinTable = '`' + table.getName() + '`';

        return _joinClause;
    }

    @Override
    public String toString() {
        return _statement.toString();
    }

    private boolean isName(String name) {
        for (int i=0; i < name.length(); i++) {
            if (".+-<>= ".indexOf(name.charAt(i)) != -1) {
                return false;
            }
        }
        return true;
    }

    private void assertNotFinalized() {
        if (_isFinalized)
            throw new IllegalStateException("The Sql statement is already finalized " +
                    "and cannot be modified.");
    }

    private FinalizedStatement finalizeStatement() {
        if (_isFinalized)
            return _finalized;

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

    private void appendSet() {

        if (_isSetAppended)
            return;

        _isSetAppended = true;
        statement().append(" SET ");
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
        public Clause orderByAscend(ISqlTable table, String columnName) {
            return _clause.orderByAscend(table, columnName);
        }

        @Override
        public Clause orderByDescend(String columnName) {
            return _clause.orderByDescend(columnName);
        }

        @Override
        public Clause orderByDescend(ISqlTable table, String columnName) {
            return _clause.orderByDescend(table, columnName);
        }

        @Override
        public Clause orderBy(String columnName, SqlOrder order) {
            return _clause.orderBy(columnName, order);
        }

        @Override
        public Clause orderBy(ISqlTable table, String columnName, SqlOrder order) {
            return _clause.orderBy(table, columnName, order);
        }

        @Override
        public Clause orderBy(IOrderGenerator orderGenerator) {
            return _clause.orderBy(orderGenerator);
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
        boolean isOrdered;

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
            return orderBy(_table, columnName, SqlOrder.ASCENDING);
        }

        @Override
        public Clause orderByAscend(ISqlTable table, String columnName) {
            return orderBy(table, columnName, SqlOrder.ASCENDING);
        }

        @Override
        public Clause orderByDescend(String columnName) {
            return orderBy(_table, columnName, SqlOrder.DESCENDING);
        }

        @Override
        public Clause orderByDescend(ISqlTable table, String columnName) {
            return orderBy(table, columnName, SqlOrder.DESCENDING);
        }

        @Override
        public Clause orderBy(String columnName, SqlOrder order) {
            return orderBy(_table, columnName, order);
        }

        @Override
        public Clause orderBy(ISqlTable table, String columnName, SqlOrder order) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(columnName);
            PreCon.notNull(order);

            appendOrder(table, columnName, order);

            return this;
        }

        @Override
        public Clause orderBy(IOrderGenerator orderGenerator) {
            PreCon.notNull(orderGenerator);

            SqlColumnOrder[] orders = orderGenerator.getOrder(_table);

            for (SqlColumnOrder order : orders) {
                appendOrder(order.getTable(), order.getColumnName(), order.getOrder());
            }

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

        private void appendOrder(ISqlTable table, String columnName, SqlOrder order) {
            if (isOrdered) {
                statement().append(',');
            } else {
                statement().append(" ORDER BY ");
            }

            statement()
                    .append('`')
                    .append(table.getName())
                    .append("`.`")
                    .append(columnName)
                    .append('`');

            switch (order) {
                case ASCENDING:
                    statement().append(" ASC");
                    break;
                case DESCENDING:
                    statement().append(" DESC");
                    break;
            }

            isOrdered = true;
        }
    }

    private class JoinClause implements ISqlUpdateJoinClause {

        @Override
        public ISqlUpdateSetter on(String column) {
            PreCon.notNullOrEmpty(column);

            String[] nameArray = TextUtils.PATTERN_DOT.split(column);
            String name = nameArray[nameArray.length - 1];

            return on(name, name);
        }

        @Override
        public ISqlUpdateSetter on(String column, String otherColumn) {
            PreCon.notNullOrEmpty(column);
            PreCon.notNullOrEmpty(otherColumn);

            statement().append(" ON ");

            if (isName(column)) {
                statement()
                        .append(_currentJoinTable)
                        .append(".`")
                        .append(column)
                        .append("`=");
            }
            else {
                statement().append(column);
            }

            if (isName(otherColumn)) {
                statement()
                        .append('`')
                        .append(_table.getName())
                        .append("`.`")
                        .append(otherColumn)
                        .append('`');
            }
            else {
                statement().append(otherColumn);
            }

            return Update.this;
        }
    }

    private class SetterOperator implements ISqlUpdateSetterOperator {

        @Override
        public Final value(@Nullable Object value) {

            if (_compoundValues != null && _final.currentSetterColumn.getDataType().isCompound()) {

                CompoundDataManager manager = _table.getDatabase().getCompoundManager();
                ICompoundDataHandler handler = manager.getHandler(_final.currentSetterColumn.getDataType());
                if (handler == null) {
                    throw new UnsupportedOperationException("Data type not supported: "
                            + _final.currentSetterColumn.getDataType().getName());
                }

                ICompoundDataIterator iterator = handler.dataIterator(value);

                while (iterator.next()) {

                    statement()
                            .append('`')
                            .append(handler.getTable().getName())
                            .append('_')
                            .append(_final.currentSetterColumn.getName())
                            .append("`.`")
                            .append(iterator.getColumnName())
                            .append("`=?");

                    values(iterator.getValue());

                    if (!iterator.isLast()) {
                        statement().append(',');
                    }
                }
            }
            else {

                appendStart();

                if (value == null) {
                    statement().append("DEFAULT");
                }
                else {
                    statement().append('?');
                    values(value);
                }
            }

            return _final;
        }

        @Override
        public Final equalsColumn(String column) {
            return equalsColumn(_table, column);
        }

        @Override
        public Final equalsColumn(ISqlTable table, String column) {
            PreCon.notNull(column);
            PreCon.notNullOrEmpty(column);

            appendStart();

            statement()
                    .append('`')
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            return _final;
        }

        @Override
        public Final add(int amount) {

            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`+")
                    .append(amount);

            return _final;
        }

        @Override
        public Final add(double amount) {

            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`+")
                    .append(amount);

            return _final;
        }

        @Override
        public Final addColumn(String column) {
            return addColumn(_table, column);
        }

        @Override
        public Final addColumn(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`+`")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            return _final;
        }

        @Override
        public Final subtract(int amount) {
            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`-")
                    .append(amount);

            return _final;
        }

        @Override
        public Final subtract(double amount) {
            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`-")
                    .append(amount);

            return _final;
        }

        @Override
        public Final subtractColumn(String column) {
            return subtractColumn(_table, column);
        }

        @Override
        public Final subtractColumn(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            appendStart();

            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`-`")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            return _final;
        }

        private void appendStart() {
            statement()
                    .append('`')
                    .append(_final.currentSetterTable.getName())
                    .append("`.`")
                    .append(_final.currentSetterColumn.getName())
                    .append("`=");
        }
    }

    private class Final implements ISqlUpdateFinal {

        ISqlTable currentSetterTable;
        ISqlTableColumn currentSetterColumn;

        @Override
        public SetterOperator set(String column) {
            return set(_table, column);
        }

        @Override
        public SetterOperator set(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            appendSet();

            currentSetterColumn = _table.getDefinition().getColumn(column);
            if (currentSetterColumn == null) {
                throw new IllegalArgumentException("A column named" + column +
                        " is not defined in table " + _table.getName());
            }
            currentSetterTable = table;

            if (_setCount > 0)
                statement().append(',');

            _setCount++;

            return _setterOperator;
        }

        @Override
        public Operator where(String column) {
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            statement()
                    .append(" WHERE `")
                    .append(column)
                    .append('`');

            return _operator;
        }

        @Override
        public ISqlUpdateOperator where(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            statement()
                    .append(" WHERE `")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            return _operator;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            finalizeStatement();

            if (_table.getDefinition().isTemp()) {
                throw new IllegalStateException(
                        "Cannot execute statement on a temporary table outside of a transaction.");
            }

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
