package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.Utils;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlNextStatementBuilder;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.generators.IColumnNameGenerator;
import com.jcwhatever.nucleus.providers.sql.statement.generators.IOrderGenerator;
import com.jcwhatever.nucleus.providers.sql.statement.generators.SqlColumnOrder;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertInto;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoExists;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoFinal;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoJoinClause;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoReselect;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoSelect;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoSetterOperator;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertIntoWhere;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertLogicalOperator;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertOperator;
import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlBuildOrExecute;
import com.jcwhatever.nucleus.providers.sql.SqlOrder;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.Rand;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;
import com.jcwhatever.nucleus.utils.text.TextUtils;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/*
 * 
 */
public class InsertInto implements ISqlInsertInto {

    private static final StatementSizeTracker _sizeTracker = new StatementSizeTracker(100, 25);

    private final Statement _statement;
    private final Table _table;
    private final Select _select = new Select();
    private final Reselect _reselect = new Reselect();
    private final Where _where = new Where();
    private final Operator _operator;
    private final InsertLogicalOperator _insertLogicalOperator;
    private final JoinClause _joinClause = new JoinClause();
    private final SetterOperator _setterOperator = new SetterOperator();
    private final Exists _exists = new Exists();
    private final Final _final = new Final();

    private List<CompoundValue> _compoundValues;
    private boolean _isFinalized;
    private FinalizedStatement _finalized;

    /**
     * Constructor.
     *
     * @param table          The table the statement is for.
     * @param intoTableName  The name of the table to insert into.
     */
    public InsertInto(Table table, String intoTableName, @Nullable Statement statement) {

        PreCon.notNull(table);
        PreCon.notNullOrEmpty(intoTableName);

        _table = table;
        _statement = statement == null
                ? new Statement(table.getDatabase(), _sizeTracker.getSize(), 15)
                : statement;

        _statement.setType(StatementType.UPDATE);

        int totalCompound = table.getDefinition().totalCompoundColumns();
        _compoundValues = totalCompound > 0 ? new ArrayList<CompoundValue>(totalCompound) : null;

        _operator = new Operator(_statement);
        _insertLogicalOperator = new InsertLogicalOperator(_statement);

        statement()
                .append("INSERT INTO `")
                .append(intoTableName)
                .append('`');
    }

    @Override
    public Select columns(String... columnNames) {

        PreCon.notNull(columnNames);

        if (columnNames.length == 0) {
            statement().append(" * ");
            return _select;
        }

        statement().append(" (");

        for (int i = 0; i < columnNames.length; i++) {

            statement()
                    .append('`')
                    .append(columnNames[i])
                    .append('`');

            if (i < columnNames.length - 1)
                statement().append(',');
        }

        statement().append(')');

        return _select;
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
        return finalizeStatement(false);
    }

    @Nullable
    private FinalizedStatement finalizeStatement(boolean force) {
        if (_isFinalized && !force)
            return _finalized;

        _select.finishSelecting();
        Utils.insertCompound(_table, _statement, _compoundValues);
        _sizeTracker.registerSize(_statement.length());
        _finalized = _statement.finalizeStatement(_table);
        _isFinalized = true;
        return _finalized;
    }

    private StringBuilder statement() {
        return _statement.getBuffer();
    }

    private void addValue(Object value) {
        _statement.getValues().add(value);
    }

    private List<Object> statementValues() {
        return _statement.getValues();
    }

    private CompoundValue addCompoundValue(ISqlTableDefinition.ISqlTableColumn column, @Nullable Object value) {
        CompoundValue compoundValue = new CompoundValue(Rand.getSafeString(10), column, value);
        _compoundValues.add(compoundValue);
        return compoundValue;
    }

    private void appendCompoundValue(@Nullable CompoundValue compoundValue) {

        if (compoundValue != null) {
            statement()
                    .append('@')
                    .append(compoundValue.getVariable());
        }
        else {
            statement()
                    .append("DEFAULT");
        }
    }

    private class Reselect implements ISqlInsertIntoReselect {

        @Override
        public Reselect select(ISqlTable table, String column) {
            _select.select(table, column);
            return this;
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final orderByAscend(String column) {
            return _final.orderByAscend(column);
        }

        @Override
        public Final orderByAscend(ISqlTable table, String columnName) {
            return _final.orderByAscend(table, columnName);
        }

        @Override
        public Final orderByDescend(String column) {
            return _final.orderByDescend(column);
        }

        @Override
        public Final orderByDescend(ISqlTable table, String columnName) {
            return _final.orderByDescend(table, columnName);
        }

        @Override
        public Final orderBy(String column, SqlOrder order) {
            return _final.orderBy(column, order);
        }

        @Override
        public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
            return _final.orderBy(table, columnName, order);
        }

        @Override
        public Final orderBy(IOrderGenerator orderGenerator) {
            return _final.orderBy(orderGenerator);
        }

        @Override
        public Operator where(String column) {
            return _where.where(column);
        }

        @Override
        public Operator where(ISqlTable table, String column) {
            return _where.where(table, column);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public ISqlNextStatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public ISqlNextStatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return new PreparedStatement[0];
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
        public ISqlBuildOrExecute commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public JoinClause innerJoin(ISqlTable table) {
            return _where.innerJoin(table);
        }

        @Override
        public JoinClause leftJoin(ISqlTable table) {
            return _where.leftJoin(table);
        }

        @Override
        public JoinClause rightJoin(ISqlTable table) {
            return _where.rightJoin(table);
        }

        @Override
        public Exists ifExists() {
            return _final.ifExists();
        }
    }

    private class Select implements ISqlInsertIntoSelect {

        boolean isSelecting;

        @Override
        public Where select(String... columns) {

            statement().append(" SELECT ");

            for (int i=0; i < columns.length; i++) {

                statement()
                        .append('`')
                        .append(columns[i])
                        .append('`');

                if (i < columns.length - 1)
                    statement().append(',');
            }

            statement().append(" FROM `")
                    .append(_table.getName())
                    .append('`');

            return _where;
        }

        @Override
        public Where select(IColumnNameGenerator nameGenerator) {
            PreCon.notNull(nameGenerator);

            return select(nameGenerator.getColumnNames(_table));
        }

        @Override
        public ISqlInsertIntoReselect select(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            if (isSelecting)
                statement().append(',');
            else
                statement().append(" SELECT ");

            statement()
                    .append('`')
                    .append(column)
                    .append('`');

            isSelecting = true;

            return _reselect;
        }

        void finishSelecting() {

            if (!isSelecting)
                return;

            isSelecting = false;

            statement().append(" FROM `")
                    .append(_table.getName())
                    .append('`');
        }
    }

    private class Where implements ISqlInsertIntoWhere {

        private String _currentJoinTable;

        @Override
        public Operator where(String column) {
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            _select.finishSelecting();

            statement()
                    .append(" WHERE ");

            if (isName(column)) {
                statement()
                        .append('`')
                        .append(column)
                        .append('`');
            }
            else {
                statement().append(column);
            }

            _operator.currentColumn = column;

            return _operator;
        }

        @Override
        public Operator where(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            _select.finishSelecting();

            statement()
                    .append(" WHERE ");

            statement()
                    .append('`')
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            _operator.currentColumn = column;

            return _operator;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public ISqlNextStatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public ISqlNextStatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
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
        public ISqlBuildOrExecute commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final orderByAscend(String columnName) {
            return _final.orderByAscend(columnName);
        }

        @Override
        public Final orderByAscend(ISqlTable table, String columnName) {
            return _final.orderByAscend(table, columnName);
        }

        @Override
        public Final orderByDescend(String columnName) {
            return _final.orderByDescend(columnName);
        }

        @Override
        public Final orderByDescend(ISqlTable table, String columnName) {
            return _final.orderByDescend(table, columnName);
        }

        @Override
        public Final orderBy(String columnName, SqlOrder order) {
            return _final.orderBy(columnName, order);
        }

        @Override
        public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
            return _final.orderBy(table, columnName, order);
        }

        @Override
        public Final orderBy(IOrderGenerator orderGenerator) {
            return _final.orderBy(orderGenerator);
        }

        @Override
        public JoinClause innerJoin(ISqlTable table) {
            PreCon.notNull(table);
            assertNotFinalized();

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
        public Exists ifExists() {
            return _final.ifExists();
        }
    }

    private class JoinClause implements ISqlInsertIntoJoinClause {

        @Override
        public Where on(String column) {
            PreCon.notNullOrEmpty(column);

            String[] nameArray = TextUtils.PATTERN_DOT.split(column);
            String name = nameArray[nameArray.length - 1];

            return on(name, name);
        }

        @Override
        public Where on(String column, String otherColumn) {
            PreCon.notNullOrEmpty(column);
            PreCon.notNullOrEmpty(otherColumn);

            statement().append(" ON ");

            if (isName(column)) {
                statement()
                        .append(_where._currentJoinTable)
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

            return _where;
        }
    }


    private class Operator extends AbstractOperator<ISqlInsertLogicalOperator> implements ISqlInsertOperator {

        public Operator(Statement statement) {
            super(statement);
        }

        @Override
        protected void assertNotFinalized() {
            InsertInto.this.assertNotFinalized();
        }

        @Override
        protected InsertLogicalOperator getConditionOperator() {

            return _insertLogicalOperator;
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final orderByAscend(String columnName) {
            return _final.orderByAscend(columnName);
        }

        @Override
        public Final orderByAscend(ISqlTable table, String columnName) {
            return _final.orderByAscend(table, columnName);
        }

        @Override
        public Final orderByDescend(String columnName) {
            return _final.orderByDescend(columnName);
        }

        @Override
        public Final orderByDescend(ISqlTable table, String columnName) {
            return _final.orderByDescend(table, columnName);
        }

        @Override
        public Final orderBy(String columnName, SqlOrder order) {
            return _final.orderBy(columnName, order);
        }

        @Override
        public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
            return _final.orderBy(table, columnName, order);
        }

        @Override
        public Final orderBy(IOrderGenerator orderGenerator) {
            return _final.orderBy(orderGenerator);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public ISqlNextStatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public ISqlNextStatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
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
        public ISqlBuildOrExecute commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public Exists ifExists() {
            return _final.ifExists();
        }
    }

    private class InsertLogicalOperator extends AbstractConditionOperator<ISqlInsertOperator>
            implements ISqlInsertLogicalOperator {

        /**
         * Constructor.
         *
         * @param statement The statement the operator applies to.
         */
        public InsertLogicalOperator(Statement statement) {
            super(statement);
        }

        @Override
        protected void assertNotFinalized() {
            InsertInto.this.assertNotFinalized();
        }

        @Override
        protected void setCurrentColumn(String columnName) {
            _operator.currentColumn = columnName;
        }

        @Override
        protected Operator getOperator() {
            return _operator;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return _final.execute();
        }

        @Override
        public ISqlNextStatementBuilder endStatement() {
            return _final.endStatement();
        }

        @Override
        public ISqlNextStatementBuilder setTable(ISqlTable table) {
            return _final.setTable(table);
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
        public ISqlBuildOrExecute commitTransaction() {
            return _final.commitTransaction();
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final orderByAscend(String columnName) {
            return _final.orderByAscend(columnName);
        }

        @Override
        public Final orderByAscend(ISqlTable table, String columnName) {
            return _final.orderByAscend(table, columnName);
        }

        @Override
        public Final orderByDescend(String columnName) {
            return _final.orderByDescend(columnName);
        }

        @Override
        public Final orderByDescend(ISqlTable table, String columnName) {
            return _final.orderByDescend(table, columnName);
        }

        @Override
        public Final orderBy(String columnName, SqlOrder order) {
            return _final.orderBy(columnName, order);
        }

        @Override
        public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
            return _final.orderBy(table, columnName, order);
        }

        @Override
        public Final orderBy(IOrderGenerator orderGenerator) {
            return _final.orderBy(orderGenerator);
        }

        @Override
        public Exists ifExists() {
            return _final.ifExists();
        }
    }

    private class SetterOperator implements ISqlInsertIntoSetterOperator {

        @Override
        public Exists value(@Nullable Object value) {

            if (_exists.currentSetterColumn.getDataType().isCompound()) {
                CompoundValue compoundValue = addCompoundValue(_exists.currentSetterColumn, value);
                appendCompoundValue(compoundValue);
            }
            else if (value == null) {
                statement().append("DEFAULT");
            }
            else {
                statement().append('?');
                addValue(value);
            }

            _exists.hasValues = true;

            return _exists;
        }

        @Override
        public Exists equalsColumn(String column) {
            return equalsColumn(_table, column);
        }

        @Override
        public Exists equalsColumn(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            statement()
                    .append('`')
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists add(int amount) {

            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`+")
                    .append(amount);

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists add(double amount) {

            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`+")
                    .append(amount);

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists addColumn(String column) {
            return addColumn(_table, column);
        }

        @Override
        public Exists addColumn(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`+`")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists subtract(int amount) {
            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`-")
                    .append(amount);

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists subtract(double amount) {
            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`-")
                    .append(amount);

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists subtractColumn(String column) {
            return subtractColumn(_table, column);
        }

        @Override
        public Exists subtractColumn(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);

            statement()
                    .append('`')
                    .append(_exists.currentTable.getName())
                    .append("`.`")
                    .append(_exists.currentSetterColumn.getName())
                    .append("`-`")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists largerColumn(ISqlTable table, String column) {
            return largerColumn(_exists.currentTable, _exists.currentSetterColumn.getName(), table, column);
        }

        @Override
        public Exists largerColumn(ISqlTable table1, String column1,
                                   ISqlTable table2, String column2) {
            PreCon.notNull(table1);
            PreCon.notNullOrEmpty(column1);
            PreCon.notNull(table2);
            PreCon.notNullOrEmpty(column2);

            statement()
                    .append("IF (")
                    .append('`')
                    .append(table1.getName())
                    .append("`.`")
                    .append(column1)
                    .append("`>`")
                    .append(table2.getName())
                    .append("`.`")
                    .append(column2)
                    .append("`,`")
                    .append(table1.getName())
                    .append("`.`")
                    .append(column1)
                    .append("`,`")
                    .append(table2.getName())
                    .append("`.`")
                    .append(column2)
                    .append("`)");

            _exists.hasValues = true;
            return _exists;
        }

        @Override
        public Exists smallerColumn(ISqlTable table, String column) {
            return smallerColumn(_exists.currentTable, _exists.currentSetterColumn.getName(), table, column);
        }

        @Override
        public Exists smallerColumn(ISqlTable table1, String column1,
                                    ISqlTable table2, String column2) {
            PreCon.notNull(table1);
            PreCon.notNullOrEmpty(column1);
            PreCon.notNull(table2);
            PreCon.notNullOrEmpty(column2);

            statement()
                    .append("IF (")
                    .append('`')
                    .append(table1.getName())
                    .append("`.`")
                    .append(column1)
                    .append("`<`")
                    .append(table2.getName())
                    .append("`.`")
                    .append(column2)
                    .append("`,`")
                    .append(table1.getName())
                    .append("`.`")
                    .append(column1)
                    .append("`,`")
                    .append(table2.getName())
                    .append("`.`")
                    .append(column2)
                    .append("`)");

            _exists.hasValues = true;
            return _exists;
        }
    }

    private class Exists implements ISqlInsertIntoExists {

        ISqlTable currentTable;
        ISqlTableDefinition.ISqlTableColumn currentSetterColumn;
        boolean hasValues;

        @Override
        public SetterOperator set(String columnName) {
            PreCon.notNullOrEmpty(columnName);
            assertNotFinalized();

            currentSetterColumn = _table.getDefinition().getColumn(columnName);
            if (currentSetterColumn == null) {
                throw new IllegalArgumentException("A column named" + columnName +
                        " is not defined in table " + _table.getName());
            }
            currentTable = _table;

            if (hasValues)
                statement().append(',');

            statement()
                    .append('`')
                    .append(columnName)
                    .append("`=");

            return _setterOperator;
        }

        @Override
        public SetterOperator set(ISqlTable table, String columnName) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(columnName);
            assertNotFinalized();

            currentSetterColumn = table.getDefinition().getColumn(columnName);
            if (currentSetterColumn == null) {
                throw new IllegalArgumentException("A column named" + columnName +
                        " is not defined in table " + _table.getName());
            }
            currentTable = table;

            if (hasValues)
                statement().append(',');

            statement()
                    .append('`')
                    .append(table.getName())
                    .append("`.`")
                    .append(columnName)
                    .append("`=");

            return _setterOperator;
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
    }

    private class Final implements ISqlInsertIntoFinal {

        boolean isOrdered;
        boolean isLimited;

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

        @Override
        public Final limit(int count) {
            PreCon.positiveNumber(count);
            PreCon.isValid(!isLimited, "LIMIT has already been set.");

            isLimited = true;

            statement()
                    .append(" LIMIT ")
                    .append(count);
            return this;
        }

        @Override
        public Final orderByAscend(String columnName) {
            return orderBy(_table, columnName, SqlOrder.ASCENDING);
        }

        @Override
        public Final orderByAscend(ISqlTable table, String columnName) {
            return orderBy(table, columnName, SqlOrder.ASCENDING);
        }

        @Override
        public Final orderByDescend(String columnName) {
            return orderBy(_table, columnName, SqlOrder.DESCENDING);
        }

        @Override
        public Final orderByDescend(ISqlTable table, String columnName) {
            return orderBy(table, columnName, SqlOrder.DESCENDING);
        }

        @Override
        public Final orderBy(String columnName, SqlOrder order) {
            return orderBy(_table, columnName, order);
        }

        @Override
        public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(columnName);
            PreCon.notNull(order);

            appendOrder(table, columnName, order);

            return this;
        }

        @Override
        public Final orderBy(IOrderGenerator orderGenerator) {
            PreCon.notNull(orderGenerator);

            SqlColumnOrder[] orders = orderGenerator.getOrder(_table);

            for (SqlColumnOrder order : orders) {
                appendOrder(order.getTable(), order.getColumnName(), order.getOrder());
            }

            return this;
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

        @Override
        public Exists ifExists() {
            assertNotFinalized();

            statement().append(" ON DUPLICATE KEY UPDATE ");
            return _exists;
        }
    }
}
