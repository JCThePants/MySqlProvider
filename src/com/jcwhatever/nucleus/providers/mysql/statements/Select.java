package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundDataManager;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataHandler;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlNextStatementBuilder;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.generators.IOrderGenerator;
import com.jcwhatever.nucleus.providers.sql.statement.generators.SqlColumnOrder;
import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlBuildOrExecute;
import com.jcwhatever.nucleus.providers.sql.SqlOrder;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelect;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectFinal;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectJoin;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectJoinClause;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectLogicalOperator;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectOperator;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelectWhere;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;
import com.jcwhatever.nucleus.utils.text.TextUtils;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link ISqlSelect}.
 */
public class Select implements ISqlSelect {

    private static final StatementSizeTracker _sizeTracker = new StatementSizeTracker(100, 25);

    private Statement _statement;
    private final Table _table;

    private Operator _operator;
    private BoolOperator _boolOperator;
    private final Where _where = new Where();
    private final Join _join = new Join();
    private final JoinClause _joinClause = new JoinClause();
    private final Final _final = new Final();

    private boolean _isSelectInto;
    private boolean _isFinalized;
    private boolean _selectAll;
    private boolean _hasJoins;
    private FinalizedStatement _finalized;

    /**
     * Constructor.
     *
     * @param table    The table the query is for.
     * @param columns  The names of the columns being queried.
     * @param query    Existing query to append to.
     */
    public Select(Table table, String[] columns,
                  @Nullable Statement query) {

        PreCon.notNull(table);
        PreCon.notNull(columns);

        _table = table;
        _statement = query == null ? new Statement(table.getDatabase(), _sizeTracker.getSize(), 5) : query;
        _selectAll = columns.length == 0;

        _statement.setColumns(columns.length > 0 ? columns : table.getDefinition().getColumnNames());
        _statement.setType(StatementType.QUERY);
    }

    @Override
    public ISqlSelectWhere into(String tableName) {
        PreCon.notNullOrEmpty(tableName);
        assertNotFinalized();

        _isSelectInto = true;

        statement()
                .append("SELECT INTO ")
                .append(tableName)
                .append(" (");

        String[] columns = _statement.getColumns();

        for (int i=0; i < columns.length; i++) {

            if (isName(columns[i])) {
                statement()
                        .append('`')
                        .append(tableName)
                        .append("`.`")
                        .append(columns[i])
                        .append('`');
            }
            else {
                statement().append(columns[i]);
            }

            if (i < columns.length - 1)
                statement().append(',');
        }

        statement().append(") SELECT ");

        for (int i=0; i < columns.length; i++) {
            statement()
                    .append('`')
                    .append(_table.getName())
                    .append("`.`")
                    .append(columns[i])
                    .append('`');

            if (i < columns.length - 1)
                statement().append(',');
        }

        statement()
                .append(" FROM ")
                .append(_table.getName());

        if (_table.getDefinition().hasCompoundDataTypes()) {
            writeCompoundStatements();
        }

        return _where;
    }

    @Override
    public Operator where(String column) {
        writeBeginning();
        return _where.where(column);
    }

    @Override
    public Operator where(ISqlTable table, String column) {
        writeBeginning();
        return _where.where(table, column);
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

    @Override
    public Final limit(int count) {
        writeBeginning();
        return _final.limit(count);
    }

    @Override
    public Final limit(int offset, int count) {
        writeBeginning();
        return _final.limit(offset, count);
    }

    @Override
    public Final orderByAscend(String columnName) {
        writeBeginning();
        return _final.orderByAscend(columnName);
    }

    @Override
    public Final orderByAscend(ISqlTable table, String columnName) {
        writeBeginning();
        return _final.orderByAscend(table, columnName);
    }

    @Override
    public Final orderByDescend(String columnName) {
        writeBeginning();
        return _final.orderByDescend(columnName);
    }

    @Override
    public Final orderByDescend(ISqlTable table, String columnName) {
        writeBeginning();
        return _final.orderByDescend(table, columnName);
    }

    @Override
    public Final orderBy(String columnName, SqlOrder order) {
        writeBeginning();
        return _final.orderBy(columnName, order);
    }

    @Override
    public Final orderBy(ISqlTable table, String columnName, SqlOrder order) {
        writeBeginning();
        return _final.orderBy(table, columnName, order);
    }

    @Override
    public Final orderBy(IOrderGenerator orderGenerator) {
        writeBeginning();
        return _final.orderBy(orderGenerator);
    }

    private boolean isName(String name) {
        for (int i=0; i < name.length(); i++) {
            if (".+-<>= ".indexOf(name.charAt(i)) != -1) {
                return false;
            }
        }
        return true;
    }

    private FinalizedStatement finalizeStatement() {
        if (_isFinalized)
            return _finalized;

        writeBeginning();
        _sizeTracker.registerSize(_statement.length());
        _finalized = _statement.finalizeStatement(_table);
        _isFinalized = true;
        return _finalized;
    }

    private void assertNotFinalized() {
        if (_isFinalized)
            throw new IllegalStateException("The Sql statement is already finalized " +
                    "and cannot be modified.");
    }

    private void writeBeginning() {
        if (_statement.length() != 0)
            return;

        assertNotFinalized();

        statement().append("SELECT ");

        if (_selectAll && _hasJoins) {
            statement().append('*');
        }
        else {


            ISqlTableDefinition definition = _table.getDefinition();
            CompoundDataManager manager = _table.getDatabase().getCompoundManager();
            String[] columns = _statement.getColumns();

            for (int i = 0; i < columns.length; i++) {

                // handle compound value table columns
                ISqlTableColumn column = definition.getColumn(columns[i]);
                if (column != null && column.getDataType().isCompound()) {


                    ICompoundDataHandler handler = manager.getHandler(column.getDataType());
                    if (handler == null) {
                        throw new UnsupportedOperationException("Data type not supported: "
                                + column.getDataType().getName());
                    }

                    String[] columnNames = handler.getTable().getDefinition().getColumnNames();

                    for (int c = 0; c < columnNames.length; c++) {

                        statement()
                                .append('`')
                                .append(handler.getTable().getName())
                                .append('_')
                                .append(columns[i])
                                .append("`.`")
                                .append(columnNames[c])
                                .append('`');

                        if (c < columnNames.length - 1)
                            statement().append(',');
                    }
                }
                else {

                    if (isName(columns[i])) {
                        statement()
                                .append('`')
                                .append(_table.getName())
                                .append("`.`")
                                .append(columns[i])
                                .append('`');
                    }
                    else {
                        statement().append(columns[i]);
                    }
                }

                if (i < columns.length - 1)
                    statement().append(',');
            }
        }

        statement()
                .append(" FROM `")
                .append(_table.getName())
                .append('`');

        if (_table.getDefinition().hasCompoundDataTypes()) {
            writeCompoundStatements();
        }
    }

    private void writeCompoundStatements() {

        assertNotFinalized();

        ISqlTableDefinition definition = _table.getDefinition();
        CompoundDataManager manager = _table.getDatabase().getCompoundManager();

        ISqlTableColumn[] columns = definition.getCompoundColumns();

        for (ISqlTableColumn column : columns) {

            ICompoundDataHandler handler = manager.getHandler(column.getDataType());
            if (handler == null) {
                throw new UnsupportedOperationException("Data type not supported: "
                        + column.getDataType().getName());
            }

            ISqlTable table = handler.getTable();

            ISqlTableColumn primary = table.getDefinition().getPrimaryKey();
            if (primary == null)
                throw new IllegalStateException("Primary key missing from compound table: "
                        + table.getName());

            statement()
                    .append(" INNER JOIN `")
                    .append(table.getName())
                    .append("` AS `")
                    .append(table.getName())
                    .append('_')
                    .append(column.getName())
                    .append("` ON `")
                    .append(_table.getName())
                    .append("`.`")
                    .append(column.getName())
                    .append("`=`")
                    .append(table.getName())
                    .append('_')
                    .append(column.getName())
                    .append("`.`")
                    .append(primary.getName())
                    .append('`');
        }
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

    @Override
    public JoinClause innerJoin(ISqlTable table) {
        _hasJoins = true;
        writeBeginning();
        return _join.innerJoin(table);
    }

    @Override
    public JoinClause leftJoin(ISqlTable table) {
        _hasJoins = true;
        writeBeginning();
        return _join.leftJoin(table);
    }

    @Override
    public JoinClause rightJoin(ISqlTable table) {
        _hasJoins = true;
        writeBeginning();
        return _join.rightJoin(table);
    }

    private class Join implements ISqlSelectJoin {

        private String _currentJoinTable;

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
        public Operator where(String column) {
            return _where.where(column);
        }

        @Override
        public Operator where(ISqlTable table, String column) {
            return _where.where(table, column);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Select.this.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return Select.this.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return Select.this.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return Select.this.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Select.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Select.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Select.this.addToTransaction(transaction);
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final limit(int offset, int count) {
            return _final.limit(offset, count);
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
        public String toString() {
            return Select.this.toString();
        }
    }

    private class JoinClause implements ISqlSelectJoinClause {

        @Override
        public Operator where(String column) {
            return _where.where(column);
        }

        @Override
        public ISqlSelectOperator where(ISqlTable table, String column) {
            return _where.where(table, column);
        }

        @Override
        public Join on(String column) {
            PreCon.notNullOrEmpty(column);

            String[] nameArray = TextUtils.PATTERN_DOT.split(column);
            String name = nameArray[nameArray.length - 1];

            return on(name, name);
        }

        @Override
        public Join on(String column, String otherColumn) {
            PreCon.notNullOrEmpty(column);
            PreCon.notNullOrEmpty(otherColumn);

            statement().append(" ON ");

            if (isName(column)) {
                statement()
                        .append(_join._currentJoinTable)
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

            return _join;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Select.this.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return Select.this.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return Select.this.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return Select.this.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Select.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Select.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Select.this.addToTransaction(transaction);
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
        public Final limit(int offset, int count) {
            return _final.limit(offset, count);
        }

        @Override
        public String toString() {
            return Select.this.toString();
        }
    }

    private class Where implements ISqlSelectWhere {

        @Override
        public Operator where(String column) {
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            statement()
                    .append(" WHERE `")
                    .append(column)
                    .append('`');

            _boolOperator = new BoolOperator();
            _operator = new Operator();

            _operator.currentColumn = column;

            return _operator;
        }

        @Override
        public Operator where(ISqlTable table, String column) {
            PreCon.notNull(table);
            PreCon.notNullOrEmpty(column);
            assertNotFinalized();

            statement()
                    .append(" WHERE `")
                    .append(table.getName())
                    .append("`.`")
                    .append(column)
                    .append('`');

            _boolOperator = new BoolOperator();
            _operator = new Operator();

            _operator.currentColumn = column;

            return _operator;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Select.this.execute();
        }

        @Override
        public StatementBuilder endStatement() {
            return Select.this.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return Select.this.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return Select.this.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Select.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Select.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Select.this.addToTransaction(transaction);
        }

        @Override
        public String toString() {
            return Select.this.toString();
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final limit(int offset, int count) {
            return _final.limit(offset, count);
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
    }

    private class Operator extends AbstractOperator<ISqlSelectLogicalOperator>
            implements ISqlSelectOperator {

        public Operator() {
            super(_statement);
        }

        @Override
        protected void assertNotFinalized() {
            Select.this.assertNotFinalized();
        }

        @Override
        protected ISqlSelectLogicalOperator getConditionOperator() {
            return _boolOperator;
        }
    }

    private class BoolOperator extends AbstractConditionOperator<ISqlSelectOperator>
            implements ISqlSelectLogicalOperator {

        public BoolOperator() {
            super(_statement);
        }

        @Override
        protected void assertNotFinalized() {
            Select.this.assertNotFinalized();
        }

        @Override
        protected void setCurrentColumn(String columnName) {
            _operator.currentColumn = columnName;
        }

        @Override
        protected ISqlSelectOperator getOperator() {
            return _operator;
        }

        @Override
        public Select unionSelect(ISqlTable table, String... columnNames) {
            PreCon.isValid(table instanceof Table);
            PreCon.notNull(columnNames);
            assertNotFinalized();

            statement().append(" UNION ");
            return new Select((Table)table, columnNames, _statement);
        }

        @Override
        public Select unionAllSelect(ISqlTable table, String... columnNames) {
            PreCon.isValid(table instanceof Table);
            PreCon.notNull(columnNames);
            assertNotFinalized();

            statement().append(" UNION ALL ");
            return new Select((Table)table, columnNames, _statement);
        }

        @Override
        public StatementBuilder endStatement() {
            return Select.this.endStatement();
        }

        @Override
        public StatementBuilder setTable(ISqlTable table) {
            return Select.this.setTable(table);
        }

        @Override
        public StatementBuilder commitTransaction() {
            return Select.this.commitTransaction();
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Select.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Select.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Select.this.addToTransaction(transaction);
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Select.this.execute();
        }

        @Override
        public String toString() {
            return Select.this.toString();
        }

        @Override
        public Final limit(int count) {
            return _final.limit(count);
        }

        @Override
        public Final limit(int offset, int count) {
            return _final.limit(offset, count);
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
    }

    private class Final implements ISqlSelectFinal {

        boolean isLimited;
        boolean isOrdered;

        @Override
        public Final limit(int count) {
            PreCon.positiveNumber(count);
            PreCon.isValid(!_isSelectInto, "LIMIT is not supported by the SELECT INTO statement.");
            PreCon.isValid(!isLimited, "LIMIT has already been set.");

            isLimited = true;

            statement()
                    .append(" LIMIT ")
                    .append(count);

            return this;
        }

        @Override
        public Final limit(int offset, int count) {
            PreCon.positiveNumber(offset);
            PreCon.positiveNumber(count);
            PreCon.isValid(!_isSelectInto, "LIMIT is not supported by the SELECT INTO statement.");
            PreCon.isValid(!isLimited, "LIMIT has already been set.");

            isLimited = true;

            statement()
                    .append(" LIMIT ")
                    .append(offset)
                    .append(',')
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
            PreCon.isValid(!_isSelectInto, "Column order is not supported by the SELECT INTO statement.");

            SqlColumnOrder[] orders = orderGenerator.getOrder(_table);

            for (SqlColumnOrder order : orders) {
                appendOrder(order.getTable(), order.getColumnName(), order.getOrder());
            }

            return this;
        }

        @Override
        public IFutureResult<ISqlResult> execute() {
            return Select.this.execute();
        }

        @Override
        public ISqlNextStatementBuilder endStatement() {
            return Select.this.endStatement();
        }

        @Override
        public ISqlNextStatementBuilder setTable(ISqlTable table) {
            return Select.this.setTable(table);
        }

        @Override
        public PreparedStatement[] prepareStatements() throws SQLException {
            return Select.this.prepareStatements();
        }

        @Override
        public ISqlStatement getStatement() {
            return Select.this.getStatement();
        }

        @Override
        public IFutureResult<ISqlResult> addToTransaction(ISqlTransaction transaction) {
            return Select.this.addToTransaction(transaction);
        }

        @Override
        public ISqlBuildOrExecute commitTransaction() {
            return Select.this.commitTransaction();
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
}
