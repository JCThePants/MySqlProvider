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
import com.jcwhatever.nucleus.providers.sql.statement.insert.ISqlInsert;
import com.jcwhatever.nucleus.providers.sql.statement.insert.ISqlInsertExists;
import com.jcwhatever.nucleus.providers.sql.statement.insert.ISqlInsertFinal;
import com.jcwhatever.nucleus.providers.sql.statement.insert.ISqlInsertSetterOperator;
import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlExecutable;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.Rand;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;
import com.jcwhatever.nucleus.utils.text.TextUtils;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link ISqlInsert}.
 */
public class Insert implements ISqlInsert {

    private static final StatementSizeTracker _sizeTracker = new StatementSizeTracker(100, 25);

    private final Statement _statement;
    private final Table _table;
    private final Final _final = new Final();
    private final Exists _exists = new Exists();
    private final SetterOperator _setterOperator = new SetterOperator();

    private List<CompoundValue> _compoundValues;
    private boolean _isFinalized;
    private FinalizedStatement _finalized;

    /**
     * Constructor.
     *
     * @param table    The table the statement is for.
     * @param columns  The names of the columns.
     */
    public Insert(Table table, String[] columns, @Nullable Statement statement) {

        PreCon.notNull(table);
        PreCon.notNull(columns);

        _table = table;
        _statement = statement == null
                ? new Statement(table.getDatabase(), _sizeTracker.getSize(), 15)
                : statement;

        _statement.setColumns(columns);
        _statement.setType(StatementType.UPDATE);

        int totalCompound = table.getDefinition().totalCompoundColumns();
        _compoundValues = totalCompound > 0 ? new ArrayList<CompoundValue>(totalCompound) : null;

        statement()
                .append("INSERT INTO `")
                .append(_table.getName())
                .append("` (");

        for (int i = 0; i < columns.length; i++) {

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

            if (i < columns.length - 1)
                statement().append(',');
        }

        statement().append(')');
    }

    @Override
    public Final values(Object... values) {
        PreCon.notNull(values);
        PreCon.isValid(_statement.getColumns().length == 0
                        || _statement.getColumns().length == values.length,
                "Number of values provided does not match number of columns.");
        assertNotFinalized();

        if (statementValues().isEmpty()) {
            statement().append(" VALUES (");
        }
        else {
            statement().append(", (");
        }

        for (int i=0; i < values.length; i++) {

            CompoundValue compoundValue = addValue(i, values[i]);

            if (compoundValue == null) {
                statement().append('?');
            }
            else {
                statement()
                        .append('@')
                        .append(compoundValue.getVariable());
            }

            if (i < values.length - 1)
                statement().append(',');
        }

        statement().append(')');

        return _final;
    }

    @Override
    public <T> Final values(Collection<T> values, ISqlInsertValueConverter<T> valuesConverter) {
        PreCon.notNull(values);
        PreCon.notNull(valuesConverter);

        for (T val : values) {
            Object[] objects = valuesConverter.getRowValues(val);
            values(objects);
        }

        return _final;
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

        Utils.insertCompound(_table, _statement, _compoundValues);
        _sizeTracker.registerSize(_statement.length());
        _finalized = _statement.finalizeStatement(_table);
        _isFinalized = true;
        return _finalized;
    }

    private StringBuilder statement() {
        return _statement.getBuffer();
    }

    private CompoundValue addValue(int columnIndex, @Nullable Object value) {
        String columnName = _statement.getColumns()[columnIndex];
        ISqlTableColumn column = null;
        boolean hasTableName = columnName.indexOf('.') != -1;

        if (!hasTableName || columnName.startsWith(_table.getName())) {

            String name;

            if (hasTableName) {
                String[] nameComponents = TextUtils.PATTERN_DOT.split(columnName);
                name = nameComponents[nameComponents.length - 1];
            }
            else {
                name = columnName;
            }

            column = _table.getDefinition().getColumn(name);
        }

        if (column != null && column.getDataType().isCompound())
            return addCompoundValue(column, value);

        _statement.getValues().add(value);
        return null;
    }

    private CompoundValue addCompoundValue(ISqlTableColumn column, @Nullable Object value) {
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

    private void addValue(Object value) {
        _statement.getValues().add(value);
    }

    private List<Object> statementValues() {
        return _statement.getValues();
    }

    private class Final implements ISqlInsertFinal, ISqlExecutable {

        @Override
        public Exists ifExists() {
            assertNotFinalized();

            statement().append(" ON DUPLICATE KEY UPDATE ");
            return _exists;
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
        public Final selectIdentity() {
            finalizeStatement();

            ISqlTableColumn column = _table.getDefinition().getPrimaryKey();
            if (column == null) {
                throw new IllegalStateException("Table '" + _table.getName()
                        + "' does not have a primary key to select an identity from.");
            }

            _statement.setType(StatementType.QUERY);
            statement()
                    .append("SELECT LAST_INSERT_ID();");

            finalizeStatement(true);
            return this;
        }
    }

    private class Exists implements ISqlInsertExists {

        ISqlTable currentTable;
        ISqlTableColumn currentSetterColumn;
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

        @Override
        public Final selectIdentity() {
            return _final.selectIdentity();
        }
    }

    private class SetterOperator implements ISqlInsertSetterOperator {

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
}
