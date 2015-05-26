package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlOperator;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.Rand;
import com.jcwhatever.nucleus.utils.converters.IConverter;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/*
 * 
 */
 abstract class AbstractOperator<T> implements ISqlOperator<T> {

    String currentColumn;
    private final Statement _statement;
    private final Table _table;
    private final List<CompoundValue> _compoundValues;

    public AbstractOperator(Statement statement) {
        PreCon.notNull(statement);

        _statement = statement;
        _table = null;
        _compoundValues = null;
    }

    public AbstractOperator(Statement statement, Table table,
                            List<CompoundValue> compound) {
        _statement = statement;
        _table = table;
        _compoundValues = compound;
    }

    protected abstract void assertNotFinalized();

    protected abstract T getConditionOperator();

    @Override
    public T isEqualTo(@Nullable Object value) {
        return singleValue("=", value);
    }

    @Override
    public <T1> T isEqualToAny(Collection<T1> values) {
        return anyValue("=", values, null);
    }

    @Override
    public <T1, T2> T isEqualToAny(Collection<T1> values, IConverter<T1, T2> converter) {
        return anyValue("=", values, converter);
    }

    @Override
    public T isEqualToColumn(String columnName) {
        return singleColumn("=", columnName);
    }

    @Override
    public T isEqualToAnyColumn(String... columnNames) {
        return anyColumn("=", columnNames);
    }

    @Override
    public T isNotEqualTo(@Nullable Object value) {
        return singleValue("!=", value);
    }

    @Override
    public <T1> T isNotEqualToAll(Collection<T1> values) {
        return allValues("!=", values, null);
    }

    @Override
    public <T1, T2> T isNotEqualToAll(Collection<T1> values, IConverter<T1, T2> converter) {
        return allValues("!=", values, null);
    }

    @Override
    public T isNotEqualToColumn(String columnName) {
        return singleColumn("!=", columnName);
    }

    @Override
    public T isNotEqualToAllColumns(String... columnNames) {
        return allColumns("!=", columnNames);
    }

    @Override
    public T isGreaterThan(@Nullable Object value) {
        return singleValue(">", value);
    }

    @Override
    public <T1> T isGreaterThanAny(Collection<T1> values) {
        return anyValue(">", values, null);
    }

    @Override
    public <T1> T isGreaterThanAll(Collection<T1> values) {
        return allValues(">", values, null);
    }

    @Override
    public <T1, T2> T isGreaterThanAny(Collection<T1> values, IConverter<T1, T2> converter) {
        return anyValue(">", values, converter);
    }

    @Override
    public <T1, T2> T isGreaterThanAll(Collection<T1> values, IConverter<T1, T2> converter) {
        return allValues(">", values, converter);
    }

    @Override
    public T isGreaterThanColumn(String columnName) {
        return singleColumn(">", columnName);
    }

    @Override
    public T isGreaterThanAnyColumn(String... columnNames) {
        return anyColumn(">", columnNames);
    }

    @Override
    public T isGreaterThanAllColumns(String... columnNames) {
        return allColumns(">", columnNames);
    }

    @Override
    public T isGreaterOrEqualTo(Object value) {
        return singleValue(">=", value);
    }

    @Override
    public <T1> T isGreaterOrEqualToAny(Collection<T1> values) {
        return anyValue(">=", values, null);
    }

    @Override
    public <T1> T isGreaterOrEqualToAll(Collection<T1> values) {
        return allValues(">=", values, null);
    }

    @Override
    public <T1, T2> T isGreaterOrEqualToAny(Collection<T1> values, IConverter<T1, T2> converter) {
        return anyValue(">=", values, converter);
    }

    @Override
    public <T1, T2> T isGreaterOrEqualToAll(Collection<T1> values, IConverter<T1, T2> converter) {
        return allValues(">=", values, converter);
    }

    @Override
    public T isGreaterOrEqualToColumn(String columnName) {
        return singleColumn(">=", columnName);
    }

    @Override
    public T isGreaterOrEqualToAnyColumn(String... columnNames) {
        return anyColumn(">=", columnNames);
    }

    @Override
    public T isGreaterOrEqualToAllColumns(String... columnNames) {
        return allColumns(">=", columnNames);
    }

    @Override
    public T isLessThan(@Nullable Object value) {
        return singleValue("<", value);
    }

    @Override
    public <T1> T isLessThanAny(Collection<T1> values) {
        return anyValue("<", values, null);
    }

    @Override
    public <T1> T isLessThanAll(Collection<T1> values) {
        return allValues("<", values, null);
    }

    @Override
    public <T1, T2> T isLessThanAny(Collection<T1> values, IConverter<T1, T2> converter) {
        return anyValue("<", values, converter);
    }

    @Override
    public <T1, T2> T isLessThanAll(Collection<T1> values, IConverter<T1, T2> converter) {
        return allValues("<", values, converter);
    }

    @Override
    public T isLessThanColumn(String columnName) {
        return singleColumn("<", columnName);
    }

    @Override
    public T isLessThanAnyColumn(String... columnNames) {
        return anyColumn("<", columnNames);
    }

    @Override
    public T isLessThanAllColumns(String... columnNames) {
        return allColumns("<", columnNames);
    }

    @Override
    public T isLessOrEqualTo(@Nullable Object value) {
        return singleValue("<=", value);
    }

    @Override
    public <T1> T isLessOrEqualToAny(Collection<T1> values) {
        return anyValue("<=", values, null);
    }

    @Override
    public <T1> T isLessOrEqualToAll(Collection<T1> values) {
        return allValues("<=", values, null);
    }

    @Override
    public <T1, T2> T isLessOrEqualToAny(Collection<T1> values, IConverter<T1, T2> converter) {
        return anyValue("<=", values , converter);
    }

    @Override
    public <T1, T2> T isLessOrEqualToAll(Collection<T1> values, IConverter<T1, T2> converter) {
        return allValues("<=", values, converter);
    }

    @Override
    public T isLessOrEqualToColumn(String columnName) {
        return singleColumn("<=", columnName);
    }

    @Override
    public T isLessOrEqualToAnyColumn(String... columnNames) {
        return anyColumn("<=", columnNames);
    }

    @Override
    public T isLessOrEqualToAllColumns(String... columnNames) {
        return allColumns("<=", columnNames);
    }

    private T singleValue(String operator, @Nullable Object value) {
        assertNotFinalized();

        statement().append(operator);
        values(value);
        return getConditionOperator();
    }

    private T singleColumn(String operator, String columnName) {
        PreCon.notNullOrEmpty(columnName);
        assertNotFinalized();

        statement()
                .append(operator)
                .append(columnName);

        return getConditionOperator();
    }

    private <T1, T2> T anyValue(
            String operator, Collection<T1> values, @Nullable IConverter<T1, T2> converter) {

        return multiValues(operator, " OR ", values, converter);
    }

    private <T1, T2> T allValues(
            String operator, Collection<T1> values, @Nullable IConverter<T1, T2> converter) {

        return multiValues(operator, " AND ", values, converter);
    }

    private T anyColumn(String operator, String[] columnNames) {
        return multiColumn(operator, " OR ", columnNames);
    }
    private T allColumns(String operator, String[] columnNames) {
        return multiColumn(operator, " AND ", columnNames);
    }

    private <T1, T2> T multiValues(String operator, String conditionOperator,
                                   Collection<T1> values, @Nullable IConverter<T1, T2> converter) {

        PreCon.notNull(values);
        PreCon.greaterThanZero(values.size());
        assertNotFinalized();

        statement().append('(');

        int count = 0;
        for (T1 value : values) {

            if (count != 0) {
                statement()
                        .append(conditionOperator)
                        .append(currentColumn);
            }

            statement().append(operator);
            values(converter == null ? value : converter.convert(value));
            count++;
        }

        statement().append(')');

        return getConditionOperator();
    }

    private T multiColumn(String operator, String conditionOperator, String[] columnNames) {
        PreCon.notNull(columnNames);
        PreCon.greaterThanZero(columnNames.length);
        assertNotFinalized();

        statement().append('(');

        for (int i=0; i < columnNames.length; i++) {

            if (i != 0) {
                statement()
                        .append(conditionOperator)
                        .append(currentColumn);
            }

            statement()
                    .append(operator)
                    .append(columnNames[i]);
        }

        statement().append(')');

        return getConditionOperator();
    }

    private StringBuilder statement() {
        return _statement.getBuffer();
    }

    private void values(@Nullable Object value) {

        if (_table == null) {
            statement().append('?');
            values().add(value);
        }
        else {

            ISqlTableColumn column = _table.getDefinition().getColumn(currentColumn);
            if (column == null) {
                throw new IllegalArgumentException("A column named" + currentColumn +
                        " is not defined in table " + _table.getName());
            }

            CompoundValue compoundValue = null;

            if (value != null && _compoundValues != null && column.getDataType().isCompound()) {
                compoundValue = new CompoundValue(Rand.getSafeString(10), column, value);
                _compoundValues.add(compoundValue);
            }

            if (compoundValue != null) {
                statement()
                        .append('@')
                        .append(compoundValue.getVariable());
            } else {
                statement().append('?');
                values().add(value);
            }
        }

    }

    private List<Object> values() {
        return _statement.getValues();
    }
}
