package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlLogicalOperator;
import com.jcwhatever.nucleus.utils.PreCon;

/*
 * 
 */
abstract class AbstractConditionOperator<T> implements ISqlLogicalOperator<T> {

    private final Statement _statement;

    public AbstractConditionOperator(Statement statement) {
        _statement = statement;
    }

    protected abstract void assertNotFinalized();

    protected abstract void setCurrentColumn(String columnName);

    protected abstract T getOperator();

    @Override
    public T and(String column) {
        PreCon.notNullOrEmpty(column);
        assertNotFinalized();

        statement()
                .append(" AND ")
                .append(column);

        setCurrentColumn(column);

        return getOperator();
    }

    @Override
    public T or(String column) {
        PreCon.notNullOrEmpty(column);
        assertNotFinalized();

        statement()
                .append(" OR ")
                .append(column);

        setCurrentColumn(column);

        return getOperator();
    }

    private StringBuilder statement() {
        return _statement.getBuffer();
    }
}
