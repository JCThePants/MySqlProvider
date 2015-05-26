package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.sql.statement.mixins.ISqlLogicalOperator;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Abstract implementation of {@link ISqlLogicalOperator<T>}.
 */
abstract class AbstractConditionOperator<T> implements ISqlLogicalOperator<T> {

    private final Statement _statement;

    /**
     * Constructor.
     *
     * @param statement  The statement the operator applies to.
     */
    public AbstractConditionOperator(Statement statement) {
        _statement = statement;
    }

    /**
     * Invoked to assert that the statement is not yet finalized.
     */
    protected abstract void assertNotFinalized();

    /**
     * Invoked to set the current working column.
     *
     * @param columnName  The name of the column.
     */
    protected abstract void setCurrentColumn(String columnName);

    /**
     * Invoked to get the statement operator handler.
     */
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
