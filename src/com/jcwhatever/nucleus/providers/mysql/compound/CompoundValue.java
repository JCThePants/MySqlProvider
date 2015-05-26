package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;

/**
 * A compound data type value.
 */
public class CompoundValue {

    private final String _variable;
    private final ISqlTableColumn _column;
    private final Object _value;

    /**
     * Constructor.
     *
     * @param variable  The name of the SQL variable that holds the value.
     * @param column    The name of the column the value is for.
     * @param value     The value.
     */
    public CompoundValue(String variable, ISqlTableColumn column, Object value) {
        _variable = variable;
        _column = column;
        _value = value;
    }

    /**
     * Get the SQL variable that holds the value.
     */
    public String getVariable() {
        return _variable;
    }

    /**
     * Get the table column the value is for.
     */
    public ISqlTableColumn getColumn() {
        return _column;
    }

    /**
     * Get the value.
     */
    public Object getValue() {
        return _value;
    }
}