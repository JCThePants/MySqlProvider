package com.jcwhatever.nucleus.providers.mysql.compound;

import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;

/*
 * 
 */
public class CompoundValue {

    private final String _variable;
    private final ISqlTableColumn _column;
    private final Object _value;

    public CompoundValue(String variable, ISqlTableColumn column, Object value) {
        _variable = variable;
        _column = column;
        _value = value;
    }

    public String getVariable() {
        return _variable;
    }

    public ISqlTableColumn getColumn() {
        return _column;
    }

    public Object getValue() {
        return _value;
    }
}