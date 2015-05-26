package com.jcwhatever.nucleus.providers.mysql.table;

import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlTableColumn}.
 */
public class TableColumnDefinition implements ISqlTableColumn {

    private final String _name;
    private final ISqlDbType _type;

    boolean _isPrimary;
    boolean _isUnique;
    boolean _isForeign;
    boolean _isAutoIncrement;
    boolean _isNullable;
    boolean _isCascadeDelete;
    String _foreignTableName;
    String _foreignTablePrimary;
    long _incrementStart = -1;
    List<String> _indexes = new ArrayList<>(3);
    boolean _hasDefaultValue;
    Object _defaultValue;

    /**
     * Constructor.
     *
     * @param name  The name of the column.
     * @param type  The column data type.
     */
    TableColumnDefinition(String name, ISqlDbType type) {
        _name = name;
        _type = type;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public ISqlDbType getDataType() {
        return _type;
    }

    @Override
    public boolean isPrimary() {
        return _isPrimary;
    }

    @Override
    public boolean isUnique() {
        return _isUnique;
    }

    @Override
    public boolean isForeign() {
        return _isForeign;
    }

    @Override
    public boolean isNullable() {
        return _isNullable;
    }

    @Override
    public boolean isAutoIncrement() {
        return _isAutoIncrement;
    }

    @Override
    public boolean hasDefaultValue() {
        return _hasDefaultValue;
    }

    @Nullable
    @Override
    public String getForeignTableName() {
        return _foreignTableName;
    }

    @Nullable
    @Override
    public String getForeignTablePrimary() {
        return _foreignTablePrimary;
    }

    @Override
    public boolean isCascadeDelete() {
        return _isCascadeDelete;
    }

    @Override
    public long getAutoIncrementStart() {
        return _incrementStart;
    }

    @Override
    public Collection<String> getIndexNames() {
        return CollectionUtils.unmodifiableList(_indexes);
    }

    @Override
    public Object getDefaultValue() {
        return _defaultValue;
    }
}
