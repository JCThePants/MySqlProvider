package com.jcwhatever.nucleus.providers.mysql.table;

import com.jcwhatever.nucleus.collections.ArrayQueue;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.utils.PreCon;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Implementation of {@link ISqlTableDefinition}.
 */
public class TableDefinition implements ISqlTableDefinition {

    private final ISqlTableColumn[] _columns;
    private final ISqlTableColumn[] _compound;
    private final String[] _columnNames;
    private final ISqlTableColumn _primary;
    private final String _engine;
    private final Map<String, ISqlTableColumn> _map;
    private final boolean _isTempTable;

    /**
     * Constructor.
     *
     * @param columns  The tables column definitions.
     * @param engine   The preferred database engine.
     */
    TableDefinition(ISqlTableColumn[] columns, @Nullable String engine, boolean isTempTable) {
        _columns = columns;
        _engine = engine;
        _map = new HashMap<>(columns.length);
        _columnNames = new String[columns.length];
        _isTempTable = isTempTable;

        ISqlTableColumn primary = null;

        Queue<ISqlTableColumn> compound = new ArrayQueue<>(columns.length);

        for (int i = 0; i < columns.length; i++) {
            ISqlTableColumn column = columns[i];
            if (column.isPrimary()) {
                primary = column;
            }

            if (column.getDataType().isCompound())
                compound.add(column);

            _map.put(column.getName(), column);
            _columnNames[i] = column.getName();
        }

        _primary = primary;
        _compound = compound.toArray(new ISqlTableColumn[compound.size()]);
    }

    @Override
    public int totalColumns() {
        return _columns.length;
    }

    @Override
    public int totalCompoundColumns() {
        return _compound.length;
    }

    @Override
    public boolean hasPrimaryKey() {
        return _primary != null;
    }

    @Override
    public boolean hasCompoundDataTypes() {
        return _compound.length > 0;
    }

    @Override
    public ISqlTableColumn[] getCompoundColumns() {
        return _compound.clone();
    }

    @Override
    public ISqlTableColumn[] getColumns() {
        return _columns.clone();
    }

    @Override
    public String[] getColumnNames() {
        return _columnNames.clone();
    }

    @Nullable
    @Override
    public ISqlTableColumn getColumn(String name) {
        PreCon.notNullOrEmpty(name);

        return _map.get(name);
    }

    @Nullable
    @Override
    public ISqlTableColumn getPrimaryKey() {
        return _primary;
    }

    @Nullable
    @Override
    public String getEngineName() {
        return _engine;
    }

    @Override
    public boolean isTemp() {
        return _isTempTable;
    }
}
