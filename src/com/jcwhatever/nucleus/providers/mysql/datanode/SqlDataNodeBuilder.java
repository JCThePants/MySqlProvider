package com.jcwhatever.nucleus.providers.mysql.datanode;

import com.jcwhatever.nucleus.providers.mysql.datanode.SqlDataNode.SqlNodeValue;
import com.jcwhatever.nucleus.providers.sql.ISqlQueryResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.datanode.ISqlDataNode;
import com.jcwhatever.nucleus.providers.sql.datanode.ISqlDataNodeBuilder;
import com.jcwhatever.nucleus.utils.PreCon;

import org.bukkit.plugin.Plugin;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlDataNodeBuilder}.
 */
public class SqlDataNodeBuilder implements ISqlDataNodeBuilder {

    private final Plugin _plugin;
    private final Map<String, SqlNodeValue> _valueMap;
    private final Setter _setter = new Setter();
    private final EmptySetter _emptySetter = new EmptySetter();

    private ISqlTable _table;
    private ISqlQueryResult _result;
    private Object _pKeyValue;

    /**
     * Constructor.
     *
     * @param plugin  The resulting data nodes owning plugin.
     */
    public SqlDataNodeBuilder(Plugin plugin) {
        PreCon.notNull(plugin);

        _plugin = plugin;
        _valueMap = new HashMap<>(25);
    }

    @Override
    public Setter fromSource(
            ISqlTable table, ISqlQueryResult result, Object pKeyValue) {

        PreCon.notNull(table);
        PreCon.notNull(result);
        PreCon.notNull(pKeyValue);

        setSource(table, result, pKeyValue);

        return _setter;
    }

    @Override
    public EmptySetter withoutSource(ISqlTable table, Object pKeyValue) {
        PreCon.notNull(table);
        PreCon.notNull(pKeyValue);

        setSource(table, null, pKeyValue);

        return _emptySetter;
    }

    private void setSource(
            ISqlTable table, @Nullable ISqlQueryResult result, Object pKeyValue) {
        _table = table;
        _result = result;
        _pKeyValue = pKeyValue;
    }

    private void setAll() throws SQLException {

        String[] columnNames;

        if (_result == null) {
            ISqlTableDefinition definition = _table.getDefinition();
             columnNames = definition.getColumnNames();
        }
        else {
            columnNames = _result.getColumns();
        }

        for (String columnName : columnNames) {
            set(columnName, columnName);
        }
    }

    private void set(String columnName, String nodeName) throws SQLException {
        PreCon.notNullOrEmpty(columnName);
        PreCon.notNull(nodeName);

        Object value = null;

        if (_result != null) {
            value = _result.getObject(columnName);
        }
        else {
            ISqlTableColumn column = _table.getDefinition().getColumn(columnName);
            if (column == null)
                throw new IllegalArgumentException("Column " + columnName + " not found.");

            if (column.hasDefaultValue()) {
                value = column.getDefaultValue();
            }
        }

        SqlNodeValue nodeValue = new SqlNodeValue(
                nodeName, columnName, _pKeyValue, _table, value);

        _valueMap.put(nodeName, nodeValue);
    }

    private void set(int columnIndex, String nodeName) throws SQLException {
        PreCon.notNull(nodeName);
        assert _result != null;

        Object value = _result.getObject(columnIndex);
        String columnName = _result.getColumns()[columnIndex];

        SqlNodeValue nodeValue = new SqlNodeValue(
                nodeName, columnName, _pKeyValue, _table, value);

        _valueMap.put(nodeName, nodeValue);
    }

    private class Setter implements ISqlDataNodeBuilderSetter {

        @Override
        public Setter fromSource(
                ISqlTable table, ISqlQueryResult result, Object pKeyValue) {
            return SqlDataNodeBuilder.this.fromSource(table, result, pKeyValue);
        }

        @Override
        public EmptySetter withoutSource(ISqlTable table, Object pKeyValue) {
            return SqlDataNodeBuilder.this.withoutSource(table, pKeyValue);
        }

        @Override
        public Setter setAll() throws SQLException {
            SqlDataNodeBuilder.this.setAll();
            return this;
        }

        @Override
        public Setter set(String columnName) throws SQLException {
            SqlDataNodeBuilder.this.set(columnName, columnName);
            return this;
        }

        @Override
        public Setter set(String columnName, String nodeName) throws SQLException {
            SqlDataNodeBuilder.this.set(columnName, nodeName);
            return this;
        }

        @Override
        public Setter set(int columnIndex, String nodeName) throws SQLException {
            SqlDataNodeBuilder.this.set(columnIndex, nodeName);
            return this;
        }

        @Override
        public SqlDataNode build() {
            return new SqlDataNode(_plugin, _valueMap);
        }
    }

    private class EmptySetter implements ISqlDataNodeBuilderEmptySetter {

        @Override
        public EmptySetter setAll() {

            try {
                SqlDataNodeBuilder.this.setAll();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return this;
        }

        @Override
        public EmptySetter set(String columnName) {
            return set(columnName, columnName);
        }

        @Override
        public EmptySetter set(String columnName, String nodeName) {

            try {
                SqlDataNodeBuilder.this.set(columnName, nodeName);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return this;
        }

        @Override
        public EmptySetter set(String columnName, Object value) {
            return set(columnName, columnName, value);
        }

        @Override
        public EmptySetter set(String columnName, String nodeName, @Nullable Object value) {
            PreCon.notNull(columnName);
            PreCon.notNull(nodeName);

            SqlNodeValue nodeValue = new SqlNodeValue(
                    nodeName, columnName, _pKeyValue, _table, value);

            _valueMap.put(nodeName, nodeValue);

            return this;
        }

        @Override
        public ISqlDataNode build() {
            return new SqlDataNode(_plugin, _valueMap);
        }

        @Override
        public Setter fromSource(ISqlTable table, ISqlQueryResult result, Object pKeyValue) {
            return SqlDataNodeBuilder.this.fromSource(table, result, pKeyValue);
        }

        @Override
        public EmptySetter withoutSource(ISqlTable table, Object pKeyValue) {
            return SqlDataNodeBuilder.this.withoutSource(table, pKeyValue);
        }
    }
}
