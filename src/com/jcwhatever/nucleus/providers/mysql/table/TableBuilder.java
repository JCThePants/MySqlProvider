package com.jcwhatever.nucleus.providers.mysql.table;

import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.providers.sql.ISqlTableBuilder;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.utils.PreCon;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Implementation of {@link ISqlTableBuilder}.
 */
public class TableBuilder implements ISqlTableBuilder {

    private final Queue<TableColumnDefinition> _columns = new ArrayDeque<>(10);
    private final Transact _transact = new Transact();
    private final Columns _columnConstraints = new Columns();
    private final Constraints _constraints = new Constraints();
    private final PrimaryKey _primary = new PrimaryKey();
    private final Foreign _foreign = new Foreign();
    private final Final _final = new Final();

    private TableColumnDefinition _column;
    private boolean _usageReadInsert;
    private boolean _usageTemp;
    private String _engine;

    @Override
    public Transact usageReadInsert() {
        _usageReadInsert = true;
        return _transact;
    }

    @Override
    public Transact usageReadInsertUpdate() {
        return _transact;
    }

    @Override
    public Columns usageTemporary() {
        _usageTemp = true;
        return _columnConstraints;
    }

    @Override
    public Columns setEngine(String engineName) {
        _engine = engineName;
        return _columnConstraints;
    }

    @Override
    public Columns defaultEngine() {
        return _columnConstraints;
    }

    private class Transact implements ISqlTableBuilderTransact {

        @Override
        public Columns transactional() {
            _engine = "InnoDB";
            return _columnConstraints;
        }

        @Override
        public Columns nonTransactional() {
            _engine = _usageReadInsert ? "MyISAM" : "InnoDB";
            return _columnConstraints;
        }
    }

    private class Columns implements ISqlTableBuilderColumns {

        @Override
        public Constraints column(String name, ISqlDbType type) {
            PreCon.notNullOrEmpty(name);
            PreCon.notNull(type);

            if (_column != null)
                _columns.add(_column);

            _column = new TableColumnDefinition(name, type);
            return _constraints;
        }
    }

    private class PrimaryKey implements ISqlTableBuilderPrimaryKey {

        @Override
        public Constraints column(String name, ISqlDbType type) {
            return _columnConstraints.column(name, type);
        }

        @Override
        public Final autoIncrement() {
            _column._isAutoIncrement = true;
            _column._incrementStart = 0;
            return _final;
        }

        @Override
        public TableDefinition define() {
            return _final.define();
        }
    }

    private class Constraints implements
            ISqlTableBuilderConstraints, ISqlTableBuilderIndexFinal, ISqlTableBuilderDefaultValueFinal {

        @Override
        public Constraints column(String name, ISqlDbType type) {
            return _columnConstraints.column(name, type);
        }

        @Override
        public PrimaryKey primary() {
            PreCon.isValid(!_column.isUnique(), "Cannot make unique key primary. Specify primary only.");
            PreCon.isValid(!_column.isForeign(), "Cannot make foreign key primary.");
            PreCon.isValid(!_column.isNullable(), "Cannot make a nullable column primary.");

            _column._isPrimary = true;
            return _primary;
        }

        @Override
        public Final unique() {
            PreCon.isValid(!_column.isPrimary(), "Primary key is already unique.");
            PreCon.isValid(!_column.isForeign(), "Foreign key cannot be made unique.");
            PreCon.isValid(!_column.isNullable(), "Cannot make a nullable column into a unique key.");

            _column._isUnique = true;
            return _final;
        }

        @Override
        public Foreign foreign(String tableName, String primaryKey) {
            PreCon.notNullOrEmpty(tableName);
            PreCon.notNullOrEmpty(primaryKey);
            PreCon.isValid(!_column.isPrimary(), "Primary key cannot be made foreign.");
            PreCon.isValid(!_column.isUnique(), "Unique key cannot be made foreign.");
            PreCon.isValid(!_column.isNullable(), "Cannot make a nullable column into a foreign key.");

            _column._isForeign = true;
            _column._foreignTableName = tableName;
            _column._foreignTablePrimary = primaryKey;
            return _foreign;
        }

        @Override
        public Constraints index(String... indexNames) {
            PreCon.notNull(indexNames);
            PreCon.greaterThanZero(indexNames.length);

            for (String name : indexNames) {
                PreCon.notNullOrEmpty(name);

                _column._indexes.add(name);
            }

            return this;
        }

        @Override
        public Final nullable() {
            PreCon.isValid(!_column.isPrimary(), "Primary key cannot be made nullable.");
            PreCon.isValid(!_column.isUnique(), "Unique key cannot be made nullable.");
            PreCon.isValid(!_column.isForeign(), "Foreign key cannot be made nullable.");

            _column._isNullable = true;
            return _final;
        }

        @Override
        public TableDefinition define() {
            return _final.define();
        }

        @Override
        public Constraints defaultValue(@Nullable Object value) {
            PreCon.isValid(!_column.isUnique(), "Unique key columns cannot have a default value.");
            PreCon.isValid(!_column.isPrimary(), "Primary key columns cannot have a default value.");

            _column._hasDefaultValue = true;
            _column._defaultValue = value;
            return this;
        }
    }

    private class Foreign implements ISqlTableBuilderForeign {

        @Override
        public Final cascadeDelete() {
            _column._isCascadeDelete = true;
            return _final;
        }

        @Override
        public TableDefinition define() {
            return _final.define();
        }

        @Override
        public Constraints column(String name, ISqlDbType type) {
            return _final.column(name, type);
        }
    }

    private class Final implements ISqlTableBuilderFinal {

        @Override
        public Constraints column(String name, ISqlDbType type) {
            return _columnConstraints.column(name, type);
        }

        @Override
        public TableDefinition define() {

            if (_column != null)
                _columns.add(_column);

            _column = null;

            ISqlTableColumn[] columns = _columns.toArray(new ISqlTableColumn[_columns.size()]);
            return new TableDefinition(columns, _engine, _usageTemp);
        }
    }
}
