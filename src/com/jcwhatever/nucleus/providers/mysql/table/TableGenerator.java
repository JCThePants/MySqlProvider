package com.jcwhatever.nucleus.providers.mysql.table;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.jcwhatever.nucleus.providers.mysql.Database;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataHandler;
import com.jcwhatever.nucleus.providers.mysql.statements.FinalizedStatements;
import com.jcwhatever.nucleus.providers.mysql.statements.Statement;
import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates sql statement to create a table.
 */
public class TableGenerator {

    private final Database _database;
    private String _name;
    private ISqlTableDefinition _definition;

    /**
     * Constructor.
     *
     * @param database    The database the table will be created in.
     * @param tableName   The name of the table.
     * @param definition  The table definition.
     */
    public TableGenerator(Database database, String tableName, ISqlTableDefinition definition) {
        _database = database;
        _name = tableName;
        _definition = definition;
    }

    /**
     * Get finalized statement used to create the table.
     */
    public FinalizedStatements getFinalized() {

        Statement unfinalized = new Statement(_database, 100, 0);

        //noinspection MismatchedQueryAndUpdateOfStringBuilder
        StringBuilder statement = unfinalized.getBuffer();

        statement
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(_name)
                .append(" (");

        ISqlTableColumn[] columns = _definition.getColumns();

        List<ISqlTableColumn> primary = new ArrayList<>(3);
        List<ISqlTableColumn> foreign = new ArrayList<>(3);
        ListMultimap<String, ISqlTableColumn> indexes =
                MultimapBuilder.hashKeys(3).arrayListValues(3).build();

        for (int i = 0; i < columns.length; i++) {
            ISqlTableColumn column = columns[i];

            ISqlDbType type = column.getDataType();

            if (type.isCompound()) {

                ICompoundDataHandler handler = _database.getCompoundManager().getHandler(type);
                if (handler == null) {
                    throw new UnsupportedOperationException("Compound data type '"
                            + type.getName() + "' not supported.");
                }

                ISqlTable handlerTable = handler.getTable();
                ISqlTableColumn handlerPrimary = handlerTable.getDefinition().getPrimaryKey();
                if (handlerPrimary == null) {
                    throw new IllegalStateException("Compound data handler is missing primary " +
                            "key in its table definition.");
                }

                ISqlDbType handlerType = handlerPrimary.getDataType();

                statement
                        .append(column.getName())
                        .append(' ')
                        .append(handlerType.getName());

                if (handlerType.size() > 0) {
                    statement
                            .append('(')
                            .append(handlerType.size())
                            .append(')');
                }

                if (column.hasDefaultValue()) {
                    throw new IllegalArgumentException(
                            "Columns with compound data values cannot have a default value.");
                }

                TableColumnDefinition columnDefinition =
                        new TableColumnDefinition(column.getName(), handlerType);
                columnDefinition._isForeign = true;
                columnDefinition._isCascadeDelete = true;
                columnDefinition._foreignTableName = handlerTable.getName();
                columnDefinition._foreignTablePrimary = handlerPrimary.getName();

                foreign.add(columnDefinition);
            }
            else {
                statement
                        .append(column.getName())
                        .append(' ')
                        .append(type.getName());

                if (type.size() > 0) {
                    statement
                            .append('(')
                            .append(type.size())
                            .append(')');
                }

                // append primary or unique key
                if (column.isPrimary()) {
                    primary.add(column);
                } else if (column.isUnique()) {
                    statement.append(" UNIQUE");
                }

                // append "not null"
                if (!column.isNullable() || column.isPrimary()) {
                    statement.append(" NOT NULL");
                }

                // append auto increment
                if (column.isAutoIncrement()) {
                    statement.append(" AUTO_INCREMENT");

                    if (column.getAutoIncrementStart() > 0) {
                        statement
                                .append('=')
                                .append(column.getAutoIncrementStart());
                    }
                }

                // record foreign key
                if (column.isForeign())
                    foreign.add(column);

                // append default value
                if (column.hasDefaultValue()) {

                    statement.append(" DEFAULT ?");
                    unfinalized.getValues().add(column.getDefaultValue());
                }
            }

            if (i < columns.length - 1)
                statement.append(',');
        }

        // append primary key
        if (!primary.isEmpty()) {
            statement.append(", PRIMARY KEY (");

            for (int i=0; i < primary.size(); i++) {
                statement.append(primary.get(i).getName());

                if (i < primary.size() - 1)
                    statement.append(',');
            }

            statement.append(')');
        }

        // append foreign keys
        for (ISqlTableColumn column : foreign) {

            statement
                    .append(",FOREIGN KEY (")
                    .append(column.getName())
                    .append(") REFERENCES ")
                    .append(column.getForeignTableName())
                    .append('(')
                    .append(column.getForeignTablePrimary())
                    .append(')');

            if (column.isCascadeDelete()) {
                statement.append(" ON DELETE CASCADE");
            }
        }

        // append indexes
        for (String indexName : indexes.keySet()) {

            List<ISqlTableColumn> indexColumns = indexes.get(indexName);
            if (indexColumns.isEmpty())
                continue;

            statement.append(",INDEX (");

            for (int i=0; i < indexColumns.size(); i++) {

                statement.append(indexColumns.get(i).getName());

                if (i < indexColumns.size() - 1)
                    statement.append(',');
            }

            statement.append(')');
        }

        statement.append(')');

        if (_definition.getEngineName() != null) {
            statement
                    .append(" ENGINE ")
                    .append(_definition.getEngineName());
        }

        unfinalized.finalizeStatement(_database.getConnection());

        return unfinalized.getFinalized();
    }
}
