package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.providers.mysql.compound.CompoundDataManager;
import com.jcwhatever.nucleus.providers.mysql.compound.CompoundValue;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataHandler;
import com.jcwhatever.nucleus.providers.mysql.compound.ICompoundDataIterator;
import com.jcwhatever.nucleus.providers.mysql.statements.Statement;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.sql.ISqlDbType;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods.
 */
public class Utils {

    private Utils() {}

    /**
     * Append values for a compound data type to an insert statement.
     *
     * @param table           The table the statement is for.
     * @param unfinalized     The statement.
     * @param compoundValues  The compound values to be appended.
     */
    public static void insertCompound(
            Table table, Statement unfinalized, List<CompoundValue> compoundValues) {

        if (compoundValues == null || compoundValues.isEmpty())
            return;

        StringBuilder statement = unfinalized.getBuffer();
        List<Object> bufferedValues = new ArrayList<>(unfinalized.getValues());
        List<Object> values = unfinalized.getValues();
        values.clear();

        StringBuilder buffer = TempBuffers.STRING_BUILDERS.get();
        buffer.setLength(0);

        buffer.append(statement);
        statement.setLength(0);

        CompoundDataManager manager = table.getDatabase().getCompoundManager();

        for (CompoundValue compound : compoundValues) {

            ISqlDbType dataType = compound.getColumn().getDataType();

            ICompoundDataHandler handler = manager.getHandler(dataType);
            if (handler == null) {
                throw new UnsupportedOperationException("Data type not supported: "
                        + dataType.getName());
            }

            ISqlTable handlerTable = handler.getTable();
            ISqlTableColumn primary = handlerTable.getDefinition().getPrimaryKey();
            if (primary == null) {
                throw new IllegalStateException("Primary key missing from compound table: "
                        + handlerTable.getName());
            }

            statement
                    .append("INSERT INTO ")
                    .append(handlerTable.getName())
                    .append(" (");

            boolean hasColumns = false;
            for (ISqlTableColumn column : handlerTable.getDefinition().getColumns()) {

                if (column.isAutoIncrement())
                    continue;

                if (hasColumns)
                    statement.append(',');

                hasColumns = true;

                statement.append(column.getName());
            }

            statement.append(") VALUES (");

            ICompoundDataIterator iterator = handler.dataIterator(compound.getValue());
            while (iterator.next()) {

                if (iterator.currentIndex() != 0)
                    statement.append(',');

                statement.append('?');

                values.add(iterator.getValue());
            }
            statement.append(");");

            unfinalized.finalizeStatement(table.getDatabase().getConnection());

            statement
                    .append("SET @")
                    .append(compound.getVariable())
                    .append("=LAST_INSERT_ID();");

            unfinalized.finalizeStatement(table.getDatabase().getConnection());
        }

        values.addAll(bufferedValues);
        statement.append(buffer);
    }
}
