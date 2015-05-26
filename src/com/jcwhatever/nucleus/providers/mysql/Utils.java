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

import java.util.List;
import java.util.Queue;

/*
 * 
 */
public class Utils {

    private Utils() {}

    public static void appendCompound(Table table, Statement unfinalizedStatement, List<CompoundValue> compoundValues) {

        if (compoundValues == null || compoundValues.isEmpty())
            return;

        StringBuilder statement = unfinalizedStatement.getBuffer();
        List<Object> values = unfinalizedStatement.getValues();

        StringBuilder buffer = TempBuffers.STRING_BUILDERS.get();
        Queue<Object> valueBuffer = TempBuffers.VALUES.get();

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
                    .append(" VALUES (");

            ICompoundDataIterator iterator = handler.dataIterator(compound.getValue());
            while (iterator.next()) {

                if (iterator.currentIndex() != 0)
                    statement.append(',');

                statement.append('?');

                valueBuffer.add(iterator.getValue());
            }
            statement.append(");");

            statement
                    .append("SET @")
                    .append(compound.getVariable())
                    .append("=0;");

            statement
                    .append("SELECT @")
                    .append(compound.getVariable())
                    .append(":=LAST_INSERT_ID();");
        }

        valueBuffer.addAll(values);
        values.clear();
        values.addAll(valueBuffer);

        statement.append(buffer);
    }

}