package com.jcwhatever.nucleus.providers.mysql.table;

import com.jcwhatever.nucleus.providers.mysql.Database;
import com.jcwhatever.nucleus.providers.mysql.statements.Delete;
import com.jcwhatever.nucleus.providers.mysql.statements.Insert;
import com.jcwhatever.nucleus.providers.mysql.statements.Select;
import com.jcwhatever.nucleus.providers.mysql.statements.Statement;
import com.jcwhatever.nucleus.providers.mysql.statements.StatementBuilder;
import com.jcwhatever.nucleus.providers.mysql.statements.Update;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Implementation of {@link ISqlTable}.
 */
public class Table implements ISqlTable {

    private final String _name;
    private final Database _database;
    private final ISqlTableDefinition _definition;

    /**
     * Constructor.
     *
     * @param name        The name of the table.
     * @param database    The tables database.
     * @param definition  The table definition.
     */
    public Table(String name, Database database, ISqlTableDefinition definition) {
        PreCon.notNullOrEmpty(name);
        PreCon.notNull(database);
        PreCon.notNull(definition);

        _name = name;
        _database = database;
        _definition = definition;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Database getDatabase() {
        return _database;
    }

    @Override
    public ISqlTableDefinition getDefinition() {
        return _definition;
    }

    @Override
    public StatementBuilder beginTransaction() {
        return new StatementBuilder(this, new Statement(_database, 0, 0), null).beginTransaction();
    }

    @Override
    public Select selectRow(String... columns) {
        return new Select(this, columns, null);
    }

    @Override
    public Select selectRows(String... columns) {
        return new Select(this, columns, null);
    }

    @Override
    public Update updateRow() {
        return new Update(this, null);
    }

    @Override
    public Update updateRows() {
        return new Update(this, null);
    }

    @Override
    public Insert insertRow(String... columns) {
        return new Insert(this, columns, null);
    }

    @Override
    public Insert insertRows(String... columns) {
        return new Insert(this, columns, null);
    }

    @Override
    public Delete deleteRow() {
        return new Delete(this, null);
    }

    @Override
    public Delete deleteRows() {
        return new Delete(this, null);
    }
}
