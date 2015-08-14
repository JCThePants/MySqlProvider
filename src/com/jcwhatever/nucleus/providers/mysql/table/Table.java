package com.jcwhatever.nucleus.providers.mysql.table;

import com.jcwhatever.nucleus.providers.mysql.Database;
import com.jcwhatever.nucleus.providers.mysql.statements.Delete;
import com.jcwhatever.nucleus.providers.mysql.statements.Insert;
import com.jcwhatever.nucleus.providers.mysql.statements.InsertInto;
import com.jcwhatever.nucleus.providers.mysql.statements.Select;
import com.jcwhatever.nucleus.providers.mysql.statements.Statement;
import com.jcwhatever.nucleus.providers.mysql.statements.StatementBuilder;
import com.jcwhatever.nucleus.providers.mysql.statements.Transaction;
import com.jcwhatever.nucleus.providers.mysql.statements.Update;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.statement.generators.IColumnNameGenerator;
import com.jcwhatever.nucleus.providers.sql.statement.insert.ISqlInsert;
import com.jcwhatever.nucleus.providers.sql.statement.insertinto.ISqlInsertInto;
import com.jcwhatever.nucleus.providers.sql.statement.select.ISqlSelect;
import com.jcwhatever.nucleus.utils.PreCon;

import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlTable}.
 */
public class Table implements ISqlTable {

    private final String _name;
    private final Database _database;
    private final ISqlTableDefinition _definition;
    private final Transaction _transaction;

    // temp table fields
    private boolean _isRemoved;
    private boolean _isCreated;

    /**
     * Constructor.
     *
     * @param name         The name of the table.
     * @param database     The tables database.
     * @param definition   The table definition.
     * @param transaction  The transaction the table belongs to (temporary tables). If
     *                     the table is not temporary, use null.
     */
    public Table(String name, Database database, ISqlTableDefinition definition,
                 @Nullable Transaction transaction) {

        PreCon.notNullOrEmpty(name);
        PreCon.notNull(database);
        PreCon.notNull(definition);

        _name = name;
        _database = database;
        _definition = definition;
        _transaction = transaction;
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
        checkRemoved();

        return new StatementBuilder(this, new Statement(_database, 0, 0), null).beginTransaction();
    }

    @Override
    public Select selectRow(String... columns) {
        checkRemoved();

        return new Select(this, columns, null);
    }

    @Override
    public Select selectRows(String... columns) {
        checkRemoved();

        return new Select(this, columns, null);
    }

    @Override
    public ISqlSelect selectRows(IColumnNameGenerator nameGenerator) {
        PreCon.notNull(nameGenerator);
        checkRemoved();

        return new Select(this, nameGenerator.getColumnNames(this), null);
    }

    @Override
    public Update updateRow() {
        checkRemoved();

        return new Update(this, null);
    }

    @Override
    public Update updateRows() {
        checkRemoved();

        return new Update(this, null);
    }

    @Override
    public Insert insertRow(String... columns) {
        checkRemoved();

        return new Insert(this, columns, null);
    }

    @Override
    public Insert insertRows(String... columns) {
        checkRemoved();

        return new Insert(this, columns, null);
    }

    @Override
    public ISqlInsert insertRows(IColumnNameGenerator nameGenerator) {
        PreCon.notNull(nameGenerator);
        checkRemoved();

        return new Insert(this, nameGenerator.getColumnNames(this), null);
    }

    @Override
    public ISqlInsertInto insertInto(ISqlTable table) {
        checkRemoved();

        return new InsertInto(this, table.getName(), null);
    }

    @Override
    public Delete deleteRow() {
        checkRemoved();

        return new Delete(this, null);
    }

    @Override
    public Delete deleteRows() {
        checkRemoved();

        return new Delete(this, null);
    }

    @Nullable
    public Transaction getTransaction() {
        return _transaction;
    }

    /**
     * Used internally if the table is a temporary table to
     * set flag indicating that the table has been removed
     * from the database.
     */
    public void setRemoved() {
        _isRemoved = true;
    }

    /**
     * Used internally if the table is a temporary table to
     * determine if the table is already removed.
     *
     * @throws IllegalStateException if the table is removed.
     */
    public void checkRemoved() {
        if (_isRemoved)
            throw new IllegalStateException("Temporary table '" + _name + "' has already been removed.");
    }

    /**
     * Used internally if the table is a temporary table to
     * set flag indicating that the table has been created.
     */
    public void setCreated() {
        _isCreated = true;
    }

    /**
     * Used internally if the table is a temporary table to
     * determine if the table has already been created.
     */
    public boolean isCreated() {
        return _isCreated;
    }
}
