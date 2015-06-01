package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.providers.mysql.compound.CompoundDataManager;
import com.jcwhatever.nucleus.providers.mysql.statements.Transaction;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.mysql.table.TableBuilder;
import com.jcwhatever.nucleus.providers.mysql.table.TableGenerator;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultAgent;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultSubscriber;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;
import com.jcwhatever.nucleus.utils.observer.future.Result;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ISqlDatabase}.
 */
public class Database implements ISqlDatabase {

    private final String _name;
    private final MySqlConnection _connection;
    private final CompoundDataManager _compoundManager;
    private final Map<String, ISqlTable> _tableMap = new HashMap<>(20);

    /**
     * Constructor.
     *
     * @param address   The database address.
     * @param name      The name of the database to connect to.
     * @param user      The user name to connect with.
     * @param password  The user password to connect with.
     */
    public Database(String address, String name, String user, String password) {

        _name = name;
        _connection = new MySqlConnection(address, name, user, password);
        _compoundManager = new CompoundDataManager(this);
    }

    /**
     * Get the compound data manager.
     */
    public CompoundDataManager getCompoundManager() {
        return _compoundManager;
    }

    @Override
    public boolean isLoaded() {
        return _compoundManager.isLoaded();
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public MySqlConnection getConnection() {
        return _connection;
    }

    @Override
    public TableBuilder createTableBuilder() {
        return new TableBuilder();
    }

    @Override
    public IFutureResult<ISqlTable> createTable(final String name, final ISqlTableDefinition definition) {
        PreCon.notNull(name);
        PreCon.notNull(definition);

        final FutureResultAgent<ISqlTable> agent = new FutureResultAgent<>();

        ISqlTable table = _tableMap.get(name);
        if (table != null)
            return agent.success(table);

        TableGenerator generator = new TableGenerator(this, name, definition);

        MySqlProvider.getProvider().execute(generator.getFinalized())
                .onSuccess(new FutureResultSubscriber<ISqlResult>() {
                    @Override
                    public void on(Result<ISqlResult> result) {
                        Table table = new Table(name, Database.this, definition);
                        _tableMap.put(name, table);
                        agent.success(table);
                    }
                });

        return agent.getFuture();
    }

    @Nullable
    @Override
    public ISqlTable getTable(String name) {
        return _tableMap.get(name);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return _connection.prepareStatement(sql);
    }

    @Override
    public ISqlTransaction createTransaction() {
        return new Transaction(this);
    }
}
