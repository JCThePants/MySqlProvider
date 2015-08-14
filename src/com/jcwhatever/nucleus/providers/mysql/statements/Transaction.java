package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.Database;
import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.mysql.table.Table;
import com.jcwhatever.nucleus.providers.mysql.table.TableGenerator;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.utils.CollectionUtils;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultAgent;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link ISqlTransaction}.
 *
 * <p>Ensures a collection of {@link FinalizedStatements} are executed as
 * a transaction. Dependent upon the affected tables supporting transactions.</p>
 */
public class Transaction extends ArrayList<FinalizedStatements> implements ISqlTransaction {

    private final Database _database;
    private final FutureResultAgent<ISqlResult> _agent = new FutureResultAgent<>();
    private List<Table> _tempTables;

    /**
     * Constructor.
     *
     * @param database  The database the statements are for.
     */
    public Transaction(Database database) {
        super(10);

        PreCon.notNull(database);

        _database = database;
    }

    @Override
    public ISqlDatabase getDatabase() {
        return _database;
    }

    @Override
    public IFutureResult<ISqlResult> append(ISqlStatement statement) {
        PreCon.isValid(statement instanceof FinalizedStatement, "Invalid implementation.");

        FinalizedStatements finStatements = new FinalizedStatements(_database, 1);
        finStatements.add((FinalizedStatement)statement);

        add(finStatements);

        return finStatements.getAgent().getFuture();
    }

    @Override
    public IFutureResult<ISqlResult> append(Collection<? extends ISqlStatement> statements) {
        PreCon.notNull(statements);

        FinalizedStatements finStatements;

        if (statements instanceof FinalizedStatements) {

            finStatements = (FinalizedStatements) statements;
        }
        else {

            finStatements = new FinalizedStatements(_database, statements.size());

            for (ISqlStatement statement : statements) {

                if (!(statement instanceof FinalizedStatement))
                    throw new IllegalArgumentException("Invalid statement implementation");

                finStatements.add((FinalizedStatement)statement);
            }
        }

        add(finStatements);
        return finStatements.getAgent().getFuture();
    }

    @Override
    public ISqlTable createTempTable(String name, ISqlTableDefinition definition) {

        if (_tempTables == null)
            _tempTables = new ArrayList<>(5);

        Table table = new Table(name, _database, definition, this);
        _tempTables.add(table);

        TableGenerator generator = new TableGenerator(_database, name, definition);
        append(generator.getFinalized());

        return table;
    }

    @Override
    public IFutureResult<ISqlResult> execute() {
        return MySqlProvider.getProvider().execute(this, _agent);
    }

    @Override
    public IFutureResult<ISqlResult> future() {
        return _agent.getFuture();
    }

    public Collection<Table> getTempTables() {
        if (_tempTables == null)
            return CollectionUtils.unmodifiableList(Table.class);

        return _tempTables;
    }
}
