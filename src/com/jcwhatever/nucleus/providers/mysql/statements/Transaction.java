package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.MySqlProvider;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlStatement;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.IFutureResult;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Sql Transaction
 */
public class Transaction extends ArrayList<FinalizedStatements> implements ISqlTransaction {

    private final ISqlDatabase _database;

    public Transaction(ISqlDatabase database) {
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
    public IFutureResult<ISqlResult> execute() {
        return MySqlProvider.getProvider().execute(this);
    }
}
