package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.providers.mysql.Msg;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlQueryResult;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultAgent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A list of {@link FinalizedStatement}'s to be executed.
 */
public class FinalizedStatements extends ArrayList<FinalizedStatement> {

    private final ISqlDatabase _database;
    private final FutureResultAgent<ISqlResult> _agent = new FutureResultAgent<>();

    /**
     * Constructor.
     *
     * <p>Initial capacity of 0.</p>
     *
     * @param database  The database the statements are for.
     */
    public FinalizedStatements(ISqlDatabase database) {
        super(0);

        _database = database;
    }

    /**
     * Constructor.
     *
     * @param database  The database the statements are for.
     * @param capacity  The initial capacity of the list.
     */
    public FinalizedStatements(ISqlDatabase database, int capacity) {
        super(capacity);

        _database = database;
    }

    /**
     * Constructor.
     *
     * @param database    The database the statements are for.
     * @param statements  The initial statements.
     */
    public FinalizedStatements(ISqlDatabase database, Collection<FinalizedStatement> statements) {
        super(statements);

        _database = database;
    }

    /**
     * Get the database the statements in the list are for.
     */
    public ISqlDatabase getDatabase() {
        return _database;
    }

    /**
     * Get a future result agent used when the statements are executed.
     */
    public FutureResultAgent<ISqlResult> getAgent() {
        return _agent;
    }

    @Override
    public boolean add(FinalizedStatement statement) {
        PreCon.notNull(statement);

        return super.add(statement);
    }

    /**
     * Execute all statements in the list on the current thread.
     *
     * @param transactionDepth  The initial transaction depth. Transaction is only
     *                          started and ended at 0.
     *
     * @return  The result of the execution.
     *
     * @throws SQLException
     */
    public ExecuteResult executeNow(int transactionDepth) throws SQLException {

        ExecuteResult result = new ExecuteResult(size(), _agent);

        Collection<FinalizedStatement> statements = getStatements();

        for (FinalizedStatement statement : statements) {

            switch (statement.getType()) {

                case QUERY: {
                    try {
                        PreparedStatement prepared = statement.prepareStatement();
                        ResultSet resultSet = prepared.executeQuery();
                        result.addResult(new StatementResult(statement, resultSet));
                    }
                    catch(SQLException e) {
                        displaySqlError(statement);
                        rollback(statement.getConnection());
                        throw e;
                    }
                    break;
                }

                case UPDATE: {
                    try {
                        PreparedStatement prepared = statement.prepareStatement();
                        result.setRowsUpdated(prepared.executeUpdate());
                    }
                    catch(SQLException e) {
                        displaySqlError(statement);
                        rollback(statement.getConnection());
                        throw e;
                    }
                    break;
                }

                case TRANSACTION_START: {
                    transactionDepth++;
                    statement.getConnection().setAutoCommit(false);
                    break;
                }

                case TRANSACTION_COMMIT: {
                    transactionDepth--;
                    if (transactionDepth == 0) {
                        statement.getConnection().commit();
                        statement.getConnection().setAutoCommit(true);
                    }
                    break;
                }
            }
        }

        return result;
    }

    private void rollback(Connection connection) throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    private void displaySqlError(FinalizedStatement statement) {

        Object[] values = statement.getValues();

        StringBuilder sb = new StringBuilder(100 + statement.getValues().length * 35);

        sb.append("Error while executing MySQL statement:\n");
        sb.append("   SQL: ").append(statement).append('\n');

        for (int i=0; i < values.length; i++) {
            sb.append("      [").append(i).append("] ");

            if (values[i] == null) {
                sb.append("<null>\n");
            }
            else {
                sb.append(values[i])
                        .append(" (")
                        .append(values[i].getClass().getSimpleName())
                        .append(".class)\n");
            }
        }

        Msg.warning(sb.toString());
    }

    protected Collection<FinalizedStatement> getStatements() {
        return this;
    }

    /**
     * Implementation of {@link ISqlResult}.
     */
    public static class ExecuteResult implements ISqlResult {

        private final List<ISqlQueryResult> results;
        private final List<Integer> rowsUpdated;
        private final FutureResultAgent<ISqlResult> agent;

        /**
         * Constructor.
         *
         * @param size   The expected size of the results.
         * @param agent  The result agent for the results.
         */
        ExecuteResult(int size, FutureResultAgent<ISqlResult> agent) {
            this.results = new ArrayList<>(size);
            this.rowsUpdated = new ArrayList<>(size);
            this.agent = agent;
        }

        /**
         * Get the future result agent.
         */
        public FutureResultAgent<ISqlResult> getAgent() {
            return agent;
        }

        @Override
        public boolean hasQueryResults() {
            return results.size() > 0;
        }

        @Override
        public boolean hasUpdatedRows() {
            return rowsUpdated.size() > 0;
        }

        @Nullable
        @Override
        public ISqlQueryResult getFirstResult() {
            if (results.size() == 0)
                return null;

            return results.get(0);
        }

        @Override
        public int getFirstRowUpdate() {
            if (rowsUpdated.size() == 0)
                return 0;

            return rowsUpdated.get(0);
        }

        @Override
        public ISqlQueryResult[] getQueryResults() {
            return results.toArray(new ISqlQueryResult[results.size()]);
        }

        @Override
        public int[] getRowsUpdated() {
            int[] r = new int[rowsUpdated.size()];

            for (int i=0; i < rowsUpdated.size(); i++) {
                r[i] = rowsUpdated.get(i);
            }
            return r;
        }

        @Override
        public void closeQueryResults() {

            for (ISqlQueryResult result : results) {

                if (result != null) {
                    try {
                        result.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        void addResult(ISqlQueryResult result) {
            results.add(result);
        }

        void setRowsUpdated(int total) {
            rowsUpdated.add(total);
        }

        void addResults(ExecuteResult result) {

            results.addAll(result.results);
            rowsUpdated.addAll(result.rowsUpdated);
        }
    }
}
