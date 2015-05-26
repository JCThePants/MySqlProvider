package com.jcwhatever.nucleus.providers.mysql.statements;

import com.jcwhatever.nucleus.Nucleus;
import com.jcwhatever.nucleus.collections.ArrayQueue;
import com.jcwhatever.nucleus.managed.scheduler.Scheduler;
import com.jcwhatever.nucleus.mixins.IDisposable;
import com.jcwhatever.nucleus.providers.mysql.statements.FinalizedStatements.ExecuteResult;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureResultAgent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/*
 * 
 */
public class StatementExecutor implements IDisposable {

    final Queue<QueuedExecutable> _results;
    private final MySqlExecutorAsync[] _executors;

    private int _executorIndex;
    private boolean _isDisposed;

    public StatementExecutor(int totalExecutors) {
        PreCon.positiveNumber(totalExecutors);

        _executors = new MySqlExecutorAsync[totalExecutors];
        _results = new ArrayQueue<>(25 * totalExecutors);

        for (int i=0; i < totalExecutors; i++) {
            MySqlExecutorAsync executor = new MySqlExecutorAsync();
            executor.setName("MySqlProvider Statement Executor #" + i);

            try {
                // offset thread execution
                Thread.sleep(5);
            } catch (InterruptedException ignore) {}

            executor.start();
            _executors[i] = executor;
        }

        Scheduler.runTaskRepeat(Nucleus.getPlugin(), 1, 1, new MySqlResultProducer());
    }

    public void execute(Transaction transaction, FutureResultAgent<ISqlResult> agent) {
        PreCon.notNull(transaction);
        PreCon.notNull(agent);

        if (isDisposed())
            throw new IllegalStateException("StatementExecutor has been disposed.");

        addStatement(new QueuedTransaction(transaction, agent));
    }

    public void execute(FinalizedStatements statements, FutureResultAgent<ISqlResult> agent) {
        PreCon.notNull(statements);
        PreCon.notNull(agent);

        if (isDisposed())
            throw new IllegalStateException("StatementExecutor has been disposed.");

        addStatement(new QueuedStatement(statements, agent));
    }

    @Override
    public boolean isDisposed() {
        return _isDisposed;
    }

    @Override
    public void dispose() {

        for (MySqlExecutorAsync _executor : _executors) {
            _executor.interrupt();
        }

        _isDisposed = true;
    }

    private void addStatement(QueuedExecutable statement) {

        MySqlExecutorAsync executor = _executors[_executorIndex];

        synchronized (executor.queue) {
            executor.queue.add(statement);
        }

        _executorIndex++;

        if (_executorIndex >= _executors.length)
            _executorIndex = 0;
    }

    private class MySqlResultProducer implements Runnable {

        @Override
        public void run() {
            synchronized (_results) {

                while (!_results.isEmpty()) {

                    QueuedExecutable queued = _results.remove();
                    queued.notifySubscribers();
                }
            }
        }
    }

    private class MySqlExecutorAsync extends Thread {

        final Queue<QueuedExecutable> queue = new ArrayQueue<>(25);
        final Queue<QueuedExecutable> resultBuffer = new ArrayQueue<>(25);

        @Override
        public void run() {

            while (true) {

                synchronized (queue) {
                    while (!queue.isEmpty()) {

                        QueuedExecutable queued = queue.remove();
                        queued.execute();

                        resultBuffer.add(queued);
                    }
                }

                synchronized (_results) {
                    _results.addAll(resultBuffer);
                    resultBuffer.clear();
                }

                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private static class QueuedTransaction extends QueuedExecutable {

        final Transaction transaction;
        final List<ExecuteResult> statementResults;

        QueuedTransaction(Transaction transaction, FutureResultAgent<ISqlResult> agent) {
            super(agent);
            this.transaction = transaction;
            this.statementResults = new ArrayList<>(transaction.size());
            this.result = new ExecuteResult(transaction.size() * 2, agent);
        }

        @Override
        void execute() {

            Connection connection = transaction.getDatabase().getConnection();

            try {

                connection.setAutoCommit(false);

                for (FinalizedStatements statements : transaction) {
                    ExecuteResult statementResult = statements.executeNow(1);
                    result.addResults(statementResult);
                    statementResults.add(statementResult);
                }

                connection.commit();
                connection.setAutoCommit(true);

                isSuccess = true;

            }
            catch (SQLException e) {
                e.printStackTrace();
                isSuccess = false;
                errorMessage = e.getMessage();
            }
        }

        @Override
        void notifySubscribers() {
            if (isSuccess) {
                for (ExecuteResult executeResult : statementResults) {
                    executeResult.getAgent().success(executeResult);
                }
                agent.success(result);
            }
            else {
                for (ExecuteResult executeResult : statementResults) {
                    executeResult.getAgent().error(null, errorMessage);
                }
                agent.error(null, errorMessage);
            }
        }
    }

    private static class QueuedStatement extends QueuedExecutable {

        final FinalizedStatements statements;

        QueuedStatement(FinalizedStatements statements, FutureResultAgent<ISqlResult> agent) {
            super(agent);
            this.statements = statements;
        }

        @Override
        void execute() {

            try {
                result = statements.executeNow(0);
                isSuccess = true;

            } catch (SQLException e) {
                e.printStackTrace();
                isSuccess = false;
                errorMessage = e.getMessage();
            }
        }

        @Override
        void notifySubscribers() {
            if (isSuccess) {
                agent.success(result);
            }
            else {
                agent.error(null, errorMessage);
            }
        }
    }

    private static abstract class QueuedExecutable {

        final FutureResultAgent<ISqlResult> agent;
        ExecuteResult result;
        String errorMessage;
        boolean isSuccess;

        QueuedExecutable(FutureResultAgent<ISqlResult> agent) {
            this.agent = agent;
        }

        abstract void execute();

        abstract void notifySubscribers();
    }
}
