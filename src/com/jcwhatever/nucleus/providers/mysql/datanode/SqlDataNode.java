/*
 * This file is part of NucleusFramework for Bukkit, licensed under the MIT License (MIT).
 *
 * Copyright (c) JCThePants (www.jcwhatever.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.jcwhatever.nucleus.providers.mysql.datanode;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.jcwhatever.nucleus.Nucleus;
import com.jcwhatever.nucleus.collections.TreeEntryNode;
import com.jcwhatever.nucleus.managed.scheduler.IScheduledTask;
import com.jcwhatever.nucleus.managed.scheduler.Scheduler;
import com.jcwhatever.nucleus.providers.sql.ISqlDatabase;
import com.jcwhatever.nucleus.providers.sql.ISqlResult;
import com.jcwhatever.nucleus.providers.sql.ISqlTable;
import com.jcwhatever.nucleus.providers.sql.ISqlTableDefinition.ISqlTableColumn;
import com.jcwhatever.nucleus.providers.sql.datanode.ISqlDataNode;
import com.jcwhatever.nucleus.providers.sql.observer.SqlAutoCloseSubscriber;
import com.jcwhatever.nucleus.providers.sql.statement.ISqlTransaction;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdate;
import com.jcwhatever.nucleus.providers.sql.statement.update.ISqlUpdateFinal;
import com.jcwhatever.nucleus.storage.MemoryDataNode;
import com.jcwhatever.nucleus.utils.PreCon;
import com.jcwhatever.nucleus.utils.observer.future.FutureAgent;
import com.jcwhatever.nucleus.utils.observer.future.FutureSubscriber;
import com.jcwhatever.nucleus.utils.observer.future.IFuture;
import com.jcwhatever.nucleus.utils.observer.future.IFuture.FutureStatus;

import org.bukkit.plugin.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ISqlDataNode}.
 */
public class SqlDataNode extends MemoryDataNode implements ISqlDataNode {

    private final Map<String, SqlNodeContext> _contextMap;
    private final Set<String> _dirtyNodes;
    private final Map<ISqlDatabase, Map<ISqlTable, Multimap<Object, SqlNodeContext>>> _contextByDb;

    private IScheduledTask _saveTask;

    /**
     * Constructor.
     *
     * @param plugin    The owning plugin.
     * @param valueMap  Map of node names to sql values.
     */
    public SqlDataNode(Plugin plugin,
                       Map<String, SqlNodeValue> valueMap) {
        super(plugin);

        PreCon.notNull(valueMap);

        _contextMap = new HashMap<>(valueMap.size() + 5);
        _dirtyNodes = new HashSet<>(valueMap.size());

        _contextByDb = new HashMap<>(5);

        for (SqlNodeValue value : valueMap.values()) {

            Map<ISqlTable, Multimap<Object, SqlNodeContext>> tableMap =
                    _contextByDb.get(value.table.getDatabase());

            if (tableMap == null) {
                tableMap = new HashMap<>(15);
                _contextByDb.put(value.table.getDatabase(), tableMap);
            }

            Multimap<Object, SqlNodeContext> keyMap = tableMap.get(value.table);

            if (keyMap == null) {
                keyMap = MultimapBuilder.hashKeys().arrayListValues().build();
                tableMap.put(value.table, keyMap);
            }

            SqlNodeContext context = new SqlNodeContext(value);

            _contextMap.put(context.nodeName, context);
            keyMap.put(value.indexValue, context);
            set(value.nodeName, value.value);
        }

        cleanAll();
    }

    @Override
    public void remove(String nodePath) {
        TreeEntryNode<String, Object> treeNode = getNodeFromPath(nodePath, false);
        if (treeNode == null)
            return;

        String fullPath = getPath(treeNode);
        if (_contextMap.containsKey(fullPath))
            _dirtyNodes.add(fullPath);

        markDirty();

        //noinspection ConstantConditions
        treeNode.getParent().removeChild(treeNode);
    }

    @Override
    @Nullable
    protected TreeEntryNode<String, Object> setNodeFromPath(String nodePath, Object value) {

        TreeEntryNode<String, Object> node = super.setNodeFromPath(nodePath, value);

        if (node != null) {
            String fullPath = getPath(node);
            if (_contextMap.containsKey(fullPath))
                _dirtyNodes.add(fullPath);
        }

        return node;
    }

    @Override
    protected void cleanAll() {

        super.cleanAll();
        _dirtyNodes.clear();
    }

    @Override
    public boolean saveSync() {
        cleanAll();
        save();
        return true;
    }

    @Override
    public IFuture save() {

        final FutureAgent agent = new FutureAgent();

        if (_dirtyNodes.size() == 0)
            return agent.cancel("Nothing to save.");

        if (_saveTask != null)
            return agent.cancel("Save is already in progress.");

        if (getPlugin().isEnabled()) {
            _saveTask = Scheduler.runTaskLater(Nucleus.getPlugin(), 5, new Runnable() {
                @Override
                public void run() {
                    save(agent);
                }
            });
        } else {
            save(agent);
        }

        return agent.getFuture().onStatus(new FutureSubscriber() {
            @Override
            public void on(FutureStatus status, @Nullable String message) {
                _saveTask = null;
            }
        });
    }

    private void save(final FutureAgent agent) {

        final List<ISqlTransaction> transactions = new ArrayList<>(_contextByDb.size());

        for (Entry<ISqlDatabase, Map<ISqlTable, Multimap<Object, SqlNodeContext>>>
                dbEntry : _contextByDb.entrySet()) {

            ISqlTransaction transaction = null;

            Map<ISqlTable, Multimap<Object, SqlNodeContext>> tableMap = dbEntry.getValue();
            assert tableMap != null;

            for (Entry<ISqlTable, Multimap<Object, SqlNodeContext>> tableEntry : tableMap.entrySet()) {

                Multimap<Object, SqlNodeContext> nodeContexts = tableEntry.getValue();

                for (Object indexValue : nodeContexts.keySet()) {

                    ISqlUpdate update = null;
                    ISqlUpdateFinal updateFinal = null;

                    Collection<SqlNodeContext> contexts = nodeContexts.get(indexValue);

                    for (SqlNodeContext context : contexts) {

                        if (!_dirtyNodes.contains(context.nodeName))
                            continue;

                        if (update == null) {
                            update = tableEntry.getKey().updateRows();
                        }

                        if (transaction == null) {
                            transaction = dbEntry.getKey().createTransaction();
                        }

                        updateFinal = update.set(context.columnName, getRoot().get(context.nodeName));
                    }

                    if (update != null) {

                        ISqlTableColumn primary = tableEntry.getKey().getDefinition().getPrimaryKey();
                        assert primary != null;

                        updateFinal.where(primary.getName()).isEqualTo(indexValue)
                                .addToTransaction(transaction);

                    }
                }
            }

            if (transaction != null) {
                transactions.add(transaction);
            }
        }

        cleanAll();

        if (transactions.size() == 0) {
            agent.cancel("No changes.");
            return;
        }

        final LinkedList<ISqlTransaction> failedTransactions = new LinkedList<>();

        for (final ISqlTransaction transaction : transactions) {

            transaction.execute()
                    .onSuccess(new SqlAutoCloseSubscriber() {
                        @Override
                        public void onResult(@Nullable ISqlResult result, @Nullable String message) {

                            transactions.remove(transaction);

                            if (transactions.size() == 0) {
                                if (failedTransactions.size() == 0) {
                                    agent.success();
                                } else {
                                    agent.error("{0} transaction(s) failed.", failedTransactions.size());
                                }
                            }
                        }
                    })
                    .onError(new SqlAutoCloseSubscriber() {
                        @Override
                        public void onResult(@Nullable ISqlResult result, @Nullable String message) {

                            transactions.remove(transaction);
                            failedTransactions.add(transaction);

                            if (transactions.size() == 0) {
                                agent.error("{0} transaction(s) failed.", failedTransactions.size());
                            }
                        }
                    });
        }
    }

    public static class SqlNodeValue {

        private final String nodeName;
        private final String columnName;
        private final Object indexValue;
        private final ISqlTable table;
        private final Object value;

        public SqlNodeValue(String nodeName, String columnName,
                              Object indexValue, ISqlTable table, @Nullable Object value) {

            PreCon.notNull(nodeName);
            PreCon.notNullOrEmpty(columnName);
            PreCon.notNull(indexValue);
            PreCon.notNull(table);

            this.nodeName = nodeName;
            this.columnName = columnName;
            this.indexValue = indexValue;
            this.table = table;
            this.value = value;
        }


        public String getNodeName() {
            return nodeName;
        }

        public String getColumnName() {
            return columnName;
        }

        public ISqlTable getTable() {
            return table;
        }

        public Object getValue() {
            return value;
        }
    }

    private static class SqlNodeContext {

        final String nodeName;
        final String columnName;
        final Object indexValue;
        final ISqlTable table;

        SqlNodeContext(SqlNodeValue value) {

            this.nodeName = value.nodeName;
            this.columnName = value.columnName;
            this.indexValue = value.indexValue;
            this.table = value.table;
        }
    }
}
