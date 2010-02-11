/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package gridool.replication;

import gridool.GridConfiguration;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridTask;
import gridool.communication.payload.GridNodeInfo;
import gridool.processors.task.GridTaskProcessor;
import gridool.replication.jobs.ReplicateTaskJob;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.jdbc.ResultSetHandler;
import xbird.util.jdbc.handlers.ColumnListHandler;
import xbird.util.jdbc.handlers.ScalarHandler;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ReplicationManager {
    private static final Log LOG = LogFactory.getLog(ReplicationManager.class);
    private static final String replicaTableName;
    static {
        replicaTableName = Settings.get("gridool.db.replication.replica_tblname", "_replica");
    }

    private final GridKernel kernel;
    private final GridNode localMasterNode;
    private final ReplicaSelector replicaSelector;
    private final ReplicaCoordinator replicaCoordinator;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Stack<String> replicaNameStack;
    @GuardedBy("lock")
    private final Map<GridNode, String> replicaDbMappingCache;

    public ReplicationManager(@Nonnull GridKernel kernel, @Nonnull GridConfiguration config) {
        this.kernel = kernel;
        this.localMasterNode = config.getLocalNode();
        this.replicaSelector = ReplicationModuleBuilder.createReplicaSelector();
        this.replicaCoordinator = ReplicationModuleBuilder.createReplicaCoordinator(kernel, config);
        this.replicaNameStack = new Stack<String>();
        this.replicaDbMappingCache = new HashMap<GridNode, String>(32);
    }

    @Nonnull
    public GridNode getLocalMasterNode() {
        return localMasterNode;
    }

    @Nonnull
    public ReplicaSelector getReplicaSelector() {
        return replicaSelector;
    }

    @Nonnull
    public ReplicaCoordinator getReplicaCoordinator() {
        return replicaCoordinator;
    }

    /**
     * @see GridTaskProcessor#processTask(GridTask)
     */
    public boolean replicateTask(@Nonnull GridTask task, @Nonnull GridNodeInfo masterNode) {
        if(!task.isReplicatable()) {// sanity check
            throw new IllegalStateException();
        }
        final List<GridNode> replicas = masterNode.getReplicas();
        if(replicas.isEmpty()) {
            return true; // there is no replica
        }

        task.setTransferToReplica(masterNode);
        final GridJobFuture<Boolean> future = kernel.execute(ReplicateTaskJob.class, new ReplicateTaskJob.JobConf(task, replicas));
        final Boolean succeed;
        try {
            succeed = future.get();
        } catch (InterruptedException e) {
            LOG.warn(e);
            return false;
        } catch (ExecutionException e) {
            LOG.error(e);
            return false;
        }
        if(succeed == null || !succeed.booleanValue()) {
            LOG.error("Replication of a task failed: " + task.getTaskId());
            return false;
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("Replicated a task '" + task.getTaskId() + "' to slave nodes: " + replicas);
        }
        return true;
    }

    public String getReplicaDatabaseName(@Nonnull Connection conn, @Nonnull GridNode masterNode)
            throws SQLException {
        synchronized(lock) {
            String replicaIdCached = replicaDbMappingCache.get(masterNode);
            if(replicaIdCached != null) {
                return replicaIdCached;
            }

            String query = "SELECT dbname FROM \"" + replicaTableName + "\" WHERE nodeinfo = ?";
            Object[] params = new Object[1];
            params[0] = masterNode.getKey();
            ResultSetHandler handler = new ScalarHandler("dbname");
            String replicaId = (String) JDBCUtils.query(conn, query, params, handler);

            replicaDbMappingCache.put(masterNode, replicaId);
            return replicaId;
        }
    }

    public boolean configureReplicaDatabase(@Nonnull Connection conn, @Nonnull GridNode masterNode, boolean addReplicas)
            throws SQLException {
        if(!addReplicas) {
            throw new UnsupportedOperationException();
        }
        synchronized(lock) {
            if(replicaDbMappingCache.containsKey(masterNode)) {
                return true;
            }
            if(!replicaNameStack.isEmpty()) {
                String replicaDbName = replicaNameStack.pop();
                if(replicaDbName != null) {
                    Object[] params = new Object[2];
                    params[0] = masterNode.getKey();
                    params[1] = replicaDbName;
                    final int rows = JDBCUtils.update(conn, "UPDATE \"" + replicaTableName
                            + "\" SET nodeinfo = ? WHERE dbname = ?", params);
                    if(rows == 1) {
                        conn.commit();
                        if(replicaDbMappingCache.put(masterNode, replicaDbName) != null) {
                            throw new IllegalStateException();
                        }
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
        LOG.error("There is no avialable replica database");
        return false;
    }

    public boolean registerReplicaDatabase(@Nonnull Connection conn, String... dbnames)
            throws SQLException {
        synchronized(lock) {
            final int dblen = dbnames.length;
            if(dblen < 1) {
                return false;
            }
            prepareReplicaTable(conn, replicaTableName, replicaNameStack);
            final Object[][] params = new String[dblen][];
            for(int i = 0; i < dblen; i++) {
                String dbname = dbnames[i];
                if(!replicaNameStack.contains(dbname)) {
                    replicaNameStack.push(dbname);
                }
                params[i] = new String[] { dbname };
            }
            JDBCUtils.batch(conn, "INSERT INTO \"" + replicaTableName + "\"(dbname) values(?)", params);
            conn.commit();
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private static void prepareReplicaTable(@Nonnull final Connection conn, @Nonnull final String replicaTableName, @Nonnull final Stack<String> replicaNameStack) {
        final String ddl = "CREATE TABLE \"" + replicaTableName
                + "\"(dbname varchar(30) primary key, nodeinfo varchar(30))";
        try {
            JDBCUtils.update(conn, ddl);
        } catch (SQLException e) {
            // table already exists
            try {
                conn.rollback();
            } catch (SQLException sqle) {
                LOG.warn("failed to rollback", sqle);
            }
            final Object result;
            String sql = "SELECT dbname FROM \"" + replicaTableName + "\" WHERE nodeinfo IS NULL";
            ColumnListHandler<String> rsh = new ColumnListHandler<String>();
            try {
                result = JDBCUtils.query(conn, sql, rsh);
            } catch (SQLException sqlex) {
                LOG.error("failed to execute a query: " + sql, sqlex);
                return;
            }
            final List<String> dbnames = (List<String>) result;
            for(final String dbname : dbnames) {
                if(!replicaNameStack.contains(dbname)) {
                    replicaNameStack.push(dbname);
                }
            }
        }
    }
}
