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
import xbird.util.jdbc.handlers.ScalarHandler;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ReplicationManager {
    private static final Log LOG = LogFactory.getLog(ReplicationManager.class);
    private static final String replicaTableName;
    static {
        replicaTableName = Settings.get("gridool.db.replication.replica_tblname", "_replica");
    }

    private final GridKernel kernel;
    private final ReplicaSelector replicaSelector;
    private final ReplicaCoordinator replicaCoordinator;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Stack<String> replicaNameStack;
    @GuardedBy("lock")
    private final Map<GridNode, String> replicaDbMappingCache;

    public ReplicationManager(@Nonnull GridKernel kernel, @Nonnull GridConfiguration config) {
        this.kernel = kernel;
        this.replicaSelector = ReplicationModuleBuilder.createReplicaSelector();
        this.replicaCoordinator = ReplicationModuleBuilder.createReplicaCoordinator(kernel, config);
        this.replicaNameStack = new Stack<String>();
        this.replicaDbMappingCache = new HashMap<GridNode, String>(32);
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

            String query = "SELECT dbname FROM \"" + replicaTableName
                    + "\" WHERE ipaddr = ? AND portnum = ?";
            Object[] params = new Object[2];
            params[0] = masterNode.getPhysicalAdress().getHostAddress();
            params[1] = masterNode.getPort();
            ResultSetHandler handler = new ScalarHandler("dbname");
            String replicaId = (String) JDBCUtils.query(query, params, handler);

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

            String replicaDbName = replicaNameStack.pop();
            if(replicaDbName != null) {
                if(replicaDbMappingCache.put(masterNode, replicaDbName) != null) {
                    throw new IllegalStateException();
                }
                Object[] params = new Object[3];
                params[0] = masterNode.getPhysicalAdress().getHostAddress();
                params[1] = masterNode.getPort();
                params[2] = replicaDbName;
                final int rows = JDBCUtils.update(conn, "UPDATE \"" + replicaTableName
                        + "\" SET ipaddr = ?, portnum = ? WHERE dbname = ?", params);
                if(rows == 1) {
                    conn.commit();
                    return true;
                } else {
                    return false;
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
            prepareReplicaTable(conn, replicaTableName);
            final Object[][] params = new String[dblen][];
            for(int i = 0; i < dblen; i++) {
                String dbname = dbnames[i];
                replicaNameStack.push(dbname);
                params[i] = new String[] { dbname };
            }
            JDBCUtils.batch(conn, "INSERT INTO \"" + replicaTableName + "\"(dbname) values(?)", params);
            conn.commit();
            return true;
        }
    }

    private static void prepareReplicaTable(@Nonnull Connection conn, @Nonnull String replicaTableName) {
        final String ddl = "CREATE TABLE \"" + replicaTableName
                + "\"(dbname varchar(30) primary key, ipaddr varchar(15), portnum int)";
        try {
            JDBCUtils.update(conn, ddl);
        } catch (SQLException e) {
            // table already exists
            try {
                conn.rollback();
            } catch (SQLException sqle) {
                LOG.warn("failed to rollback", sqle);
            }
        }
    }
}
