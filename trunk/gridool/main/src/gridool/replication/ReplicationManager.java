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
import gridool.GridException;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridTask;
import gridool.communication.payload.GridNodeInfo;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.GridDbUtils;
import gridool.processors.task.GridTaskProcessor;
import gridool.replication.jobs.ReplicateTaskJob;

import java.sql.Connection;
import java.sql.ResultSet;
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
import xbird.util.lang.ClassUtils;

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
        replicaTableName = Settings.get("gridool.db.replication.replica_tbl", "_replica");
    }

    private final GridKernel kernel;
    private final DBAccessor dba;
    private final GridNodeInfo localMasterNode;
    private final ReplicaSelector replicaSelector;
    private final ReplicaCoordinator replicaCoordinator;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Stack<String> replicaNameStack;
    @GuardedBy("lock")
    private final Map<String, String> replicaDbMappingCache;

    public ReplicationManager(@Nonnull GridKernel kernel, @Nonnull DBAccessor dba, @Nonnull GridConfiguration config) {
        this.kernel = kernel;
        this.dba = dba;
        this.localMasterNode = config.getLocalNode();
        this.replicaSelector = ReplicationModuleBuilder.createReplicaSelector();
        this.replicaCoordinator = ReplicationModuleBuilder.createReplicaCoordinator(kernel, config);
        this.replicaNameStack = new Stack<String>();
        this.replicaDbMappingCache = new HashMap<String, String>(32);
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

    public void start() throws GridException {
        final String sql = "SELECT dbname, nodeinfo FROM \"" + replicaTableName
                + "\" ORDER BY entryno ASC"; // order by to push newer one into the top of stack
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String dbname = rs.getString(1);
                    assert (dbname != null);
                    String nodeinfo = rs.getString(2);
                    if(nodeinfo == null) {
                        if(!replicaNameStack.contains(dbname)) {
                            replicaNameStack.push(dbname);
                        }
                    } else {
                        String prevMapping = replicaDbMappingCache.put(nodeinfo, dbname);
                        if(prevMapping != null && nodeinfo.equals(prevMapping)) {
                            throw new IllegalStateException("Invalid mapping for node '" + nodeinfo
                                    + "' [Old:" + prevMapping + ", New:" + dbname + ']');
                        }
                    }
                }
                return null;
            }
        };
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true);
        try {
            if(!prepareReplicaTable(conn, replicaTableName, true)) {
                JDBCUtils.query(conn, sql, rsh);
            }
        } catch (SQLException e) {
            // avoid table does not exist error
            if(LOG.isDebugEnabled()) {
                LOG.debug("Table '" + replicaTableName + "' does not exist?", e);
            }
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
    }

    public void stop() throws GridException {}

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
            LOG.info("Replicated a task " + ClassUtils.getSimpleClassName(task) + '['
                    + task.getTaskId() + "] to slave nodes: " + replicas);
        }
        return true;
    }

    public String getReplicaDatabaseName(@Nonnull Connection conn, @Nonnull GridNode masterNode)
            throws SQLException {
        final String masterNodeId = masterNode.getKey();
        synchronized(lock) {
            String replicaIdCached = replicaDbMappingCache.get(masterNodeId);
            if(replicaIdCached != null) {
                return replicaIdCached;
            }

            String query = "SELECT dbname FROM \"" + replicaTableName + "\" WHERE nodeinfo = ?";
            Object[] params = new Object[1];
            params[0] = masterNodeId;
            ResultSetHandler handler = new ScalarHandler("dbname");
            String replicaId = (String) JDBCUtils.query(conn, query, params, handler);

            replicaDbMappingCache.put(masterNodeId, replicaId);
            return replicaId;
        }
    }

    public boolean configureReplicaDatabase(@Nonnull Connection conn, @Nonnull GridNode masterNode, boolean addReplicas)
            throws SQLException {
        if(!addReplicas) {
            throw new UnsupportedOperationException();
        }
        final String masterNodeId = masterNode.getKey();
        synchronized(lock) {
            if(replicaDbMappingCache.containsKey(masterNodeId)) {
                return true;
            }
            if(!replicaNameStack.isEmpty()) {
                String replicaDbName = replicaNameStack.pop();
                if(replicaDbName != null) {
                    Object[] params = new Object[2];
                    params[0] = masterNodeId;
                    params[1] = replicaDbName;
                    final int rows = JDBCUtils.update(conn, "UPDATE \"" + replicaTableName
                            + "\" SET nodeinfo = ? WHERE dbname = ?", params);
                    if(rows == 1) {
                        conn.commit();
                        if(replicaDbMappingCache.put(masterNodeId, replicaDbName) != null) {
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
        final int dblen = dbnames.length;
        if(dblen < 1) {
            return false;
        }
        synchronized(lock) {
            int remaining = 0;
            for(int i = 0; i < dblen; i++) {
                final String dbname = dbnames[i];
                if(replicaNameStack.contains(dbname)) {
                    dbnames[i] = null;
                } else if(replicaDbMappingCache.containsValue(dbname)) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Replica '" + dbname + "' is already mapped");
                    }
                    dbnames[i] = null;
                } else {
                    remaining++;
                }
            }
            if(remaining > 0) {
                //prepareReplicaTable(conn, replicaTableName);
                final Object[][] params = new String[remaining][];
                for(int i = 0; i < dblen; i++) {
                    final String dbname = dbnames[i];
                    if(dbname != null) {
                        params[i] = new String[] { dbname };
                        replicaNameStack.push(dbname);
                    }
                }
                JDBCUtils.batch(conn, "INSERT INTO \"" + replicaTableName + "\"(dbname) values(?)", params);
                conn.commit();
            }
            return true;
        }
    }

    private static boolean prepareReplicaTable(@Nonnull final Connection conn, @Nonnull final String replicaTableName, final boolean autoCommit) {
        final String ddl = "CREATE TABLE \""
                + replicaTableName
                + "\"(dbname varchar(30) PRIMARY KEY, nodeinfo VARCHAR(30), entryno INT auto_increment)";
        try {
            JDBCUtils.update(conn, ddl);
            if(!autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("failed executing a query: " + ddl, e);
            }
            // avoid table already exists error
            if(!autoCommit) {
                try {
                    conn.rollback();
                } catch (SQLException sqle) {
                    LOG.warn("failed to rollback", sqle);
                }
            }
            return false;
        }
        return true;
    }
}
