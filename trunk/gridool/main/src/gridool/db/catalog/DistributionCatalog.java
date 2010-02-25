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
package gridool.db.catalog;

import gridool.GridException;
import gridool.GridNode;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.GridDbUtils;
import gridool.util.GridUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.jdbc.ResultSetHandler;
import xbird.util.lang.ArrayUtils;

/**
 * Class to manage how tables are distributed among nodes as primary/replica.
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DistributionCatalog {
    private static final Log LOG = LogFactory.getLog(DistributionCatalog.class);

    public static final String defaultDistributionKey = "";
    public static final String DummyFieldNameForPrimaryKey = "";
    public static final String hiddenFieldName;
    private static final String distributionTableName;
    private static final String partitionkeyTableName;
    public static final String tableIdSQLDataType;
    static {
        hiddenFieldName = Settings.get("gridool.db.hidden_fieldnam", "_hidden");
        distributionTableName = Settings.get("gridool.db.partitioning.distribution_tbl", "_distribution");
        partitionkeyTableName = Settings.get("gridool.db.partitioning.partitionkey_tbl", "_partitionkey");
        tableIdSQLDataType = Settings.get("gridool.db.partitioning.tableid_sqldatatype", "SMALLINT"); // 16 bit signed integer
    }

    @Nonnull
    private final DBAccessor dbAccessor;
    @Nonnull
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, Map<NodeWithState, List<NodeWithState>>> distributionMap;
    @GuardedBy("lock")
    private final Map<String, NodeWithState> nodeStateMap;

    @GuardedBy("partitionKeyMap")
    private final Map<String, Integer> tableIdMap;

    public DistributionCatalog(@CheckForNull DBAccessor dbAccessor) {
        if(dbAccessor == null) {
            throw new IllegalArgumentException();
        }
        this.dbAccessor = dbAccessor;
        this.distributionMap = new HashMap<String, Map<NodeWithState, List<NodeWithState>>>(12);
        this.nodeStateMap = new HashMap<String, NodeWithState>(64);
        this.tableIdMap = new HashMap<String, Integer>(12);
    }

    public void start() throws GridException {
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, true);
        try {
            if(!prepareTables(conn, distributionTableName, true)) {
                inquireDistributionTable(conn);
                inquirePartitionKeyTable(conn);
            }
        } catch (SQLException e) {
            LOG.fatal("Failed to setup DistributionCatalog", e);
            throw new GridException("Failed to setup DistributionCatalog", e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
    }

    private void inquireDistributionTable(final Connection conn) throws SQLException {
        final String sql = "SELECT distkey, node, masternode, state FROM \""
                + distributionTableName + "\" ORDER BY masternode ASC"; // NULLs last (for MonetDB)
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String distkey = rs.getString(1);
                    String nodestr = rs.getString(2);
                    String masterStr = rs.getString(3); // may be null
                    int state = rs.getInt(4);

                    Map<NodeWithState, List<NodeWithState>> masterSlaveMapping = distributionMap.get(distkey);
                    if(masterSlaveMapping == null) {
                        masterSlaveMapping = new HashMap<NodeWithState, List<NodeWithState>>(32);
                        distributionMap.put(distkey, masterSlaveMapping);
                    }
                    NodeWithState nodeWS = internNodeState(nodestr, state);
                    if(masterStr == null) {// master
                        masterSlaveMapping.put(nodeWS, new ArrayList<NodeWithState>(4));
                    } else {// slave
                        NodeWithState masterWS = getNodeState(masterStr);
                        if(masterWS == null) {
                            throw new IllegalStateException("Master node of slave '" + nodeWS.node
                                    + "' is not found");
                        }
                        List<NodeWithState> slaves = masterSlaveMapping.get(masterWS);
                        if(slaves == null) {//sanity check
                            throw new IllegalStateException("Slaves list is null for master: "
                                    + masterStr);
                        }
                        slaves.add(nodeWS);
                    }
                }
                return null;
            }
        };
        JDBCUtils.query(conn, sql, rsh);
    }

    private void inquirePartitionKeyTable(final Connection conn) throws SQLException {
        final String sql = "SELECT tablename, id FROM \"" + partitionkeyTableName + '"';
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String tableName = rs.getString(1);
                    int partitionNo = rs.getInt(2);
                    assert (tableName != null);
                    tableIdMap.put(tableName, partitionNo);
                }
                return null;
            }
        };
        JDBCUtils.query(conn, sql, rsh);
    }

    public void stop() throws GridException {}

    public void registerPartition(@Nonnull GridNode master, @Nonnull final List<GridNode> slaves, @Nonnull final String distKey)
            throws GridException {
        synchronized(lock) {
            boolean needToInsertDb = true;
            Map<NodeWithState, List<NodeWithState>> mapping = distributionMap.get(distKey);
            if(mapping == null) {
                mapping = new HashMap<NodeWithState, List<NodeWithState>>(32);
                mapping.put(wrapNode(master, true), wrapNodes(slaves, true));
                if(distributionMap.put(distKey, mapping) != null) {
                    throw new IllegalStateException();
                }
            } else {
                NodeWithState masterWS = wrapNode(master, true);
                final List<NodeWithState> slaveList = mapping.get(masterWS);
                if(slaveList == null) {
                    if(mapping.put(masterWS, wrapNodes(slaves, true)) != null) {
                        throw new IllegalStateException();
                    }
                } else {
                    boolean noAdd = true;
                    for(final GridNode slave : slaves) {
                        NodeWithState slaveWS = wrapNode(slave, true);
                        if(!slaveList.contains(slaveWS)) {
                            slaveList.add(slaveWS);
                            noAdd = false;
                        }
                    }
                    needToInsertDb = !noAdd;
                }
            }
            if(!needToInsertDb) {
                return;
            }
            final String insertQuery = "INSERT INTO \"" + distributionTableName
                    + "\" VALUES(?, ?, ?, ?)";
            final Object[][] params = toNewParams(distKey, master, slaves);
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, false);
            try {
                JDBCUtils.batch(conn, insertQuery, params);
                conn.commit();
            } catch (SQLException e) {
                String errmsg = "Failed to execute a query: " + insertQuery;
                LOG.error(errmsg, e);
                try {
                    conn.rollback();
                } catch (SQLException sqle) {
                    LOG.warn("rollback failed", e);
                }
                throw new GridException(errmsg, e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
        }
    }

    private static Object[][] toNewParams(@Nonnull final String distkey, @Nonnull final GridNode master, @Nonnull final List<GridNode> slaves) {
        final Object[][] params = new Object[slaves.size() + 1][];
        final Integer normalState = NodeState.normal.getStateNumber();
        final String masterRaw = GridUtils.toNodeInfo(master);
        params[0] = new Object[] { distkey, masterRaw, null, normalState };
        for(int i = 0; i < slaves.size(); i++) {
            GridNode node = slaves.get(i);
            String slaveRaw = GridUtils.toNodeInfo(node);
            params[i + 1] = new Object[] { distkey, slaveRaw, masterRaw, normalState };
        }
        return params;
    }

    @Nonnull
    public GridNode[] getMasters(@Nullable final String distKey) {
        final GridNode[] masters;
        synchronized(lock) {
            final Map<NodeWithState, List<NodeWithState>> mapping = distributionMap.get(distKey);
            if(mapping == null) {
                return new GridNode[0];
            }
            List<GridNode> list = unwrapNodes(mapping.keySet(), true);
            masters = ArrayUtils.toArray(list, GridNode[].class);
        }
        return masters;
    }

    @Nonnull
    public GridNode[] getSlaves(@Nonnull final GridNode master, @Nullable final String distKey) {
        final GridNode[] slaves;
        synchronized(lock) {
            final Map<NodeWithState, List<NodeWithState>> mapping = distributionMap.get(distKey);
            if(mapping == null) {
                return new GridNode[0];
            }
            final List<NodeWithState> slaveList = mapping.get(master);
            if(slaveList == null) {
                return new GridNode[0];
            }
            List<GridNode> list = unwrapNodes(slaveList, true);
            slaves = ArrayUtils.toArray(list, GridNode[].class);
        }
        return slaves;
    }

    @Nullable
    public NodeState getNodeState(@Nonnull final GridNode node) {
        final String nodeID = node.getKey();
        synchronized(lock) {
            final NodeWithState nodeinfo = nodeStateMap.get(nodeID);
            if(nodeinfo == null) {
                return null;
            }
            return nodeinfo.state;
        }
    }

    public NodeState setNodeState(@Nonnull final GridNode node, @Nonnull final NodeState newState)
            throws GridException {
        final String nodeID = node.getKey();
        final NodeState prevState;
        synchronized(lock) {
            NodeWithState nodeWS = nodeStateMap.get(nodeID);
            if(nodeWS == null) {
                nodeWS = new NodeWithState(node, newState);
                nodeStateMap.put(nodeID, nodeWS);
                prevState = null;
            } else {
                prevState = nodeWS.state;
                nodeWS.state = newState;
                return prevState;
            }
            final String sql = "UPDATE \"" + distributionTableName
                    + "\" SET state = ? WHERE node = ?";
            int nodeState = newState.getStateNumber();
            String nodeRaw = GridUtils.toNodeInfo(node);
            final Object[] params = new Object[] { nodeState, nodeRaw };
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, true);
            try {
                JDBCUtils.update(conn, sql, params);
            } catch (SQLException e) {
                String errmsg = "failed to execute a query: " + sql;
                LOG.error(errmsg, e);
                throw new GridException(errmsg, e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
        }
        return prevState;
    }

    public int getTableId(@Nonnull final String tableName, final boolean failFast) {
        final Integer cachedTableId;
        synchronized(tableIdMap) {
            cachedTableId = tableIdMap.get(tableName);
        }
        if(cachedTableId == null) {
            if(failFast) {
                throw new IllegalArgumentException("TableId is not resolved: " + tableName);
            } else {
                return -1;
            }
        } else {
            return cachedTableId.intValue();
        }
    }

    @Nonnull
    public int[] bindTableId(@Nonnull final String[] tableNames, @Nonnull final String templateTableNamePrefix)
            throws GridException {
        final int numTableNames = tableNames.length;
        if(numTableNames == 0) {
            return new int[0];
        }

        final int[] tableIds = new int[numTableNames];
        Arrays.fill(tableIds, -1);
        final String insertQuery = "INSERT INTO \"" + partitionkeyTableName
                + "\"(tablename) VALUES(?)";
        final String selectQuery = "SELECT tablename, id FROM \"" + partitionkeyTableName + '"';
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(final ResultSet rs) throws SQLException {
                for(int i = 0; rs.next(); i++) {
                    String tblname = rs.getString(1);
                    int pos = Arrays.binarySearch(tableNames, tblname);
                    if(pos >= 0) {
                        int key = rs.getInt(2);
                        tableIds[pos] = key;
                    }
                }
                return null;
            }
        };
        final Object[][] params = new Object[numTableNames][];
        for(int i = 0; i < numTableNames; i++) {
            params[i] = new Object[] { tableNames[i] };
        }
        synchronized(tableIdMap) {
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, true);
            try {
                JDBCUtils.batch(conn, insertQuery, params);
                JDBCUtils.query(conn, selectQuery, rsh);
            } catch (SQLException e) {
                LOG.error(e);
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            for(int i = 0; i < numTableNames; i++) {
                String tblname = tableNames[i];
                int tid = tableIds[i];
                if(tid == -1) {
                    throw new IllegalStateException("Table ID is not registered for table: "
                            + tblname);
                }
                tableIdMap.put(tblname, tid);
                String templateTableName = templateTableNamePrefix + tblname;
                tableIdMap.put(templateTableName, tid);
            }

        }
        return tableIds;
    }

    public void registerTableId(@Nonnull final String tableName, final int tableId)
            throws SQLException {
        final Integer tableIdObj = tableId;
        synchronized(tableIdMap) {
            if(tableIdMap.put(tableName, tableIdObj) == null) {
                final String insertQuery = "INSERT INTO \"" + partitionkeyTableName
                        + "\" VALUES(?, ?)";
                final Connection conn = dbAccessor.getPrimaryDbConnection();
                try {
                    JDBCUtils.update(conn, insertQuery, new Object[] { tableName, tableIdObj });
                } catch (SQLException e) {
                    String errmsg = "failed to execute a query: " + insertQuery;
                    LOG.error(errmsg, e);
                    throw e;
                } finally {
                    JDBCUtils.closeQuietly(conn);
                }
            }
        }
    }

    public int getTablePartitionNo(final String tableName, final boolean failFast) {
        int tableId = getTableId(tableName, failFast);
        int partitionNo = getTablePartitionNo(tableId);
        return partitionNo;
    }

    private static int getTablePartitionNo(final int tableId) {
        if(tableId < 1) {
            throw new IllegalArgumentException("Illegal tableId: " + tableId);
        }
        return 1 << (tableId - 1); // REVIEWME
    }

    private static final class NodeWithState {

        @Nonnull
        final GridNode node;
        @Nonnull
        NodeState state;

        NodeWithState(GridNode node) {
            this(node, NodeState.normal);
        }

        NodeWithState(GridNode node, NodeState state) {
            this.node = node;
            this.state = state;
        }

        NodeWithState(GridNode node, int stateNo) {
            this.node = node;
            this.state = NodeState.resolve(stateNo);
        }

        boolean isValid() {
            return state.isValid();
        }

        @Override
        public int hashCode() {
            return node.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == this) {
                return true;
            }
            if(obj instanceof NodeWithState) {
                NodeWithState other = (NodeWithState) obj;
                return node.equals(other.node);
            }
            return false;
        }

        @Override
        public String toString() {
            return node.toString() + " (" + state + ')';
        }

    }

    private NodeWithState internNodeState(final String nodeInfo, final int stateNo) {
        GridNode node = GridUtils.fromNodeInfo(nodeInfo);
        String nodeId = node.getKey();
        NodeWithState nodeWS = nodeStateMap.get(nodeId);
        if(nodeWS == null) {
            nodeWS = new NodeWithState(node, stateNo);
            nodeStateMap.put(nodeId, nodeWS);
        }
        return nodeWS;
    }

    @Nullable
    private NodeWithState getNodeState(final String nodeInfo) {
        GridNode node = GridUtils.fromNodeInfo(nodeInfo);
        String nodeId = node.getKey();
        return nodeStateMap.get(nodeId);
    }

    @Nonnull
    private NodeWithState wrapNode(final GridNode node, final boolean replace) {
        final String nodeId = node.getKey();
        NodeWithState nodeWS = nodeStateMap.get(nodeId);
        if(nodeWS == null) {
            nodeWS = new NodeWithState(node);
            nodeStateMap.put(nodeId, nodeWS);
        } else {
            if(replace && !nodeWS.isValid()) {
                nodeWS.state = NodeState.normal;
            }
        }
        return nodeWS;
    }

    @Nonnull
    private List<NodeWithState> wrapNodes(final List<GridNode> nodes, final boolean replace) {
        if(nodes.isEmpty()) {
            return Collections.emptyList();
        }
        final List<NodeWithState> list = new ArrayList<NodeWithState>(nodes.size());
        for(GridNode node : nodes) {
            NodeWithState nodeWS = wrapNode(node, replace);
            if(replace || nodeWS.isValid()) {
                list.add(nodeWS);
            }
        }
        return list;
    }

    @Nonnull
    private List<GridNode> unwrapNodes(final Collection<NodeWithState> nodes, final boolean validate) {
        if(nodes.isEmpty()) {
            return Collections.emptyList();
        }
        final List<GridNode> list = new ArrayList<GridNode>(nodes.size());
        if(validate) {
            for(NodeWithState ns : nodes) {
                if(ns.isValid()) {
                    list.add(ns.node);
                }
            }
        } else {
            for(NodeWithState ns : nodes) {
                list.add(ns.node);
            }
        }
        return list;
    }

    private static boolean prepareTables(@Nonnull final Connection conn, final String distributionTableName, final boolean autoCommit) {
        final String ddl = "CREATE TABLE \""
                + distributionTableName
                + "\"(distkey varchar(50) NOT NULL, node varchar(50) NOT NULL, masternode varchar(50), state SMALLINT NOT NULL);\n"
                + "CREATE TABLE \"" + partitionkeyTableName
                + "\"(tablename varchar(30) PRIMARY KEY, id " + tableIdSQLDataType
                + " auto_increment);";
        try {
            JDBCUtils.update(conn, ddl);
            if(!autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
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
