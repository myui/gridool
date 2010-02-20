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
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.db.catalog.UpdatePartitionInCatalogJob.UpdatePartitionInCatalogJobConf;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.GridDbUtils;
import gridool.util.GridUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import xbird.util.struct.Pair;

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
    public static final String distributionTableName;
    public static final String partitioningKeyTableName;
    static {
        hiddenFieldName = Settings.get("gridool.db.hidden_fieldnam", "_hidden");
        distributionTableName = Settings.get("gridool.db.partitioning.distribution_tbl", "_distribution");
        partitioningKeyTableName = Settings.get("gridool.db.partitioning.partitionkey_tbl", "_partitioning");
    }

    @Nonnull
    private final GridKernel kernel;
    @Nonnull
    private final DBAccessor dbAccessor;
    @Nonnull
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, Map<NodeWithState, List<NodeWithState>>> distributionMap;
    @GuardedBy("lock")
    private final Map<String, NodeWithState> nodeStateMap;

    /**
     * TableName => [ColumnName => PartitionKey]
     */
    @GuardedBy("partitionKeyMappping")
    private final Map<String, Map<String, PartitionKey>> partitionKeyMappping;

    public DistributionCatalog(@CheckForNull GridKernel kernel, @CheckForNull DBAccessor dbAccessor) {
        if(kernel == null) {
            throw new IllegalArgumentException();
        }
        if(dbAccessor == null) {
            throw new IllegalArgumentException();
        }
        this.kernel = kernel;
        this.dbAccessor = dbAccessor;
        this.distributionMap = new HashMap<String, Map<NodeWithState, List<NodeWithState>>>(12);
        this.nodeStateMap = new HashMap<String, NodeWithState>(64);
        this.partitionKeyMappping = new HashMap<String, Map<String, PartitionKey>>(32);
    }

    public void start() throws GridException {

        final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, true);
        try {
            if(!prepareTables(conn, distributionTableName, true)) {
                inquireDistributionTable(conn);
                inquirePartitioningKeyTable(conn);
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

    private void inquirePartitioningKeyTable(final Connection conn) throws SQLException {
        final String sql = "SELECT tablename, columnname, primarykey, partitionid FROM \""
                + partitioningKeyTableName + '"';
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String tableName = rs.getString(1);
                    String columnName = rs.getString(2);
                    boolean isPrimaryKey = rs.getBoolean(3);
                    int partitionId = rs.getInt(4);
                    Map<String, PartitionKey> columnMapping = partitionKeyMappping.get(tableName);
                    if(columnMapping == null) {
                        columnMapping = new HashMap<String, PartitionKey>(12);
                        partitionKeyMappping.put(tableName, columnMapping);
                    }
                    PartitionKey key = new PartitionKey(isPrimaryKey, partitionId);
                    columnMapping.put(columnName, key);
                    if(isPrimaryKey) {
                        columnMapping.put(DummyFieldNameForPrimaryKey, key);
                    }
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

    public int getPartitioningKey(@Nonnull final String tableName, @Nonnull final String fieldName)
            throws SQLException {
        final PartitionKey key;
        synchronized(partitionKeyMappping) {
            Map<String, PartitionKey> fieldPartitionMap = partitionKeyMappping.get(tableName);
            if(fieldPartitionMap == null) {
                fieldPartitionMap = new HashMap<String, PartitionKey>(12);
                partitionKeyMappping.put(tableName, fieldPartitionMap);
                final Connection conn = dbAccessor.getPrimaryDbConnection();
                try {
                    inquirePartitioningKeyPositions(conn, tableName, fieldPartitionMap, true);
                } finally {
                    JDBCUtils.closeQuietly(conn);
                }
            }
            key = fieldPartitionMap.get(fieldName);
        }
        if(key == null) {
            throw new IllegalArgumentException("Partitioning key is not registered for "
                    + tableName + '.' + fieldName);
        }
        return key.getPartitionNo();
    }

    /**
     * @return Column positions of primary key and foreign keys.
     * @throws GridException 
     */
    @Nonnull
    public Pair<int[], int[]> bindPartitioningKeyPositions(@Nonnull final String templateTableName, @Nonnull final String actualTableName)
            throws GridException {
        final Pair<int[], int[]> keys;
        final Map<String, PartitionKey> fieldPartitionMap = new HashMap<String, PartitionKey>(12);
        boolean updateDb = true;
        synchronized(partitionKeyMappping) {
            if(partitionKeyMappping.put(actualTableName, fieldPartitionMap) != null) {
                updateDb = false;
            }
            boolean autocommit = !updateDb;
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, autocommit);
            try {
                // inquire PK/FK relationship on database catalog
                keys = inquirePartitioningKeyPositions(conn, templateTableName, fieldPartitionMap, false);
                // store partitioning information into database.       
                if(updateDb) {
                    storePartitioningInformation(conn, actualTableName, fieldPartitionMap);
                    conn.commit();
                }
            } catch (SQLException e) {
                String errmsg = "failed to get partitioning keys for table: " + templateTableName;
                LOG.error(errmsg, e);
                if(updateDb) {
                    try {
                        conn.rollback();
                    } catch (SQLException rbe) {
                        LOG.warn("rollback failed", rbe);
                    }
                }
                throw new GridException(errmsg, e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
        }
        if(updateDb) {//TODO REVIEWME permit delay?
            UpdatePartitionInCatalogJobConf jobConf = new UpdatePartitionInCatalogJobConf(actualTableName, fieldPartitionMap);
            GridJobFuture<Boolean> future = kernel.execute(UpdatePartitionInCatalogJob.class, jobConf);
            Boolean status = GridUtils.invokeGet(future);
            if(Boolean.TRUE != status) {
                LOG.warn("Failed to execute UpdatePartitionInCatalogJob");
            }
        }
        return keys;
    }

    private static Pair<int[], int[]> inquirePartitioningKeyPositions(@Nonnull final Connection conn, @Nonnull final String tableName, @Nonnull final Map<String, PartitionKey> fieldPartitionMap, final boolean returnNull)
            throws SQLException {
        final List<String> keys = new ArrayList<String>();
        final DatabaseMetaData meta = conn.getMetaData();
        final String catalog = conn.getCatalog();

        // primary key
        final ResultSet rs1 = meta.getPrimaryKeys(catalog, null, tableName);
        try {
            while(rs1.next()) {
                String pk = rs1.getString("COLUMN_NAME");
                keys.add(pk);
            }
        } finally {
            rs1.close();
        }
        int[] pkeyIdxs = null;
        String pkColumnName = null;
        final int pkeyColumns = keys.size();
        if(pkeyColumns != 0) {
            final int[] idxs = new int[pkeyColumns];
            for(int i = 0; i < pkeyColumns; i++) {
                String columnName = keys.get(i);
                if(pkeyColumns == 1 && i == 0) {
                    pkColumnName = columnName;
                }
                ResultSet rs = meta.getColumns(catalog, null, tableName, columnName);
                if(!rs.next()) {
                    throw new IllegalStateException("Existing primary key '" + columnName
                            + "' was not defined in the catalog");
                }
                int pos = rs.getInt("ORDINAL_POSITION");
                idxs[i] = pos;
                rs.close();
            }
            Arrays.sort(idxs);
            pkeyIdxs = idxs;
        }
        keys.clear();

        // foreign key 
        final Map<Integer, String> columnPosNameMapping = new HashMap<Integer, String>(8);
        final ResultSet rs2 = meta.getImportedKeys(catalog, null, tableName);
        try {
            while(rs2.next()) {
                final String fkColumnName = rs2.getString("FKCOLUMN_NAME");
                if(!keys.contains(fkColumnName)) {
                    if(!fkColumnName.equals(pkColumnName)) {
                        keys.add(fkColumnName);
                    }
                }
            }
        } finally {
            rs2.close();
        }
        final int fkeyColumns = keys.size();
        int[] fkeyIdxs = null;
        if(fkeyColumns != 0) {
            final int[] idxs = new int[fkeyColumns];
            for(int i = 0; i < fkeyColumns; i++) {
                String columnName = keys.get(i);
                ResultSet rs = meta.getColumns(catalog, null, tableName, columnName);
                if(!rs.next()) {
                    throw new IllegalStateException("Existing foreign key '" + columnName
                            + "' was not defined in the catalog");
                }
                int pos = rs.getInt("ORDINAL_POSITION");
                idxs[i] = pos;
                columnPosNameMapping.put(pos, columnName);
                rs.close();
            }
            Arrays.sort(idxs);
            fkeyIdxs = idxs;
        }

        int shift = 0;
        if(pkeyIdxs != null && pkeyColumns == 1) {// FIXME workaround for avoiding multiple keys for partitioning key
            PartitionKey pkeyForPrimary = new PartitionKey(true, 1);
            shift++;
            fieldPartitionMap.put(DummyFieldNameForPrimaryKey, pkeyForPrimary);
            //if(pkeyColumns == 1) {
            fieldPartitionMap.put(pkColumnName, pkeyForPrimary);
            //}
        }
        if(fkeyIdxs != null) {
            for(int i = 0; i < fkeyIdxs.length; i++) {
                int fkey = fkeyIdxs[i];
                String columnName = columnPosNameMapping.get(fkey);
                PartitionKey pkey = new PartitionKey(false, 1 << shift);
                shift++;
                fieldPartitionMap.put(columnName, pkey);
            }
        }
        return returnNull ? null : new Pair<int[], int[]>(pkeyIdxs, fkeyIdxs);
    }

    public void updatePartitioningInformation(@Nonnull final String tableName, @Nonnull final Map<String, PartitionKey> fieldPartitionMap)
            throws GridException {
        synchronized(partitionKeyMappping) {
            if(partitionKeyMappping.containsKey(tableName)) {
                return;
            }
            partitionKeyMappping.put(tableName, fieldPartitionMap);
            final Connection conn = GridDbUtils.getPrimaryDbConnection(dbAccessor, false);
            try {
                storePartitioningInformation(conn, tableName, fieldPartitionMap);
                conn.commit();
            } catch (SQLException e) {
                String errmsg = "failed to store partitioning information for a table: "
                        + tableName;
                LOG.error(errmsg, e);
                try {
                    conn.rollback();
                } catch (SQLException rbe) {
                    LOG.warn("rollback failed", rbe);
                }
                throw new GridException(errmsg, e);
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
        }
    }

    private static void storePartitioningInformation(@Nonnull final Connection conn, @Nonnull final String tableName, @Nonnull final Map<String, PartitionKey> fieldPartitionMap)
            throws SQLException {
        final int numPartitionKeys = fieldPartitionMap.size();
        if(numPartitionKeys == 0) {
            return;
        }
        final String sql = "INSERT INTO \"" + partitioningKeyTableName + "\" VALUES(?, ?, ?, ?)";
        final List<Object[]> paramsList = new ArrayList<Object[]>(fieldPartitionMap.size());
        for(final Map.Entry<String, PartitionKey> e : fieldPartitionMap.entrySet()) {
            String columnName = e.getKey();
            PartitionKey key = e.getValue();
            if(key.isPrimary() && DummyFieldNameForPrimaryKey.equals(columnName)) {
                continue;
            }
            Object[] o = new Object[] { tableName, columnName, key.isPrimary(),
                    key.getPartitionNo() };
            paramsList.add(o);
        }
        Object[][] params = ArrayUtils.toArray(paramsList, Object[][].class);
        JDBCUtils.batch(conn, sql, params);
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
                + "\"(distkey varchar(50) NOT NULL, node varchar(50) NOT NULL, masternode varchar(50), state TINYINT NOT NULL);\n"
                + "CREATE TABLE \""
                + partitioningKeyTableName
                + "\"(tablename varchar(30) NOT NULL, columnname varchar(30) NOT NULL, primarykey boolean NOT NULL, partitionid int NOT NULL)";
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
