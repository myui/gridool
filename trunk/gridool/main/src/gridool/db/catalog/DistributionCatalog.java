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

import gridool.GridNode;
import gridool.db.helpers.DBAccessor;

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

import xbird.config.Settings;
import xbird.util.collections.ints.IntArrayList;
import xbird.util.jdbc.JDBCUtils;
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

    public static final String defaultDistributionKey = "";
    public static final String DummyFieldNameForPrimaryKey = "";
    public static final String hiddenFieldName;
    static {
        hiddenFieldName = Settings.get("gridool.db.hidden_fieldnam", "_hidden");
    }

    @Nonnull
    private final DBAccessor dbAccessor;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, Map<NodeWithState, List<NodeWithState>>> distributionCache;
    @GuardedBy("lock")
    private final Map<GridNode, NodeWithState> nodeStateMap;

    @GuardedBy("partitionKeyMappping")
    private final Map<String, Map<String, PartitionKey>> partitionKeyMappping;

    public DistributionCatalog(@CheckForNull DBAccessor dbAccessor) {
        if(dbAccessor == null) {
            throw new IllegalArgumentException();
        }
        this.dbAccessor = dbAccessor;
        this.distributionCache = new HashMap<String, Map<NodeWithState, List<NodeWithState>>>(12);
        this.nodeStateMap = new HashMap<GridNode, NodeWithState>(64);
        this.partitionKeyMappping = new HashMap<String, Map<String, PartitionKey>>(32);
    }

    public void registerPartition(@Nonnull GridNode master, @Nonnull final List<GridNode> slaves, @Nonnull final String distKey) {
        synchronized(lock) {
            Map<NodeWithState, List<NodeWithState>> mapping = distributionCache.get(distKey);
            if(mapping == null) {
                mapping = new HashMap<NodeWithState, List<NodeWithState>>(32);
                distributionCache.put(distKey, mapping);
                mapping.put(wrapNode(master, true), wrapNodes(slaves, true));
            } else {
                List<NodeWithState> oldSlaves = mapping.get(mapping);
                if(oldSlaves == null) {
                    mapping.put(wrapNode(master, true), wrapNodes(slaves, true));
                } else {
                    for(GridNode slave : slaves) {
                        oldSlaves.add(wrapNode(slave, true));
                    }
                }
            }
        }
    }

    public GridNode[] getMasters(@Nullable final String distKey) {
        final GridNode[] masters;
        synchronized(lock) {
            final Map<NodeWithState, List<NodeWithState>> mapping = distributionCache.get(distKey);
            if(mapping == null) {
                return new GridNode[0];
            }
            List<GridNode> list = unwrapNodes(mapping.keySet(), true);
            masters = ArrayUtils.toArray(list, GridNode[].class);
        }
        return masters;
    }

    public GridNode[] getSlaves(@Nonnull final GridNode master, @Nullable final String distKey) {
        final GridNode[] slaves;
        synchronized(lock) {
            final Map<NodeWithState, List<NodeWithState>> mapping = distributionCache.get(distKey);
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
        synchronized(lock) {
            final NodeWithState nodeinfo = nodeStateMap.get(node);
            if(nodeinfo == null) {
                return null;
            }
            return nodeinfo.state;
        }
    }

    public NodeState setNodeState(@Nonnull final GridNode node, @Nonnull final NodeState newState) {
        synchronized(lock) {
            NodeWithState nodeinfo = nodeStateMap.get(node);
            if(nodeinfo == null) {
                nodeinfo = new NodeWithState(node, newState);
                nodeStateMap.put(node, nodeinfo);
                return null;
            } else {
                NodeState prevState = nodeinfo.state;
                nodeinfo.state = newState;
                return prevState;
            }
        }
    }

    public int getPartitioningKey(@Nonnull final String tableName, @Nonnull final String fieldName)
            throws SQLException {
        synchronized(partitionKeyMappping) {
            Map<String, PartitionKey> fieldPartitionMap = partitionKeyMappping.get(tableName);
            if(fieldPartitionMap == null) {
                fieldPartitionMap = new HashMap<String, PartitionKey>(12);
                partitionKeyMappping.put(tableName, fieldPartitionMap);
                final Connection conn = dbAccessor.getPrimaryDbConnection();
                try {
                    getPartitioningKeyPositions(tableName, conn, fieldPartitionMap, true);
                } finally {
                    JDBCUtils.closeQuietly(conn);
                }
            }
            PartitionKey key = fieldPartitionMap.get(fieldName);
            return key.partitionNo;
        }
    }

    @Nonnull
    public Pair<int[], int[]> getPartitioningKeyPositions(@Nonnull final String tableName)
            throws SQLException {
        final Pair<int[], int[]> keys;
        synchronized(partitionKeyMappping) {
            Map<String, PartitionKey> fieldPartitionMap = partitionKeyMappping.get(tableName);
            if(fieldPartitionMap != null) {
                int[] pkey = null;
                final IntArrayList fkeyList = new IntArrayList(12);
                for(final PartitionKey key : fieldPartitionMap.values()) {
                    if(key.isPrimary) {
                        pkey = key.columnsPos;
                    } else {
                        int pos = key.getColumnPos(0);
                        fkeyList.add(pos);
                    }
                }
                int[] fkeys = fkeyList.isEmpty() ? null : fkeyList.toArray();
                keys = new Pair<int[], int[]>(pkey, fkeys);
            } else {
                fieldPartitionMap = new HashMap<String, PartitionKey>(12);
                partitionKeyMappping.put(tableName, fieldPartitionMap);
                final Connection conn = dbAccessor.getPrimaryDbConnection();
                try {
                    keys = getPartitioningKeyPositions(tableName, conn, fieldPartitionMap, false);
                } finally {
                    JDBCUtils.closeQuietly(conn);
                }
            }
        }
        return keys;
    }

    private static final class PartitionKey {

        private final boolean isPrimary;
        private final int[] columnsPos;
        private final int partitionNo;

        PartitionKey(int[] columnsPos, boolean isPrimary, int partitionNo) {
            if(columnsPos == null) {
                throw new IllegalArgumentException();
            }
            if(columnsPos.length == 0) {
                throw new IllegalArgumentException();
            }
            this.columnsPos = columnsPos;
            this.isPrimary = isPrimary;
            this.partitionNo = partitionNo;
        }

        int getColumnPos(int index) {
            if(index != 0 && index >= columnsPos.length) {
                throw new IndexOutOfBoundsException("Index: " + index + ", Size: "
                        + columnsPos.length);
            }
            return columnsPos[index];
        }

    }

    private static Pair<int[], int[]> getPartitioningKeyPositions(@Nonnull final String tableName, @Nonnull final Connection conn, @Nonnull final Map<String, PartitionKey> fieldPartitionMap, final boolean returnNull)
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
        if(pkeyIdxs != null) {
            PartitionKey pkeyForPrimary = new PartitionKey(pkeyIdxs, true, 1 << shift);
            shift++;
            fieldPartitionMap.put(DummyFieldNameForPrimaryKey, pkeyForPrimary);
        }
        if(fkeyIdxs != null) {
            for(int i = 0; i < fkeyIdxs.length; i++) {
                int fkey = fkeyIdxs[i];
                String columnName = columnPosNameMapping.get(fkey);
                PartitionKey pkey = new PartitionKey(pkeyIdxs, false, 1 << shift);
                shift++;
                fieldPartitionMap.put(columnName, pkey);
            }
        }        
        return returnNull ? null : new Pair<int[], int[]>(pkeyIdxs, fkeyIdxs);
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

        boolean isValid() {
            return state.isValid();
        }
    }

    private NodeWithState wrapNode(final GridNode node, final boolean replace) {
        NodeWithState nodeWS = nodeStateMap.get(node);
        if(nodeWS == null || (replace && nodeWS.isValid() == false)) {
            nodeWS = new NodeWithState(node);
            nodeStateMap.put(node, nodeWS);
        }
        return nodeWS;
    }

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
}
