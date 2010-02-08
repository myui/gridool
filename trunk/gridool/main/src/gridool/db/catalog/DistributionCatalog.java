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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import xbird.config.Settings;
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
    public static final String hiddenFieldName;
    static {
        hiddenFieldName = Settings.get("gridool.db.hidden_fieldnam", "_hidden");
    }

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, Map<GridNode, List<GridNode>>> distributionCache;

    public DistributionCatalog() {
        this.distributionCache = new HashMap<String, Map<GridNode, List<GridNode>>>(12);
    }

    public void registerPartition(@Nonnull final GridNode master, @Nonnull final List<GridNode> slaves, @Nonnull final String distKey) {
        synchronized(lock) {
            Map<GridNode, List<GridNode>> mapping = distributionCache.get(distKey);
            if(mapping == null) {
                mapping = new HashMap<GridNode, List<GridNode>>(32);
                distributionCache.put(distKey, mapping);
                mapping.put(master, slaves);
            } else {
                List<GridNode> oldSlaves = mapping.get(mapping);
                if(oldSlaves == null) {
                    mapping.put(master, slaves);
                } else {
                    oldSlaves.addAll(slaves);
                }
            }
        }
    }

    public GridNode[] getMasters(@Nullable final String distKey) {
        final GridNode[] masters;
        synchronized(lock) {
            final Map<GridNode, List<GridNode>> mapping = distributionCache.get(distKey);
            if(mapping == null) {
                return new GridNode[0];
            }
            masters = ArrayUtils.toArray(mapping.keySet(), GridNode[].class);
        }
        return masters;
    }

    public GridNode[] getSlaves(@Nonnull final GridNode master, @Nullable final String distKey) {
        final GridNode[] slaves;
        synchronized(lock) {
            final Map<GridNode, List<GridNode>> mapping = distributionCache.get(distKey);
            if(mapping == null) {
                return new GridNode[0];
            }
            final List<GridNode> slaveList = mapping.get(master);
            if(slaveList == null) {
                return new GridNode[0];
            }
            slaves = ArrayUtils.toArray(slaveList, GridNode[].class);
        }
        return slaves;
    }

    public int getPartitioningKey(@Nonnull final String tableName, @Nonnull final String fieldName) {
        return -1;
    }

    @Nonnull
    public static Pair<int[], int[]> getPartitioningKeys(@Nonnull final Connection conn, @Nonnull final String tableName)
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
                rs.close();
            }
            Arrays.sort(idxs);
            fkeyIdxs = idxs;
        }

        return new Pair<int[], int[]>(pkeyIdxs, fkeyIdxs);
    }
}
