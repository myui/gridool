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
package gridool.db.helpers;

import gridool.GridException;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.dba.DBAccessor;
import gridool.db.partitioning.phihash.DBPartitioningJobConf;
import gridool.db.partitioning.phihash.NodeWithPartitionNo;
import gridool.db.partitioning.phihash.monetdb.MonetDBCsvLoadOperation;
import gridool.db.partitioning.phihash.monetdb.MonetDBInvokeCsvLoadJob;
import gridool.dfs.GridXferClient;
import gridool.util.GridUtils;
import gridool.util.collections.LRUMap;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.primitive.MutableLong;
import gridool.util.string.StringUtils;
import gridool.util.struct.Pair;
import gridool.util.xfer.RunnableFileSender;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridDbUtils {
    private static final Log LOG = LogFactory.getLog(GridDbUtils.class);

    private GridDbUtils() {}

    @Nonnull
    public static Connection getPrimaryDbConnection(final DBAccessor dba, boolean autoCommit)
            throws GridException {
        final Connection conn;
        try {
            conn = dba.getPrimaryDbConnection();
            if(conn.getAutoCommit() != autoCommit) {
                conn.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException("failed connecting to the primary database: "
                    + dba.getPrimaryDbName());
        }
        return conn;
    }

    @Nonnull
    public static Pair<PrimaryKey, Collection<ForeignKey>> getPrimaryForeignKeys(@Nonnull final DBAccessor dba, @Nonnull final String templateTableName)
            throws GridException {
        final PrimaryKey pkey;
        final Collection<ForeignKey> fkeys;
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
        try {
            pkey = GridDbUtils.getPrimaryKey(conn, templateTableName, true);
            fkeys = GridDbUtils.getForeignKeys(conn, templateTableName, true);
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException(e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
        if(pkey == null) {
            throw new IllegalStateException("Primary key is not defined on table: "
                    + templateTableName);
        }
        return new Pair<PrimaryKey, Collection<ForeignKey>>(pkey, fkeys);
    }

    @Nullable
    public static PrimaryKey getPrimaryKey(@Nonnull final Connection conn, @CheckForNull final String pkTableName, final boolean reserveAdditionalInfo)
            throws SQLException {
        if(pkTableName == null) {
            throw new IllegalArgumentException();
        }
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final ResultSet rs = metadata.getPrimaryKeys(catalog, null, pkTableName);
        final PrimaryKey pkey;
        try {
            if(!rs.next()) {
                return null;
            }
            String pkName = rs.getString("PK_NAME");
            pkey = new PrimaryKey(pkName, pkTableName);
            do {
                pkey.addColumn(rs);
            } while(rs.next());
        } finally {
            rs.close();
        }

        if(reserveAdditionalInfo) {
            // set foreign key column positions
            pkey.setColumnPositions(metadata);
            // set exported keys
            final Collection<ForeignKey> exportedKeys = getExportedKeys(conn, pkTableName, false);
            if(!exportedKeys.isEmpty()) {
                final List<String> pkColumnsProbe = pkey.getColumnNames();
                final int numPkColumnsProbe = pkColumnsProbe.size();
                final List<ForeignKey> exportedKeyList = new ArrayList<ForeignKey>(4);
                outer: for(ForeignKey fk : exportedKeys) {
                    List<String> names = fk.getPkColumnNames();
                    if(names.size() != numPkColumnsProbe) {
                        continue;
                    }
                    for(String name : names) {
                        if(!pkColumnsProbe.contains(name)) {
                            continue outer;
                        }
                    }
                    exportedKeyList.add(fk);
                }
                if(!exportedKeyList.isEmpty()) {
                    pkey.setExportedKeys(exportedKeyList);
                }
            }
        }
        return pkey;
    }

    /**
     * @return column position is not provided in the returning foreign keys
     */
    @Nonnull
    public static Collection<ForeignKey> getExportedKeys(@Nonnull final Connection conn, @Nullable final String pkTableName, final boolean setColumnPositions)
            throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final Map<String, ForeignKey> mapping = new HashMap<String, ForeignKey>(4);
        final ResultSet rs = metadata.getExportedKeys(catalog, null, pkTableName);
        try {
            while(rs.next()) {
                final String fkName = rs.getString("FK_NAME");
                ForeignKey fk = mapping.get(fkName);
                if(fk == null) {
                    String fkTableName = rs.getString("FKTABLE_NAME");
                    fk = new ForeignKey(fkName, fkTableName, pkTableName);
                    mapping.put(fkName, fk);
                }
                fk.addColumn(rs, metadata);
            }
        } finally {
            rs.close();
        }
        final Collection<ForeignKey> fkeys = mapping.values();
        if(setColumnPositions) {
            for(ForeignKey fk : fkeys) {
                fk.setColumnPositions(metadata);
            }
        }
        return fkeys;
    }

    public static boolean hasParentTable(@Nonnull final Connection conn, @Nullable final String pkTableName)
            throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final ResultSet rs = metadata.getExportedKeys(catalog, null, pkTableName);
        try {
            return rs.next();
        } finally {
            rs.close();
        }
    }

    public static boolean hasParentTable(@Nonnull final PrimaryKey childTablePk) {
        List<ForeignKey> fkeys = childTablePk.getExportedKeys();
        if(fkeys == null || fkeys.isEmpty()) {
            return false;
        }
        return true;
    }

    public static boolean hasParentTableExportedKey(@Nonnull final DBAccessor dba, @Nonnull final PrimaryKey childTablePk)
            throws GridException {
        List<ForeignKey> fkeys = childTablePk.getExportedKeys();
        if(fkeys == null || fkeys.isEmpty()) {
            return false;
        }
        ForeignKey fk = fkeys.get(0);
        String parentTable = fk.getTableName();
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, false);
        final boolean hasParent;
        try {
            hasParent = GridDbUtils.hasParentTable(conn, parentTable);
        } catch (SQLException e) {
            LOG.error("Failed to find parent table: " + parentTable, e);
            throw new GridException(e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
        return hasParent;
    }

    /**
     * @return column position is provided in the returning foreign keys
     */
    @Nonnull
    public static Collection<ForeignKey> getForeignKeys(@Nonnull final Connection conn, @Nullable final String fkTableName, final boolean setColumnPositions)
            throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String catalog = conn.getCatalog();
        final Map<String, ForeignKey> mapping = new HashMap<String, ForeignKey>(4);
        final ResultSet rs = metadata.getImportedKeys(catalog, null, fkTableName);
        try {
            while(rs.next()) {
                final String fkName = rs.getString("FK_NAME");
                ForeignKey fk = mapping.get(fkName);
                if(fk == null) {
                    //String fkTableName = rs.getString("FKTABLE_NAME");
                    String pkTableName = rs.getString("PKTABLE_NAME");
                    fk = new ForeignKey(fkName, fkTableName, pkTableName);
                    mapping.put(fkName, fk);
                }
                fk.addColumn(rs, metadata);
            }
        } finally {
            rs.close();
        }
        final Collection<ForeignKey> fkeys = mapping.values();
        if(setColumnPositions) {
            for(ForeignKey fk : fkeys) {
                fk.setColumnPositions(metadata);
            }
        }
        return fkeys;
    }

    public static String getCombinedColumnName(final List<String> columnNames, final boolean sortByName) {
        final int size = columnNames.size();
        if(size == 0) {
            throw new IllegalArgumentException();
        }
        if(size == 1) {
            return columnNames.get(0);
        }
        if(sortByName) {
            Collections.sort(columnNames);
        }
        final StringBuilder buf = new StringBuilder(32);
        for(int i = 0; i < size; i++) {
            if(i != 0) {
                buf.append('&');
            }
            String name = columnNames.get(i);
            buf.append(name);
        }
        return buf.toString();
    }

    public static String makeCopyIntoFileQuery(final String subquery, final String outputFilePath) {
        return "COPY (" + subquery + ") INTO '" + outputFilePath
                + "' USING DELIMITERS '|','\n','\"'";
    }

    public static String makeCopyIntoTableQuery(final String tableName, final String inputFilePath, final int expectedRecords) {
        if(expectedRecords <= 0) {
            throw new IllegalArgumentException("Illegal expected records: " + expectedRecords);
        }
        return "COPY " + expectedRecords + " RECORDS INTO \"" + tableName + "\" FROM '"
                + inputFilePath + "' USING DELIMITERS '|','\n','\"'";
    }

    public static String combineFields(@Nonnull final String[] fields, final int numFields, @Nonnull final StringBuilder buf) {
        if(numFields < 1) {
            throw new IllegalArgumentException("Illegal numField: " + numFields);
        }
        if(numFields == 1) {
            return fields[0];
        }
        StringUtils.clear(buf);
        buf.append(fields[0]);
        for(int i = 1; i < numFields; i++) {
            buf.append('|');
            buf.append(fields[i]);
        }
        return buf.toString();
    }

    public static String[] getFkIndexNames(final Collection<ForeignKey> fkeys, final int numFkeys) {
        final String[] idxNames = new String[numFkeys];
        final Iterator<ForeignKey> itor = fkeys.iterator();
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = itor.next();
            String fkTable = fk.getFkTableName();
            List<String> fkColumns = fk.getFkColumnNames();
            idxNames[i] = GridDbUtils.getIndexName(fkTable, fkColumns);
        }
        return idxNames;
    }

    public static int[][] getFkPositions(final Collection<ForeignKey> fkeys, final int numFkeys) {
        final int[][] positions = new int[numFkeys][];
        final Iterator<ForeignKey> itor = fkeys.iterator();
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = itor.next();
            int[] fkeyPos = fk.getFkColumnPositions(true);
            positions[i] = fkeyPos;
        }
        return positions;
    }

    @Nullable
    public static String[] getParentTableFkIndexNames(final PrimaryKey childTablePkey) {
        final List<ForeignKey> parentFkeys = childTablePkey.getExportedKeys();
        if(parentFkeys == null) {
            return null;
        }
        final int numParents = parentFkeys.size();
        if(numParents == 0) {
            return null;
        }
        final String[] idxNames = new String[numParents];
        for(int i = 0; i < numParents; i++) {
            ForeignKey parentFkey = parentFkeys.get(i);
            String fkTable = parentFkey.getFkTableName();
            List<String> fkColumns = parentFkey.getFkColumnNames();
            String idxName = GridDbUtils.getIndexName(fkTable, fkColumns);
            idxNames[i] = idxName;
        }
        return idxNames;
    }

    public static int getMaxColumnCount(@Nonnull final PrimaryKey pkey, @Nonnull final Collection<ForeignKey> fkeys) {
        int max = pkey.getColumnNames().size();
        for(final ForeignKey fkey : fkeys) {
            int size = fkey.getColumnNames().size();
            max = Math.max(size, max);
        }
        return max;
    }

    public static int[] getChildTablesPartitionNo(final Collection<ForeignKey> fkeys, final int numFkeys, final DistributionCatalog catalog) {
        final int[] partitionNo = new int[numFkeys];
        final Iterator<ForeignKey> itor = fkeys.iterator();
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = itor.next();
            String tblname = fk.getPkTableName();
            int n = catalog.getTablePartitionNo(tblname, true);
            partitionNo[i] = n;
        }
        return partitionNo;
    }

    public static String getIndexName(final String tableName, final List<String> columnNames) {
        final int numColumns = columnNames.size();
        if(numColumns == 0) {
            throw new IllegalArgumentException("No columns was specified for table: " + tableName);
        }
        final StringBuilder buf = new StringBuilder(32);
        buf.append(tableName);
        buf.append('.');
        for(int i = 0; i < numColumns; i++) {
            if(i != 0) {
                buf.append('_');
            }
            String colname = columnNames.get(i);
            buf.append(colname);
        }
        buf.append(".fktbl");
        return buf.toString();
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static LRUMap<String, List<NodeWithPartitionNo>>[] getFkIndexCaches(final int numFks) {
        if(numFks == 0) {
            return null;
        }
        final LRUMap<String, List<NodeWithPartitionNo>>[] caches = new LRUMap[numFks];
        for(int i = 0; i < numFks; i++) {
            caches[i] = new LRUMap<String, List<NodeWithPartitionNo>>(1000);
        }
        return caches;
    }

    public static String generateCopyIntoQuery(final String tableName, final DBPartitioningJobConf jobConf) {
        return "COPY INTO \"" + tableName + "\" FROM '<src>' USING DELIMITERS '"
                + jobConf.getFieldSeparator() + "', '" + jobConf.getRecordSeparator() + "', '"
                + jobConf.getStringQuote() + '\'';
    }

    public static void sendfile(final ExecutorService sencExecs, final GridXferClient dfsClient, final String fileName, final FastByteArrayOutputStream data, final GridNode dstNode, final int dstPort) {
        InetAddress dstAddr = dstNode.getPhysicalAdress();
        Runnable run = new RunnableFileSender(data, fileName, null, dstAddr, dstPort, true, true);
        sencExecs.submit(run);

        String alteredFileName = GridUtils.alterFileName(fileName, dstNode);
        List<GridNode> replicas = dstNode.getReplicas();
        for(GridNode replicaNode : replicas) {
            dstAddr = replicaNode.getPhysicalAdress();
            run = new RunnableFileSender(data, alteredFileName, null, dstAddr, dstPort, true, true);
            sencExecs.submit(run);
        }
    }

    public static long invokeCsvLoadJob(final GridKernel kernel, final String csvFileName, final HashMap<GridNode, MutableLong> assignMap, final DBPartitioningJobConf jobConf) {
        String connectUrl = jobConf.getConnectUrl();
        String tableName = jobConf.getTableName();
        String createTableDDL = jobConf.getCreateTableDDL();
        String copyIntoQuery = generateCopyIntoQuery(tableName, jobConf);
        String alterTableDDL = jobConf.getAlterTableDDL();

        MonetDBCsvLoadOperation ops = new MonetDBCsvLoadOperation(connectUrl, tableName, csvFileName, createTableDDL, copyIntoQuery, alterTableDDL);
        ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
        final Pair<MonetDBCsvLoadOperation, Map<GridNode, MutableLong>> pair = new Pair<MonetDBCsvLoadOperation, Map<GridNode, MutableLong>>(ops, assignMap);

        final GridJobFuture<Long> future = kernel.execute(MonetDBInvokeCsvLoadJob.class, pair);
        final Long numInserted;
        try {
            numInserted = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie.getMessage(), ie);
            throw new IllegalStateException(ie);
        } catch (ExecutionException ee) {
            LOG.error(ee.getMessage(), ee);
            throw new IllegalStateException(ee);
        }
        return (numInserted == null) ? -1L : numInserted.longValue();
    }

    public static String generateCsvFileName(final DBPartitioningJobConf jobConf, final GridNode senderNode) {
        String tblName = jobConf.getTableName();
        String addr = senderNode.getPhysicalAdress().getHostAddress();
        return tblName + addr + ".csv";
    }

}
