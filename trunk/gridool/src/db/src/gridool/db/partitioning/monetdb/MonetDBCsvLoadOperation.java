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
package gridool.db.partitioning.phihash.monetdb;

import gridool.GridException;
import gridool.GridNode;
import gridool.Settings;
import gridool.db.DBOperation;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.MonetDBUtils;
import gridool.db.partitioning.phihash.csv.distmm.InMemoryMappingIndex;
import gridool.dht.ILocalDirectory;
import gridool.dht.btree.IndexException;
import gridool.locking.LockManager;
import gridool.util.GridUtils;
import gridool.util.datetime.StopWatch;
import gridool.util.io.FileDeletionThread;
import gridool.util.io.IOUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.primitive.Primitives;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nonnegative;
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
public final class MonetDBCsvLoadOperation extends DBOperation {
    private static final long serialVersionUID = 2815346044185945907L;
    private static final Log LOG = LogFactory.getLog(MonetDBCsvLoadOperation.class);
    private static final String driverClassName = "nl.cwi.monetdb.jdbc.MonetDriver";

    private static final boolean ENV_MONETDB_USE_NOV09_OPT;
    private static final long ENV_COPYINTO_PIECES;
    private static final boolean ENV_RESTART_AFTER_COPYINTO;
    private static final long ENV_SLEEP_AFTER_RESTART;
    static {
        ENV_MONETDB_USE_NOV09_OPT = Boolean.parseBoolean(Settings.getThroughSystemProperty("gridool.db.monetdb.use_nov09_opt"));
        ENV_COPYINTO_PIECES = Primitives.parseLong(Settings.get("gridool.db.monetdb.copyinto_pieces"), 20000000L);
        ENV_RESTART_AFTER_COPYINTO = Boolean.parseBoolean(Settings.get("gridool.db.monetdb.restart_after_copyinto"));
        ENV_SLEEP_AFTER_RESTART = Primitives.parseLong(Settings.get("gridool.db.monetdb.sleep_after_restart"), -1L);
    }

    @Nonnull
    private/* final */String tableName;
    @Nonnull
    private/* final */String csvFileName;
    @Nonnull
    private/* final */String createTableDDL;
    @Nullable
    private/* final */String copyIntoQuery;
    private/* final */long expectedNumRecords = -1L;
    @Nullable
    private/* final */String alterTableDDL;

    public MonetDBCsvLoadOperation() {}

    public MonetDBCsvLoadOperation(@Nonnull String connectUrl, @Nonnull String tableName, @Nonnull String csvFileName, @Nonnull String createTableDDL, @Nullable String copyIntoQuery, @Nullable String alterTableDDL) {
        super(driverClassName, connectUrl);
        this.tableName = tableName;
        this.csvFileName = csvFileName;
        this.createTableDDL = createTableDDL;
        this.copyIntoQuery = copyIntoQuery;
        this.alterTableDDL = alterTableDDL;
    }

    public MonetDBCsvLoadOperation(@Nonnull MonetDBCsvLoadOperation ops, long numRecords) {
        super(driverClassName, ops.connectUrl);
        this.tableName = ops.tableName;
        this.csvFileName = ops.csvFileName;
        this.createTableDDL = ops.createTableDDL;
        this.copyIntoQuery = ops.copyIntoQuery;
        this.expectedNumRecords = numRecords;
        this.alterTableDDL = ops.alterTableDDL;
        this.userName = ops.userName;
        this.password = ops.password;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public String getCreateTableDDL() {
        return createTableDDL;
    }

    public String getCopyIntoQuery() {
        return copyIntoQuery;
    }

    public String getAlterTableDDL() {
        return alterTableDDL;
    }

    @Override
    public Long execute() throws SQLException, GridException {
        if(expectedNumRecords == -1L) {
            throw new IllegalStateException();
        }
        // clear index buffer
        final ILocalDirectory index = registry.getDirectory();
        try {
            index.purgeAll(true);
            index.setBulkloading(false);
        } catch (IndexException dbe) {
            LOG.error(dbe);
        }
        InMemoryMappingIndex mmidx = registry.getMappingIndex();
        mmidx.saveIndex(true, false);

        LockManager lockMgr = registry.getLockManager();
        //String dbid = getConnectionUrl();
        //ReadWriteLock systblLock = lockMgr.obtainLock(dbid);
        // [Design notes] Better performance can be expected for a single bulkload process than multiple one.
        ReadWriteLock systblLock = lockMgr.obtainLock(DBAccessor.SYS_TABLE_SYMBOL);

        long actualInserted = 0L;
        final Lock exlock = systblLock.writeLock();
        exlock.lock();
        final Connection conn = getConnection();
        final String dburl = getConnectionUrl();
        try {
            // #1 create table
            prepareTable(conn, createTableDDL, tableName);
            // #2 invoke COPY INTO
            final StopWatch sw = new StopWatch();
            if(expectedNumRecords > 0L) {
                if(ENV_MONETDB_USE_NOV09_OPT) {
                    JDBCUtils.update(conn, "SET optimizer='nov2009_pipe'");
                    actualInserted = invokeCopyInto(conn, copyIntoQuery, tableName, csvFileName, expectedNumRecords);
                    JDBCUtils.update(conn, "SET optimizer='default_pipe'");
                } else {
                    actualInserted = invokeCopyInto(conn, copyIntoQuery, tableName, csvFileName, expectedNumRecords);
                }
                if(actualInserted != expectedNumRecords) {
                    String errmsg = "Expected records (" + expectedNumRecords
                            + ") != Actual records (" + actualInserted + "): \n"
                            + getCopyIntoQuery(copyIntoQuery, expectedNumRecords);
                    LOG.error(errmsg);
                    throw new GridException(errmsg);
                }
                LOG.info("Elapsed time for COPY " + actualInserted + " RECORDS INTO " + tableName
                        + " [" + dburl + "]: " + sw.toString());
            }
            // #3 create indices and constraints
            if(alterTableDDL != null) {
                sw.start();
                alterTable(conn, alterTableDDL);
                LOG.info("Elapsed time for creating indices and constraints on table '" + tableName
                        + "': " + sw.toString());
            }
        } finally {
            JDBCUtils.closeQuietly(conn);
            if(ENV_RESTART_AFTER_COPYINTO && actualInserted > ENV_COPYINTO_PIECES) {
                if(MonetDBUtils.restartLocalDbInstance(dburl)) {
                    if(ENV_SLEEP_AFTER_RESTART > 0L) {
                        try {
                            Thread.sleep(ENV_SLEEP_AFTER_RESTART);
                        } catch (InterruptedException e) {
                            ;
                        }
                    }
                } else {
                    LOG.warn("failed to restart MonetDB instance: " + dburl);
                }
            }
            exlock.unlock();
        }
        return actualInserted;
    }

    private static void prepareTable(final Connection conn, final String createTableDDL, final String tableName)
            throws SQLException {
        final String sql = createTableDDL + "; ALTER TABLE \"" + tableName + "\" ADD \""
                + DistributionCatalog.hiddenFieldName + "\" "
                + DistributionCatalog.tableIdSQLDataType + ';';
        try {
            JDBCUtils.update(conn, sql);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            if(LOG.isDebugEnabled()) {
                LOG.debug("Table already exists. Try to truncate " + tableName, e);
            }
            // fall through
        }
    }

    private static long invokeCopyInto(final Connection conn, final String copyIntoQuery, final String tableName, final String fileName, final long numRecords)
            throws SQLException {
        final File loadFile = prepareLoadFile(fileName);
        final String queryTpl = complementCopyIntoQuery(copyIntoQuery, loadFile);
        long rtotal = 0;
        try {
            if(ENV_COPYINTO_PIECES > 0) {
                for(long offset = 0; offset < numRecords; offset += ENV_COPYINTO_PIECES) {
                    final long rest = numRecords - offset;
                    if(rest > 0) {
                        final String query;
                        if(rest > ENV_COPYINTO_PIECES) {
                            query = getCopyIntoQuery(queryTpl, ENV_COPYINTO_PIECES, offset);
                        } else {
                            query = getCopyIntoQuery(queryTpl, rest, offset);
                        }
                        final int ret = JDBCUtils.update(conn, query);
                        if(ret > 0) {
                            rtotal += ret;
                        } else {
                            LOG.warn("Unexpected result '" + ret + "' for query: " + query);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                String query = getCopyIntoQuery(queryTpl, numRecords);
                rtotal = JDBCUtils.update(conn, query);
            }
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction: " + queryTpl, e);
            conn.rollback();
            throw e;
        } finally {
            new FileDeletionThread(loadFile, LOG).start();
        }
        return rtotal;
    }

    private static String getCopyIntoQuery(@Nonnull final String query, final long numRecords) {
        return query.replaceFirst("COPY ", "COPY " + numRecords + " RECORDS ");
    }

    private static String getCopyIntoQuery(@Nonnull final String query, final long numRecords, @Nonnegative final long offset) {
        return query.replaceFirst("COPY ", "COPY " + numRecords + " OFFSET " + offset + " RECORDS ");
    }

    private static void alterTable(final Connection conn, final String sql) throws SQLException {
        try {
            JDBCUtils.update(conn, sql);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction", e);
            conn.rollback();
            throw e;
        }
    }

    private static File prepareLoadFile(final String fileName) {
        File colDir = GridUtils.getWorkDir(true);
        final File file = new File(colDir, fileName);
        if(!file.exists()) {
            throw new IllegalStateException("Loading file does not exist: "
                    + file.getAbsolutePath());
        }
        return file;
    }

    private static String complementCopyIntoQuery(final String query, final File loadFile) {
        String path = loadFile.getAbsolutePath();
        return query.replaceFirst("<src>", path);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.tableName = IOUtils.readString(in);
        this.csvFileName = IOUtils.readString(in);
        this.createTableDDL = IOUtils.readString(in);
        this.copyIntoQuery = IOUtils.readString(in);
        this.expectedNumRecords = in.readLong();
        this.alterTableDDL = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(tableName, out);
        GridNode masterNode = getMasterNode();
        if(masterNode == null) {
            IOUtils.writeString(csvFileName, out);
        } else {
            String altered = GridUtils.alterFileName(csvFileName, masterNode);
            IOUtils.writeString(altered, out);
        }
        IOUtils.writeString(createTableDDL, out);
        IOUtils.writeString(copyIntoQuery, out);
        out.writeLong(expectedNumRecords);
        IOUtils.writeString(alterTableDDL, out);
    }

}
