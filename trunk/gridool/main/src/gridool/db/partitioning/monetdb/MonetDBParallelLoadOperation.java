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
package gridool.db.partitioning.monetdb;

import gridool.GridException;
import gridool.GridNode;
import gridool.db.DBOperation;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.DBAccessor;
import gridool.directory.ILocalDirectory;
import gridool.locking.LockManager;
import gridool.util.GridUtils;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;
import xbird.util.datetime.StopWatch;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBParallelLoadOperation extends DBOperation {
    private static final long serialVersionUID = 2815346044185945907L;
    private static final Log LOG = LogFactory.getLog(MonetDBParallelLoadOperation.class);
    private static final String driverClassName = "nl.cwi.monetdb.jdbc.MonetDriver";

    @Nonnull
    private/* final */String tableName;
    @Nonnull
    private/* final */String csvFileName;
    @Nonnull
    private/* final */String createTableDDL;
    @Nullable
    private/* final */String copyIntoQuery;
    private/* final */int expectedNumRecords = -1;
    @Nullable
    private/* final */String alterTableDDL;

    public MonetDBParallelLoadOperation() {}

    public MonetDBParallelLoadOperation(@Nonnull String connectUrl, @Nonnull String tableName, @Nonnull String csvFileName, @Nonnull String createTableDDL, @Nullable String copyIntoQuery, @Nullable String alterTableDDL) {
        super(driverClassName, connectUrl);
        this.tableName = tableName;
        this.csvFileName = csvFileName;
        this.createTableDDL = createTableDDL;
        this.copyIntoQuery = copyIntoQuery;
        this.alterTableDDL = alterTableDDL;
    }

    public MonetDBParallelLoadOperation(@Nonnull MonetDBParallelLoadOperation ops, @Nullable String copyIntoQuery, int numRecords) {
        super(driverClassName, ops.connectUrl);
        this.tableName = ops.tableName;
        this.csvFileName = ops.csvFileName;
        this.createTableDDL = ops.createTableDDL;
        this.copyIntoQuery = copyIntoQuery;
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

    public String getCopyIntoQuery(final int numRecords) {
        assert (copyIntoQuery != null);
        assert (numRecords > 0) : numRecords;
        return copyIntoQuery.replaceFirst("COPY ", "COPY " + numRecords + " RECORDS ");
    }

    public String getAlterTableDDL() {
        return alterTableDDL;
    }

    @Override
    public Integer execute() throws SQLException, GridException {
        if(expectedNumRecords == -1) {
            throw new IllegalStateException();
        }
        // clear index buffer
        final ILocalDirectory index = registry.getDirectory();
        try {
            index.purgeAll(true);
            index.setBulkloading(false);
        } catch (DbException dbe) {
            LOG.error(dbe);
        }

        final LockManager lockMgr = registry.getLockManager();
        final ReadWriteLock systblLock = lockMgr.obtainLock(DBAccessor.SYS_TABLE_SYMBOL);
        final Connection conn = getConnection();
        int numInserted = 0;
        try {
            // #1 create table
            prepareTable(conn, createTableDDL, tableName, systblLock);
            // #2 invoke COPY INTO
            final StopWatch sw = new StopWatch();
            if(copyIntoQuery != null) {
                numInserted = invokeCopyInto(conn, copyIntoQuery, tableName, csvFileName, systblLock);
                if(numInserted != expectedNumRecords) {
                    String errmsg = "Expected records (" + expectedNumRecords
                            + ") != Actual records (" + numInserted + "): \n" + copyIntoQuery;
                    LOG.error(errmsg);
                    throw new GridException(errmsg);
                }
                LOG.info("Elapsed time for COPY " + numInserted + " RECORDS INTO " + tableName
                        + ": " + sw.toString());
            }
            // #3 create indices and constraints
            if(alterTableDDL != null) {
                sw.start();
                alterTable(conn, alterTableDDL, systblLock);
                LOG.info("Elapsed time for creating indices and constraints on table '" + tableName
                        + "': " + sw.toString());
            }
        } finally {
            JDBCUtils.closeQuietly(conn);
        }

        return numInserted;
    }

    private static void prepareTable(final Connection conn, final String createTableDDL, final String tableName, final ReadWriteLock rwlock)
            throws SQLException {
        final String sql = createTableDDL + "; ALTER TABLE \"" + tableName + "\" ADD \""
                + DistributionCatalog.hiddenFieldName + "\" "
                + DistributionCatalog.tableIdSQLDataType + ';';
        final Lock lock = rwlock.writeLock(); // exclusive lock for system table in MonetDB
        try {
            lock.lock();
            JDBCUtils.update(conn, sql);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            if(LOG.isDebugEnabled()) {
                LOG.debug("Table already exists. Try to truncate " + tableName, e);
            }
            truncateTable(conn, tableName);
            // fall through
        } finally {
            lock.unlock();
        }
    }

    private static void truncateTable(@Nonnull final Connection conn, @Nonnull final String tableName)
            throws SQLException {
        String dml = "DELETE FROM " + tableName;
        JDBCUtils.update(conn, dml);
    }

    private static int invokeCopyInto(final Connection conn, final String copyIntoQuery, final String tableName, final String fileName, final ReadWriteLock rwlock)
            throws SQLException {
        final File loadFile = prepareLoadFile(fileName);
        final String query = complementCopyIntoQuery(copyIntoQuery, loadFile);

        final Lock lock = rwlock.readLock(); // REVIEWME read lock for system table
        final int ret;
        try {
            lock.lock();
            ret = JDBCUtils.update(conn, query);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction", e);
            conn.rollback();
            throw e;
        } finally {
            lock.unlock();
            if(!loadFile.delete()) {
                LOG.warn("Could not remove a tempolary file: " + loadFile.getAbsolutePath());
            }
        }
        return ret;
    }

    private static void alterTable(final Connection conn, final String sql, final ReadWriteLock rwlock)
            throws SQLException {
        final Lock lock = rwlock.writeLock(); // exclusive lock for system table in MonetDB
        try {
            lock.lock();
            JDBCUtils.update(conn, sql);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("rollback a transaction", e);
            conn.rollback();
            throw e;
        } finally {
            lock.unlock();
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
        this.expectedNumRecords = in.readInt();
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
        out.writeInt(expectedNumRecords);
        IOUtils.writeString(alterTableDDL, out);
    }

}
