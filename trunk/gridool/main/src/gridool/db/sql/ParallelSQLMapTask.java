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
package gridool.db.sql;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.helpers.DBAccessor;
import gridool.db.sql.SQLTranslator.QueryString;
import gridool.locking.LockManager;
import gridool.replication.ReplicationManager;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.net.NetUtils;
import xbird.util.xfer.TransferUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ParallelSQLMapTask extends GridTaskAdapter {
    private static final long serialVersionUID = 2478800827882047565L;
    private static final Log LOG = LogFactory.getLog(ParallelSQLMapTask.class);

    private final GridNode taskMasterNode;
    private final int taskNumber;
    private final String query;
    private final String taskTableName;
    private final InetAddress dstAddr;
    private final int dstPort;

    // remote only
    @GridRegistryResource
    private transient GridResourceRegistry registry;
    @GridConfigResource
    private transient GridConfiguration config;

    // local only
    private transient DistributionCatalog catalog;

    @SuppressWarnings("unchecked")
    public ParallelSQLMapTask(@Nonnull GridJob job, @Nonnull GridNode masterNode, int taskNumber, @Nonnull String query, @Nonnull String taskTableName, @Nonnull InetSocketAddress retSockAddr, @Nonnull DistributionCatalog catalog) {
        super(job, true);
        this.taskMasterNode = masterNode;
        this.taskNumber = taskNumber;
        this.query = query;
        this.taskTableName = taskTableName;
        this.dstAddr = retSockAddr.getAddress();
        this.dstPort = retSockAddr.getPort();
        this.catalog = catalog;
    }

    @Override
    public List<GridNode> listFailoverCandidates(@Nullable GridNode localNode, @Nullable GridTaskRouter router) {
        GridNode[] slaves = catalog.getSlaves(taskMasterNode, DistributionCatalog.defaultDistributionKey);
        router.resolve(slaves);
        return Arrays.asList(slaves);
    }

    /**
     * Get slaves for this task master. Note that IP may change since last distribution in Dynamic IP settings.
     */
    @Nonnull
    GridNode[] getRegisteredSlaves() {
        return catalog.getSlaves(taskMasterNode, DistributionCatalog.defaultDistributionKey);
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Nonnull
    public GridNode getTaskMasterNode() {
        return taskMasterNode;
    }

    @Nonnull
    public String getTaskTableName() {
        return taskTableName;
    }

    @Override
    protected ParallelSQLMapTaskResult execute() throws GridException {
        assert (registry != null);

        final File tmpFile;
        try {
            tmpFile = File.createTempFile("PSQLMap" + taskNumber + '_', '_' + NetUtils.getLocalHostAddress());
        } catch (IOException e) {
            throw new GridException(e);
        }

        final QueryString[] queries = SQLTranslator.divideQuery(query, true);
        final String selectQuery;
        final boolean singleStatement = (queries.length == 1);
        if(singleStatement) {
            selectQuery = query;
        } else {
            for(QueryString qs : queries) {
                if(qs.isSelect()) {
                    selectQuery = qs.getQuery();
                    break;
                }
            }
            throw new IllegalStateException();
        }

        GridNode localNode = config.getLocalNode();
        GridNode senderNode = getSenderNode();
        assert (senderNode != null);
        final boolean useCreateTableAS = senderNode.equals(localNode);

        final int fetchedRows;
        final Connection dbConn = getDbConnection(taskMasterNode, registry);
        try {
            if(singleStatement) {
                dbConn.setAutoCommit(true);
                // #1 invoke COPY INTO file
                if(useCreateTableAS) {
                    LockManager lockMgr = registry.getLockManager();
                    ReadWriteLock rwlock = lockMgr.obtainLock(localNode);
                    fetchedRows = executeCreateTableAs(dbConn, selectQuery, taskTableName, rwlock);
                } else {
                    fetchedRows = executeCopyIntoFile(dbConn, selectQuery, tmpFile);
                }
            } else {
                dbConn.setAutoCommit(false);
                // #1-1 DDL before map SELECT queries (e.g., create view)
                issueDDLBeforeSelect(dbConn, queries);
                // #1-2 invoke COPY INTO file
                if(useCreateTableAS) {
                    LockManager lockMgr = registry.getLockManager();
                    ReadWriteLock rwlock = lockMgr.obtainLock(localNode);
                    fetchedRows = executeCreateTableAs(dbConn, selectQuery, taskTableName, rwlock);
                } else {
                    fetchedRows = executeCopyIntoFile(dbConn, selectQuery, tmpFile);
                }
                // #1-3 DDL after map SELECT queries (e.g., drop view)
                issueDDLAfterSelect(dbConn, queries);
                dbConn.commit();
            }
        } catch (SQLException e) {
            String errmsg = "Failed to execute a query: \n" + query;
            LOG.error(errmsg, e);
            if(singleStatement) {
                try {
                    dbConn.rollback();
                } catch (SQLException rbe) {
                    LOG.warn("Rollback failed", rbe);
                }
            }
            if(!tmpFile.delete()) {
                LOG.warn("Failed to delete a temp file: " + tmpFile.getAbsolutePath());
            }
            throw new GridException(errmsg, e);
        } finally {
            JDBCUtils.closeQuietly(dbConn);
        }
        assert (fetchedRows != -1);

        String sentFileName = null;
        if(fetchedRows > 0) {
            // #2 send file
            try {
                TransferUtils.sendfile(tmpFile, dstAddr, dstPort, true);
                sentFileName = tmpFile.getName();
            } catch (IOException e) {
                throw new GridException("failed to sending a file", e);
            } finally {
                if(!tmpFile.delete()) {
                    LOG.warn("Failed to delete a temp file: " + tmpFile.getAbsolutePath());
                }
            }
        }
        return new ParallelSQLMapTaskResult(taskMasterNode, sentFileName, taskNumber, fetchedRows);
    }

    private static int executeCopyIntoFile(@Nonnull final Connection conn, @Nonnull final String mapQuery, @Nonnull final File outFile)
            throws SQLException {
        if(!outFile.canWrite()) {// sanity check
            throw new IllegalStateException("File is not writable: " + outFile.getAbsolutePath());
        }
        assert (mapQuery.indexOf(';') == -1) : mapQuery;
        String filepath = outFile.getAbsolutePath();
        String copyIntoQuery = "COPY (" + mapQuery + ") INTO '" + filepath
                + "' USING DELIMITERS '|','\n','\"'";

        if(LOG.isInfoEnabled()) {
            LOG.info("Executing a Map SQL query: \n" + copyIntoQuery);
        }
        return JDBCUtils.update(conn, copyIntoQuery);
    }

    private static int executeCreateTableAs(@Nonnull final Connection conn, @Nonnull final String mapQuery, @Nonnull final String taskTableName, @Nonnull final ReadWriteLock rwlock)
            throws SQLException {
        assert (mapQuery.indexOf(';') == -1) : mapQuery;
        String ddl = "CREATE TABLE \"" + taskTableName + "\" AS (" + mapQuery + ") WITH DATA";
        if(LOG.isInfoEnabled()) {
            LOG.info("Executing a Map SQL query: \n" + ddl);
        }
        final Lock lock = rwlock.writeLock();
        try {
            lock.lock();
            JDBCUtils.update(conn, ddl);
        } finally {
            lock.unlock();
        }
        return -1;
    }

    private static void issueDDLBeforeSelect(@Nonnull final Connection conn, @Nonnull final QueryString[] queries)
            throws GridException {
        assert (queries.length > 1);
        final StringBuilder queryBuf = new StringBuilder(256);
        for(QueryString qs : queries) {
            if(qs.isSelect()) {
                break;
            } else {
                assert (qs.isDDL());
                queryBuf.append(qs.getQuery());
                queryBuf.append(";\n");
            }
        }
        if(queryBuf.length() == 0) {
            return;
        }
        final String query = queryBuf.toString();
        try {
            JDBCUtils.update(conn, query);
        } catch (SQLException e) {
            LOG.error("Failed to execute a query: " + query, e);
            throw new GridException(e);
        }
    }

    private static void issueDDLAfterSelect(@Nonnull final Connection conn, @Nonnull final QueryString[] queries)
            throws GridException {
        assert (queries.length > 1);
        final StringBuilder queryBuf = new StringBuilder(256);
        boolean foundSelect = false;
        for(QueryString qs : queries) {
            if(qs.isSelect()) {
                assert (foundSelect == false);
                foundSelect = true;
            } else {
                if(foundSelect) {
                    assert (qs.isDDL());
                    queryBuf.append(qs.getQuery());
                    queryBuf.append(";\n");
                }
            }
        }
        if(queryBuf.length() == 0) {
            return;
        }
        final String query = queryBuf.toString();
        try {
            JDBCUtils.update(conn, query);
        } catch (SQLException e) {
            LOG.error("Failed to execute a query: " + query, e);
            throw new GridException(e);
        }
    }

    @Nonnull
    private static Connection getDbConnection(final GridNode taskMasterNode, final GridResourceRegistry registry)
            throws GridException {
        DBAccessor dba = registry.getDbAccessor();
        ReplicationManager replMgr = registry.getReplicationManager();

        final Connection dbConn;
        GridNode localMaster = replMgr.getLocalMasterNode();
        if(taskMasterNode.equals(localMaster)) {
            dbConn = DBAccessor.getPrimaryDbConnection(dba, false);
        } else {
            final Connection primaryConn = DBAccessor.getPrimaryDbConnection(dba, false);
            try {
                String replicaDbName = replMgr.getReplicaDatabaseName(primaryConn, taskMasterNode);
                dbConn = dba.getConnection(replicaDbName);
            } catch (SQLException e) {
                LOG.error(e);
                throw new GridException(e);
            } finally {
                JDBCUtils.closeQuietly(primaryConn);
            }
        }
        return dbConn;
    }

    static final class ParallelSQLMapTaskResult implements Externalizable {

        @Nonnull
        private/* final */GridNode masterNode;
        @Nullable
        private/* final */String fileName;
        private/* final */int taskNumber;
        private/* final */int numRows;

        public ParallelSQLMapTaskResult() {}//Externalizable

        ParallelSQLMapTaskResult(@Nonnull GridNode masterNode, @Nullable String fileName, int taskNumber, int numRows) {
            assert (numRows >= 0) : numRows;
            this.masterNode = masterNode;
            this.fileName = fileName;
            this.taskNumber = taskNumber;
            this.numRows = numRows;
        }

        @Nonnull
        public GridNode getMasterNode() {
            return masterNode;
        }

        @Nullable
        public String getFileName() {
            return fileName;
        }

        @Nonnegative
        public int getTaskNumber() {
            return taskNumber;
        }

        public int getNumRows() {
            return numRows;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.masterNode = (GridNode) in.readObject();
            this.fileName = IOUtils.readString(in);
            this.taskNumber = in.readInt();
            this.numRows = in.readInt();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(masterNode);
            IOUtils.writeString(fileName, out);
            out.writeInt(taskNumber);
            out.writeInt(numRows);
        }

    }

}
