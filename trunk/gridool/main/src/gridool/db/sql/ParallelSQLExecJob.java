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

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.catalog.NodeState;
import gridool.db.helpers.DBAccessor;
import gridool.db.sql.ParallelSQLMapTask.ParallelSQLMapTaskResult;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.xfer.RecievedFileWriter;
import xbird.util.xfer.TransferRequestListener;
import xbird.util.xfer.TransferServer;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ParallelSQLExecJob extends GridJobBase<ParallelSQLExecJob.JobConf, String> {
    private static final long serialVersionUID = -3258710936720234846L;
    private static final Log LOG = LogFactory.getLog(ParallelSQLExecJob.class);

    private static final int DEFAULT_RECVFILE_WRITER_CONCURRENCY = 3;
    private static final int DEFAULT_COPYINTO_TABLE_CONCURRENCY = 2;

    // remote resources    
    @GridRegistryResource
    private transient GridResourceRegistry registry;

    // --------------------------------------------
    // local only resources    

    private transient String outputTableName;
    private transient boolean isOutputAsView;
    @Deprecated
    private transient int oneThirdOfTasks;
    private transient ExecutorService recvExecs;
    private transient ExecutorService copyintoExecs;

    // --------------------------------------------

    public ParallelSQLExecJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf) throws GridException {
        // phase #1 preparation
        DistributionCatalog catalog = registry.getDistributionCatalog();
        SQLTranslator translator = new SQLTranslator(catalog);
        final String mapQuery = translator.translateQuery(jobConf.mapQuery);
        final GridNode[] masters = catalog.getMasters(DistributionCatalog.defaultDistributionKey);
        DBAccessor dba = registry.getDbAccessor();
        String retTableName = jobConf.outputTableName;
        final boolean asView = jobConf.isOutputAsView();
        runPreparation(dba, mapQuery, masters, retTableName, asView);

        final InetSocketAddress sockAddr;
        final TransferServer xferServer = createTransferServer(jobConf.getRecvFileConcurrency());
        try {
            sockAddr = xferServer.setup();
        } catch (IOException e) {
            throw new GridException("failed to setup TransferServer", e);
        }

        // phase #2 map tasks
        final int numNodes = masters.length;
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        for(int tasknum = 0; tasknum < numNodes; tasknum++) {
            GridNode node = masters[tasknum];
            ParallelSQLMapTask task = new ParallelSQLMapTask(this, node, tasknum, mapQuery, sockAddr, catalog);
            map.put(task, node);
        }
        this.outputTableName = retTableName;
        this.isOutputAsView = asView;
        this.oneThirdOfTasks = Math.max(numNodes / 3, 2);
        this.recvExecs = startFileReciever(xferServer);
        this.copyintoExecs = ExecutorFactory.newFixedThreadPool(jobConf.getCopyIntoTableConcurrency(), "CopyIntoTableThread", false);
        return map;
    }

    private static void runPreparation(final DBAccessor dba, final String mapQuery, final GridNode[] masters, final String outputTableName, final boolean isOutputAsView)
            throws GridException {
        final String prepareQuery = constructInitQuery(mapQuery, masters, outputTableName, isOutputAsView);
        final Connection conn;
        try {
            conn = dba.getPrimaryDbConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            LOG.error("An error caused in the preparation phase", e);
            throw new GridException(e);
        }
        try {
            JDBCUtils.update(conn, prepareQuery);
        } catch (SQLException e) {
            LOG.error("An error caused in the preparation phase", e);
            throw new GridException(e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
    }

    private static String constructInitQuery(final String mapQuery, final GridNode[] masters, final String outputTableName, final boolean isOutputAsView) {
        if(masters.length == 0) {
            throw new IllegalArgumentException();
        }
        final StringBuilder buf = new StringBuilder(512);
        buf.append("CREATE VIEW ");
        final String tmpViewName = "tmp" + outputTableName;
        buf.append(tmpViewName);
        buf.append(" AS (\n");
        buf.append(mapQuery);
        buf.append(");\n");
        final int numTasks = masters.length;
        for(int i = 0; i < numTasks; i++) {
            buf.append("CREATE TABLE ");
            buf.append(tmpViewName);
            buf.append("task");
            buf.append(i);
            buf.append(" (LIKE ");
            buf.append(tmpViewName);
            buf.append(");\n");
        }
        buf.append("CREATE VIEW ");
        if(isOutputAsView) {
            buf.append(getViewTableName(outputTableName));
        } else {
            buf.append(outputTableName);
        }
        buf.append(" AS (\n");
        final int lastTask = numTasks - 1;
        for(int i = 0; i < lastTask; i++) {
            buf.append("SELECT * FROM ");
            buf.append(tmpViewName);
            buf.append("task");
            buf.append(i);
            buf.append(" UNION ALL \n");
        }
        buf.append("SELECT * FROM ");
        buf.append(tmpViewName);
        buf.append("task");
        buf.append(lastTask);
        buf.append("\n);");
        return buf.toString();
    }

    private static String getViewTableName(String tableName) {
        return "v_" + tableName;
    }

    private static TransferServer createTransferServer(@Nonnegative int concurrency) {
        DbCollection rootCol = DbCollection.getRootCollection();
        File colDir = rootCol.getDirectory();
        TransferRequestListener listener = new RecievedFileWriter(colDir);
        return new TransferServer(concurrency, listener);
    }

    private static ExecutorService startFileReciever(TransferServer server) {
        ExecutorService execServ = ExecutorFactory.newSingleThreadExecutor("FileReceiver", false);
        execServ.submit(server);
        return execServ;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final ParallelSQLMapTaskResult taskResult = result.getResult();
        if(taskResult == null) {
            // on task failure
            if(LOG.isWarnEnabled()) {
                GridException err = result.getException();
                LOG.warn("task failed: " + result.getTaskId(), err);
            }
            DistributionCatalog catalog = registry.getDistributionCatalog();
            GridNode failedNode = result.getExecutedNode();
            assert (failedNode != null);
            catalog.setNodeState(failedNode, NodeState.suspected);
            return GridTaskResultPolicy.FAILOVER;
        }

        // # 3 invoke COPY INTO table
        final DBAccessor dba = registry.getDbAccessor();
        copyintoExecs.execute(new Runnable() {
            public void run() {
                try {
                    invokeCopyIntoTable(taskResult, outputTableName, dba);
                } catch (GridException e) {
                    LOG.error(e);
                    throw new IllegalStateException("Copy Into table failed: " + outputTableName, e);
                }
            }
        });

        return GridTaskResultPolicy.CONTINUE;
    }

    private static void invokeCopyIntoTable(@Nonnull final ParallelSQLMapTaskResult result, @Nonnull final String outputTableName, @Nonnull final DBAccessor dba)
            throws GridException {
        final String sql = constructCopyIntoQuery(result, outputTableName);
        final Connection conn = GridUtils.getPrimaryDbConnection(dba);
        try {
            conn.setAutoCommit(false);
            JDBCUtils.update(conn, sql);
            conn.commit();
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException("failed to execute a query: " + sql, e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
    }

    private static String constructCopyIntoQuery(final ParallelSQLMapTaskResult result, final String outputTableName) {
        int taskNum = result.getTaskNumber();
        String tableName = getTemporaryTaskTableName(outputTableName, taskNum);
        String fileName = result.getFileName();
        String filePath = getLoadFilePath(fileName);
        int records = result.getNumRows();
        return "COPY " + records + " RECORDS INTO '" + tableName + "' FROM '" + filePath + '\'';
    }

    private static String getTemporaryTaskTableName(String retTableName, int taskNumber) {
        return "tmp" + retTableName + "task" + taskNumber;
    }

    private static String getLoadFilePath(String fileName) {
        DbCollection rootCol = DbCollection.getRootCollection();
        File colDir = rootCol.getDirectory();
        File file = new File(colDir, fileName);
        if(!file.exists()) {
            throw new IllegalStateException("File does not exist: " + file.getAbsolutePath());
        }
        return file.getAbsolutePath();
    }

    public String reduce() throws GridException {
        // wait for all 'COPY INTO table' queries finish
        ExecutorUtils.shutdownAndAwaitTermination(copyintoExecs);
        // cancel remaining tasks (for speculative execution)  
        recvExecs.shutdownNow();

        // invoke final aggregation query
        if(!isOutputAsView) {
            final DBAccessor dba = registry.getDbAccessor();
            invokeReduceQuery(outputTableName, dba);
            // TODO clear up tmp resources in other thread
        }
        
        return outputTableName;
    }

    private static void invokeReduceQuery(@Nonnull final String outputTableName, @Nonnull final DBAccessor dba)
            throws GridException {
        String inputViewName = getViewTableName(outputTableName);
        final String reduceQuery = "CREATE TABLE " + outputTableName + " AS (SELECT * FROM "
                + inputViewName + ") WITH DATA";

        final Connection conn = GridUtils.getPrimaryDbConnection(dba);
        try {
            JDBCUtils.update(conn, reduceQuery);
        } catch (SQLException e) {
            String errmsg = "failed running a reduce query: " + reduceQuery;
            LOG.error(errmsg, e);
            throw new GridException(errmsg, e);
        } finally {
            JDBCUtils.closeQuietly(conn);
        }
    }

    public static final class JobConf implements Externalizable {

        @Nonnull
        private String outputTableName;
        @Nonnull
        private String mapQuery;
        @Nonnull
        private String reduceQuery;

        private boolean outputAsView;
        @Deprecated
        private long waitForStartSpeculativeTask; // TODO

        private int recvFileConcurrency = DEFAULT_RECVFILE_WRITER_CONCURRENCY;
        private int copyIntoTableConcurrency = DEFAULT_COPYINTO_TABLE_CONCURRENCY;

        public JobConf() {}//Externalizable

        public JobConf(@Nonnull String mapQuery, @Nonnull String reduceQuery) {
            this(null, mapQuery, reduceQuery, false, -1L);
        }

        public JobConf(@Nullable String outputTableName, @Nonnull String mapQuery, @Nonnull String reduceQuery, boolean outputAsView, long waitForStartSpeculativeTask) {
            this.outputTableName = (outputTableName == null) ? GridUtils.generateQueryName()
                    : outputTableName;
            this.mapQuery = mapQuery;
            this.reduceQuery = reduceQuery;
            this.outputAsView = outputAsView;
            this.waitForStartSpeculativeTask = waitForStartSpeculativeTask;
        }

        public boolean isOutputAsView() {
            return outputAsView;
        }

        public void setOutputAsView(boolean outputAsView) {
            this.outputAsView = outputAsView;
        }

        public long getWaitForStartSpeculativeTask() {
            return waitForStartSpeculativeTask;
        }

        public void setWaitForStartSpeculativeTask(long waitForStartSpeculativeTask) {
            this.waitForStartSpeculativeTask = waitForStartSpeculativeTask;
        }

        public int getRecvFileConcurrency() {
            return recvFileConcurrency;
        }

        public void setRecvFileConcurrency(int recvFileConcurrency) {
            this.recvFileConcurrency = recvFileConcurrency;
        }

        public int getCopyIntoTableConcurrency() {
            return copyIntoTableConcurrency;
        }

        public void setCopyIntoTableConcurrency(int copyIntoTableConcurrency) {
            this.copyIntoTableConcurrency = copyIntoTableConcurrency;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.outputTableName = IOUtils.readString(in);
            this.mapQuery = IOUtils.readString(in);
            this.reduceQuery = IOUtils.readString(in);
            this.outputAsView = in.readBoolean();
            this.waitForStartSpeculativeTask = in.readLong();
            this.recvFileConcurrency = in.readInt();
            this.copyIntoTableConcurrency = in.readInt();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(outputTableName, out);
            IOUtils.writeString(mapQuery, out);
            IOUtils.writeString(reduceQuery, out);
            out.writeBoolean(outputAsView);
            out.writeLong(waitForStartSpeculativeTask);
            out.writeInt(recvFileConcurrency);
            out.writeInt(copyIntoTableConcurrency);
        }

    }

}
