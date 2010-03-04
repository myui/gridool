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
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.catalog.NodeState;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.GridDbUtils;
import gridool.db.sql.ParallelSQLMapTask.ParallelSQLMapTaskResult;
import gridool.db.sql.SQLTranslator.QueryString;
import gridool.locking.LockManager;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;
import xbird.util.csv.CsvWriter;
import xbird.util.io.IOUtils;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.jdbc.ResultSetHandler;
import xbird.util.net.NetUtils;
import xbird.util.string.StringUtils;

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

    private static final int DEFAULT_COPYINTO_TABLE_CONCURRENCY = 2;
    private static final float DEFAULT_INVOKE_SPECULATIVE_TASKS_FACTOR = 0.75f;
    private static final String TMP_TABLE_NAME_PREFIX = "_tmp";
    private static final String MOCK_TABLE_NAME_PREFIX = "_mock";
    private static final String MONETDB_NULL_STRING = "";

    // remote resources    
    @GridRegistryResource
    private transient GridResourceRegistry registry;
    @GridConfigResource
    private transient GridConfiguration config;

    // --------------------------------------------
    // local only resources    

    private transient Map<String, ParallelSQLMapTask> remainingTasks;
    private transient Set<GridNode> finishedNodes;

    private transient String outputName;
    private transient String createMergeViewDDL;
    private transient String reduceQuery;
    private transient OutputMethod outputMethod;
    private transient String destroyQuery;
    private transient long mapStarted;
    private transient long waitForStartSpeculativeTask;
    private transient int thresholdForSpeculativeTask;
    //private transient ExecutorService recvExecs;
    private transient ExecutorService copyintoExecs;

    // --------------------------------------------

    public ParallelSQLExecJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(final GridTaskRouter router, final JobConf jobConf)
            throws GridException {
        // phase #1 preparation (prepare tables)
        DistributionCatalog catalog = registry.getDistributionCatalog();
        final SQLTranslator translator = new SQLTranslator(catalog);
        final String mapQuery = translator.translateQuery(jobConf.mapQuery);
        final GridNode[] masters = catalog.getMasters(DistributionCatalog.defaultDistributionKey);
        router.resolve(masters);
        final int numNodes = masters.length;
        if(numNodes == 0) {
            return Collections.emptyMap();
        }

        DBAccessor dba = registry.getDbAccessor();
        LockManager lockMgr = registry.getLockManager();
        GridNode localNode = config.getLocalNode();
        ReadWriteLock rwlock = lockMgr.obtainLock(DBAccessor.SYS_TABLE_SYMBOL);
        final String outputName = jobConf.getOutputName();
        runPreparation(dba, mapQuery, masters, localNode, rwlock, outputName);

        // phase #2 map tasks
        final InetAddress localHostAddr = NetUtils.getLocalHost();
        final int port = config.getFileReceiverPort();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        final Map<String, ParallelSQLMapTask> reverseMap = new HashMap<String, ParallelSQLMapTask>(numNodes);
        for(int tasknum = 0; tasknum < numNodes; tasknum++) {
            GridNode node = masters[tasknum];
            String taskTableName = getTaskResultTableName(outputName, tasknum);
            ParallelSQLMapTask task = new ParallelSQLMapTask(this, node, tasknum, mapQuery, taskTableName, localHostAddr, port, catalog);
            map.put(task, node);
            String taskId = task.getTaskId();
            reverseMap.put(taskId, task);
        }

        this.remainingTasks = reverseMap;
        this.finishedNodes = new HashSet<GridNode>(numNodes);
        this.outputName = outputName;
        this.createMergeViewDDL = constructMergeViewDDL(masters, outputName);
        this.reduceQuery = getReduceQuery(jobConf.reduceQuery, outputName, translator);
        this.outputMethod = jobConf.getOutputMethod();
        this.destroyQuery = constructDestroyQuery(masters, outputName, outputMethod);
        this.mapStarted = System.currentTimeMillis();
        this.waitForStartSpeculativeTask = (numNodes <= 1) ? -1L
                : jobConf.getWaitForStartSpeculativeTask();
        this.thresholdForSpeculativeTask = Math.max(1, (int) (numNodes * jobConf.getInvokeSpeculativeTasksFactor()));
        this.copyintoExecs = ExecutorFactory.newFixedThreadPool(jobConf.getCopyIntoTableConcurrency(), "CopyIntoTableThread", true);

        if(LOG.isDebugEnabled()) {
            LOG.debug("\n[Map query]\n" + mapQuery + "\n[Reduce query]\n" + reduceQuery);
        }
        return map;
    }

    private static void runPreparation(@Nonnull final DBAccessor dba, @Nonnull final String mapQuery, @Nonnull final GridNode[] masters, @Nonnull final GridNode localNode, @Nonnull final ReadWriteLock rwlock, @Nonnull final String outputName)
            throws GridException {
        final String prepareQuery = constructTaskResultTablesDDL(mapQuery, masters, localNode, outputName);
        final Connection conn;
        try {
            conn = dba.getPrimaryDbConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            LOG.error("An error caused in the preparation phase", e);
            throw new GridException(e);
        }
        final Lock wlock = rwlock.writeLock();
        try {
            wlock.lock();
            JDBCUtils.update(conn, prepareQuery);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("An error caused in the preparation phase", e);
            try {
                conn.rollback();
            } catch (SQLException sqle) {
                LOG.warn("Failed to rollback", sqle);
            }
            throw new GridException(e);
        } finally {
            wlock.unlock();
            JDBCUtils.closeQuietly(conn);
        }
    }

    @Nonnull
    private static String constructTaskResultTablesDDL(final String mapQuery, final GridNode[] masters, final GridNode localNode, final String outputName) {
        if(masters.length == 0) {
            throw new IllegalArgumentException();
        }
        String mockSelectQuery;
        if(StringUtils.countMatches(mapQuery, ';') > 1) {
            QueryString[] queries = SQLTranslator.divideQuery(mapQuery, true);
            mockSelectQuery = (queries.length == 1) ? mapQuery
                    : SQLTranslator.selectFirstSelectQuery(queries);
        } else {
            mockSelectQuery = mapQuery;
        }

        final String unionViewName = getMergeViewName(outputName);
        final StringBuilder buf = new StringBuilder(512);
        buf.append("CREATE VIEW \"");
        final String mockViewName = getMockTableName(outputName);
        buf.append(mockViewName);
        buf.append("\" AS (\n");
        buf.append(mockSelectQuery);
        buf.append("\n);\n");
        final int numTasks = masters.length;
        for(int i = 0; i < numTasks; i++) {
            GridNode node = masters[i];
            if(!node.equals(localNode)) {
                buf.append("CREATE TABLE \"");
                buf.append(unionViewName);
                buf.append("task");
                buf.append(i);
                buf.append("\" (LIKE \"");
                buf.append(mockViewName);
                buf.append("\");");
            }
        }
        return buf.toString();
    }

    @Nonnull
    private static String constructMergeViewDDL(final GridNode[] masters, final String outputName) {
        if(masters.length == 0) {
            throw new IllegalArgumentException();
        }
        final String unionViewName = getMergeViewName(outputName);
        final StringBuilder buf = new StringBuilder(512);
        buf.append("CREATE VIEW \"");
        buf.append(unionViewName);
        buf.append("\" AS (\n");
        final int lastTask = masters.length - 1;
        for(int i = 0; i < lastTask; i++) {
            buf.append("SELECT * FROM \"");
            buf.append(unionViewName);
            buf.append("task");
            buf.append(i);
            buf.append("\" UNION ALL \n");
        }
        buf.append("SELECT * FROM \"");
        buf.append(unionViewName);
        buf.append("task");
        buf.append(lastTask);
        buf.append("\"\n)");
        return buf.toString();
    }

    @Nonnull
    private static String constructDestroyQuery(@Nonnull final GridNode[] masters, @Nonnull final String outputName, @Nonnull final OutputMethod outputMethod) {
        if(masters.length == 0) {
            throw new IllegalArgumentException();
        }
        final StringBuilder buf = new StringBuilder(512);
        // #1 drop mock view
        final String mockViewName = getMockTableName(outputName);
        buf.append("DROP VIEW \"" + mockViewName + "\";\n");
        if(outputMethod != OutputMethod.view) {
            final String unionViewName = getMergeViewName(outputName);
            // #2 drop union view
            buf.append("DROP VIEW \"");
            buf.append(unionViewName);
            buf.append("\";");
            // #3 drop tmp tables
            final int numTasks = masters.length;
            for(int i = 0; i < numTasks; i++) {
                buf.append("\nDROP TABLE \"");
                buf.append(unionViewName);
                buf.append("task");
                buf.append(i);
                buf.append("\";");
            }
        }
        return buf.toString();
    }

    private static String getMockTableName(@Nonnull final String outputName) {
        return MOCK_TABLE_NAME_PREFIX + outputName;
    }

    private static String getMergeViewName(@Nonnull final String outputName) {
        return TMP_TABLE_NAME_PREFIX + outputName;
    }

    private static String getTaskResultTableName(@Nonnull final String retTableName, final int taskNumber) {
        return TMP_TABLE_NAME_PREFIX + retTableName + "task" + taskNumber;
    }

    private static String getReduceQuery(@Nonnull String queryTemplate, @Nonnull String outputName, @Nonnull SQLTranslator translator)
            throws GridException {
        String translated = translator.translateQuery(queryTemplate);
        final String inputViewName = getMergeViewName(outputName);
        return translated.replace("<src>", '"' + inputViewName + '"');
    }

    public GridTaskResultPolicy result(final GridTaskResult result) throws GridException {
        final String taskId = result.getTaskId();
        final ParallelSQLMapTask task = remainingTasks.get(taskId);
        if(task == null) {
            // task already returned by an other node.
            return GridTaskResultPolicy.SKIP;
        }

        final ParallelSQLMapTaskResult taskResult = result.getResult();
        if(taskResult == null) {
            // on task failure                       
            if(LOG.isWarnEnabled()) {
                GridException err = result.getException();
                LOG.warn("task failed: " + result.getTaskId(), err);
            }

            GridNode taskMaster = task.getTaskMasterNode();
            GridNode failedNode = result.getExecutedNode();
            assert (failedNode != null);
            DistributionCatalog catalog = registry.getDistributionCatalog();
            catalog.setNodeState(failedNode, NodeState.suspected);

            if(failedNode.equals(taskMaster)) {
                return GridTaskResultPolicy.FAILOVER;
            } else {
                // fail-over handling is not needed for a speculative task
                return GridTaskResultPolicy.SKIP;
            }
        }

        if(remainingTasks.remove(taskId) == null) {
            LOG.warn("Unexpected condition: other thread concurrently removed the task: " + taskId); // TODO REVIEWME Could this cause?
            return GridTaskResultPolicy.SKIP;
        }

        GridNode taskMaster = taskResult.getMasterNode();
        finishedNodes.add(taskMaster);

        // # 3 invoke COPY INTO table if needed
        final LockManager lockMgr = registry.getLockManager();
        final int numFetchedRows = taskResult.getNumRows();
        if(numFetchedRows > 0) {
            final DBAccessor dba = registry.getDbAccessor();
            copyintoExecs.execute(new Runnable() {
                public void run() {
                    final int inserted;
                    try {
                        inserted = invokeCopyIntoTable(taskResult, outputName, dba, lockMgr);
                    } catch (GridException e) {
                        LOG.error(e);
                        throw new IllegalStateException("Copy Into table failed: " + outputName, e);
                    }
                    if(LOG.isInfoEnabled()) {
                        LOG.info("Merged " + inserted + " records for '" + outputName + "' from "
                                + result.getExecutedNode());
                    }
                }
            });
        } else {
            if(numFetchedRows == 0) {
                if(LOG.isInfoEnabled()) {
                    LOG.info("No result found on a node: " + result.getExecutedNode());
                }
            } else {
                assert (numFetchedRows == -1);
                if(LOG.isDebugEnabled()) {
                    LOG.debug("TaskResultTable is directly created: " + task.getTaskTableName());
                }
            }
        }

        // TODO invoke speculative tasks if needed 
        if(waitForStartSpeculativeTask > 0 && !remainingTasks.isEmpty()) {
            long elapsed = mapStarted - System.currentTimeMillis();
            if(elapsed >= waitForStartSpeculativeTask) {
                int numFinished = finishedNodes.size();
                if(numFinished >= thresholdForSpeculativeTask) {
                    setSpeculativeTasks(result, remainingTasks, finishedNodes);
                    return GridTaskResultPolicy.CONTINUE_WITH_SPECULATIVE_TASKS;
                }
            }
        }

        return GridTaskResultPolicy.CONTINUE;
    }

    private static int invokeCopyIntoTable(@Nonnull final ParallelSQLMapTaskResult result, @Nonnull final String outputName, @Nonnull final DBAccessor dba, @Nonnull final LockManager lockMgr)
            throws GridException {
        final File file = getImportingFile(result, outputName);
        int taskNum = result.getTaskNumber();
        final String tableName = getTaskResultTableName(outputName, taskNum);
        final String sql = constructCopyIntoQuery(file, result, tableName);

        ReadWriteLock systableLock = lockMgr.obtainLock(DBAccessor.SYS_TABLE_SYMBOL);
        final Lock lock = systableLock.writeLock(); // FIXME why exclusive lock? => sometimes result wrong result [Trick] read lock for system tables
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true);
        final int affected;
        try {
            lock.lock();
            affected = JDBCUtils.update(conn, sql);
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException("failed to execute a query: " + sql, e);
        } finally {
            lock.unlock();
            if(!file.delete()) {
                LOG.warn("Could not delete a file: " + file.getAbsolutePath());
            }
            JDBCUtils.closeQuietly(conn);
        }
        int expected = result.getNumRows();
        if(affected != expected) {
            String warnmsg = "COPY INTO TABLE failed [Expected: " + expected + ", Inserted: "
                    + affected + ']';
            LOG.warn(warnmsg);
            throw new GridException(warnmsg);
        }
        return affected;
    }

    private static File getImportingFile(final ParallelSQLMapTaskResult result, final String outputName) {
        String fileName = result.getFileName();
        if(fileName == null) {
            throw new IllegalStateException();
        }
        File colDir = GridUtils.getWorkDir(true);
        File file = new File(colDir, fileName);
        if(!file.exists()) {
            throw new IllegalStateException("File does not exist: " + file.getAbsolutePath());
        }
        return file;
    }

    private static String constructCopyIntoQuery(final File file, final ParallelSQLMapTaskResult result, final String tableName) {
        final int records = result.getNumRows();
        final String filePath = file.getAbsolutePath();
        return "COPY " + records + " RECORDS INTO \"" + tableName + "\" FROM '" + filePath
                + "' USING DELIMITERS '|','\n','\"'";
    }

    private static void setSpeculativeTasks(@Nonnull final GridTaskResult result, @Nonnull final Map<String, ParallelSQLMapTask> remainingTasks, @Nonnull final Set<GridNode> finishedNodes) {
        if(remainingTasks.isEmpty()) {
            return;
        }

        final List<GridTask> tasksToRun = new ArrayList<GridTask>(remainingTasks.size());
        for(final ParallelSQLMapTask task : remainingTasks.values()) {
            boolean hasCandicate = false;
            final GridNode[] candidates = task.getRegisteredSlaves();
            for(final GridNode candidate : candidates) {
                if(finishedNodes.contains(candidate)) {
                    hasCandicate = true;
                    break;
                }
            }
            if(hasCandicate) {
                tasksToRun.add(task);
            }
        }
        if(!tasksToRun.isEmpty()) {
            result.setSpeculativeTasks(tasksToRun);
        }
    }

    public String reduce() throws GridException {
        // wait for all 'COPY INTO table' queries finish
        ExecutorUtils.shutdownAndAwaitTermination(copyintoExecs);

        LockManager lockMgr = registry.getLockManager();
        final ReadWriteLock rwlock = lockMgr.obtainLock(DBAccessor.SYS_TABLE_SYMBOL);

        // #1 Invoke final aggregation query
        final String res;
        final DBAccessor dba = registry.getDbAccessor();
        final Connection conn = GridDbUtils.getPrimaryDbConnection(dba, true); /* autocommit=true*/
        try {
            createMergeView(conn, createMergeViewDDL, rwlock);
            res = invokeReduceQuery(conn, reduceQuery, outputName, outputMethod, rwlock);
        } finally {
            JDBCUtils.closeQuietly(conn);
            // #2 Invoke garbage collection in other thread (remove tmp tables/views)
            invokeGarbageDestroyer(dba, destroyQuery, rwlock);
        }

        return res;
    }

    private static void createMergeView(@Nonnull final Connection conn, @Nonnull final String ddl, @Nonnull final ReadWriteLock rwlock)
            throws GridException {
        if(LOG.isInfoEnabled()) {
            LOG.info("Create a merge view: \n" + ddl);
        }
        final Lock wlock = rwlock.writeLock();
        try {
            wlock.lock();
            JDBCUtils.update(conn, ddl);
        } catch (SQLException e) {
            String errmsg = "failed running a reduce query: " + ddl;
            LOG.error(errmsg, e);
            try {
                conn.rollback();
            } catch (SQLException rbe) {
                LOG.warn("Rollback failed", rbe);
            }
            throw new GridException(errmsg, e);
        } finally {
            wlock.unlock();
        }
    }

    private static String invokeReduceQuery(@Nonnull final Connection conn, @Nonnull final String reduceQuery, @Nonnull final String outputName, @Nonnull final OutputMethod outputMethod, @Nonnull final ReadWriteLock rwlock)
            throws GridException {
        switch(outputMethod) {
            case csvFile:
                return invokeCsvOutputReduce(conn, reduceQuery, outputName, rwlock);
            case scalarString:
                return invokeStringOutputReduce(conn, reduceQuery, rwlock);
            case view:
            case table:
                return invokeReduceDDL(conn, reduceQuery, outputName, outputMethod, rwlock);
            default:
                throw new IllegalStateException("Unexpected outputMethod: " + outputMethod);
        }
    }

    private static String invokeCsvOutputReduce(final Connection conn, final String reduceQuery, final String outputTableName, final ReadWriteLock rwlock)
            throws GridException {
        File colDir = GridUtils.getWorkDir(true);
        final File outFile = new File(colDir, outputTableName + ".csv");
        final CsvWriter writer = new CsvWriter(outFile);
        final ResultSetHandler rsh = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                int numRows = writer.writeAll(rs, MONETDB_NULL_STRING, false);
                if(LOG.isInfoEnabled()) {
                    LOG.info("Result row count: " + numRows);
                }
                return null;
            }
        };

        if(LOG.isInfoEnabled()) {
            LOG.info("Executing a Reduce SQL query: \n" + reduceQuery);
        }
        final Lock rlock = rwlock.readLock();
        try {
            rlock.lock();
            conn.setReadOnly(true);
            JDBCUtils.query(conn, reduceQuery, rsh);
        } catch (SQLException e) {
            String errmsg = "failed running a reduce query: " + reduceQuery;
            LOG.error(errmsg, e);
            try {
                conn.rollback();
            } catch (SQLException rbe) {
                LOG.warn("Rollback failed", rbe);
            }
            throw new GridException(errmsg, e);
        } finally {
            rlock.unlock();
            writer.close();
        }

        if(!outFile.exists()) {
            throw new IllegalStateException("Output file does not exist:"
                    + outFile.getAbsolutePath());
        }
        return outFile.getAbsolutePath();
    }

    private static String invokeStringOutputReduce(final Connection conn, final String reduceQuery, final ReadWriteLock rwlock)
            throws GridException {
        final ResultSetHandler rsh = new ResultSetHandler() {
            public String handle(ResultSet rs) throws SQLException {
                if(rs.next()) {
                    String firstResult = rs.getString(1);
                    return firstResult;
                }
                return null;
            }
        };

        if(LOG.isInfoEnabled()) {
            LOG.info("Executing a Reduce SQL query: \n" + reduceQuery);
        }
        final String result;
        final Lock rlock = rwlock.readLock();
        try {
            rlock.lock();
            conn.setReadOnly(true);
            result = (String) JDBCUtils.query(conn, reduceQuery, rsh);
        } catch (SQLException e) {
            String errmsg = "failed running a reduce query: " + reduceQuery;
            LOG.error(errmsg, e);
            try {
                conn.rollback();
            } catch (SQLException rbe) {
                LOG.warn("Rollback failed", rbe);
            }
            throw new GridException(errmsg, e);
        } finally {
            rlock.unlock();
        }
        return result;
    }

    private static String invokeReduceDDL(final Connection conn, final String reduceQuery, final String outputTableName, final OutputMethod outputMethod, final ReadWriteLock rwlock)
            throws GridException {
        final String query;
        if(outputMethod == OutputMethod.view) {
            query = "CREATE VIEW \"" + outputTableName + "\" AS (\n" + reduceQuery + ')';
        } else if(outputMethod == OutputMethod.table) {
            query = "CREATE TABLE \"" + outputTableName + "\" AS (\n" + reduceQuery + ") WITH DATA";
        } else {
            throw new IllegalStateException("Unexpected OutputMethod: " + outputMethod);
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("Executing a Reduce SQL query: \n" + query);
        }
        final Lock wlock = rwlock.writeLock();
        try {
            wlock.lock();
            JDBCUtils.update(conn, query);
        } catch (SQLException e) {
            String errmsg = "failed running a reduce query: " + query;
            LOG.error(errmsg, e);
            try {
                conn.rollback();
            } catch (SQLException rbe) {
                LOG.warn("Rollback failed", rbe);
            }
            throw new GridException(errmsg, e);
        } finally {
            wlock.unlock();
        }
        return outputTableName;
    }

    private static void invokeGarbageDestroyer(@Nonnull final DBAccessor dba, @Nonnull final String destroyQuery, @Nonnull final ReadWriteLock rwlock) {
        GarbageTableDestroyer destroyer = new GarbageTableDestroyer(destroyQuery, dba, rwlock);
        Thread destroyThread = new Thread(destroyer, "GarbageTableDestroyer");
        destroyThread.setDaemon(true);
        destroyThread.start();
    }

    private static final class GarbageTableDestroyer implements Runnable {

        private final String destroyQuery;
        private final DBAccessor dba;
        private final ReadWriteLock rwlock;

        GarbageTableDestroyer(@CheckForNull String destroyQuery, @Nonnull DBAccessor dba, @Nonnull ReadWriteLock rwlock) {
            if(destroyQuery == null) {
                throw new IllegalArgumentException();
            }
            this.destroyQuery = destroyQuery;
            this.dba = dba;
            this.rwlock = rwlock;
        }

        public void run() {
            Connection conn = null;
            final Lock wlock = rwlock.writeLock();
            try {
                conn = dba.getPrimaryDbConnection();
                conn.setAutoCommit(false);
                wlock.lock();
                JDBCUtils.update(conn, destroyQuery);
                conn.commit();
            } catch (SQLException e) {
                String errmsg = "failed running a destroy query: " + destroyQuery;
                LOG.warn(errmsg, e);
            } finally {
                wlock.unlock();
                JDBCUtils.closeQuietly(conn);
            }
        }
    }

    public static final class JobConf implements Externalizable {

        @Nonnull
        private/* final */String outputName;
        @Nonnull
        private/* final */String mapQuery;
        @Nonnull
        private/* final */String reduceQuery;
        @Nonnull
        private/* final */OutputMethod outputMethod;

        private long waitForStartSpeculativeTask;
        private float invokeSpeculativeTasksFactor = DEFAULT_INVOKE_SPECULATIVE_TASKS_FACTOR;
        private int copyIntoTableConcurrency = DEFAULT_COPYINTO_TABLE_CONCURRENCY;

        public JobConf() {}//Externalizable

        public JobConf(@Nonnull String mapQuery, @Nonnull String reduceQuery) {
            this(null, mapQuery, reduceQuery, OutputMethod.csvFile, -1L);
        }

        public JobConf(@Nullable String outputName, @Nonnull String mapQuery, @Nonnull String reduceQuery, @Nonnull OutputMethod outputMethod, long waitForStartSpeculativeTask) {
            this.outputName = (outputName == null) ? GridUtils.generateQueryName() : outputName;
            this.mapQuery = mapQuery;
            this.reduceQuery = reduceQuery;
            this.outputMethod = outputMethod;
            this.waitForStartSpeculativeTask = waitForStartSpeculativeTask;
        }

        @Nonnull
        public String getOutputName() {
            return outputName;
        }

        public void setOutputName(@Nonnull String outputName) {
            this.outputName = outputName;
        }

        @Nonnull
        public OutputMethod getOutputMethod() {
            return outputMethod;
        }

        public void setOutputMethod(@Nonnull OutputMethod outputMethod) {
            this.outputMethod = outputMethod;
        }

        public long getWaitForStartSpeculativeTask() {
            return waitForStartSpeculativeTask;
        }

        /**
         * @param mills Wait time in milliseconds for speculative execution. Set -1L To disable speculative execution (default: -1L).
         */
        public void setWaitForStartSpeculativeTask(@Nonnegative long mills) {
            this.waitForStartSpeculativeTask = mills;
        }

        public float getInvokeSpeculativeTasksFactor() {
            return invokeSpeculativeTasksFactor;
        }

        /**
         * @param factor 0 < factor < 1
         */
        public void setInvokeSpeculativeTasksFactor(float factor) {
            if(factor <= 0f || factor >= 1f) {
                throw new IllegalArgumentException("Illegal factor: " + factor);
            }
            this.invokeSpeculativeTasksFactor = factor;
        }

        public int getCopyIntoTableConcurrency() {
            return copyIntoTableConcurrency;
        }

        public void setCopyIntoTableConcurrency(int copyIntoTableConcurrency) {
            this.copyIntoTableConcurrency = copyIntoTableConcurrency;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.outputName = IOUtils.readString(in);
            this.mapQuery = IOUtils.readString(in);
            this.reduceQuery = IOUtils.readString(in);
            this.outputMethod = OutputMethod.resolve(in.readByte());
            this.waitForStartSpeculativeTask = in.readLong();
            this.copyIntoTableConcurrency = in.readInt();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(outputName, out);
            IOUtils.writeString(mapQuery, out);
            IOUtils.writeString(reduceQuery, out);
            out.writeByte(outputMethod.id);
            out.writeLong(waitForStartSpeculativeTask);
            out.writeInt(copyIntoTableConcurrency);
        }
    }

    public enum OutputMethod {

        /** Default File */
        csvFile(1),
        /** As table */
        table(2),
        /** As view */
        view(3),
        /** scalar string */
        scalarString(4);

        private final int id;

        private OutputMethod(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static OutputMethod resolve(@Nonnull final int id) {
            switch(id) {
                case 1:
                    return csvFile;
                case 2:
                    return table;
                case 3:
                    return view;
                case 4:
                    return scalarString;
                default:
                    throw new IllegalArgumentException("Illegal OutputMethod ID: " + id);
            }
        }

        public static OutputMethod resolve(@Nonnull final String method) {
            if("csv".equalsIgnoreCase(method) || "csvfile".equalsIgnoreCase(method)) {
                return csvFile;
            } else if("table".equalsIgnoreCase(method)) {
                return table;
            } else if("view".equalsIgnoreCase(method)) {
                return view;
            } else if("scalarString".equalsIgnoreCase(method)) {
                return scalarString;
            } else {
                throw new IllegalArgumentException("Illegal OutputMethod: " + method);
            }
        }
    }

}
