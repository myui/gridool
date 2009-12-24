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

import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridNode;
import gridool.db.DBInsertOperation;
import gridool.db.record.MultiKeyRowPlaceholderRecord;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.task.DBMapShuffleTaskBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import xbird.util.collections.ArrayQueue;
import xbird.util.concurrent.collections.ConcurrentIdentityHashMap;
import xbird.util.datetime.StopWatch;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.primitive.MutableInt;
import xbird.util.string.StringUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class MonetDBTableAdvPartitioningBulkloadBatchTask extends
        DBMapShuffleTaskBase<MultiKeyRowPlaceholderRecord, MultiKeyRowPlaceholderRecord> {
    private static final long serialVersionUID = 4820678759683359352L;

    private transient int[] pkeyIdxs = null;
    private transient int[] fkeyIdxs = null;

    private transient final AtomicBoolean firstShuffleAttempt = new AtomicBoolean(true);
    private transient volatile String tailCopyIntoQuery;
    private transient final ConcurrentMap<GridNode, MutableInt> assignedRecMap = new ConcurrentIdentityHashMap<GridNode, MutableInt>(64);

    @SuppressWarnings("unchecked")
    public MonetDBTableAdvPartitioningBulkloadBatchTask(GridJob job, DBMapReduceJobConf jobConf) {
        super(job, jobConf);
        this.shuffleUnits = 10000;
    }

    @Override
    protected void preprocess(final Connection conn, final ResultSet results) throws SQLException {
        final String inputTable = jobConf.getInputTable();
        Pair<int[], int[]> keys = JDBCUtils.getPartitioningKeys(conn, inputTable);
        this.pkeyIdxs = keys.getFirst();
        this.fkeyIdxs = keys.getSecond();
        assert (pkeyIdxs != null || fkeyIdxs != null);
    }

    @Override
    protected void readFields(MultiKeyRowPlaceholderRecord record, ResultSet results)
            throws SQLException {
        record.configureRecord(pkeyIdxs, fkeyIdxs);
        record.readFields(results);
    }

    @Override
    protected boolean process(MultiKeyRowPlaceholderRecord record) {
        shuffle(record);
        return true;
    }

    @Override
    protected void invokeShuffle(final ExecutorService shuffleExecPool, final ArrayQueue<MultiKeyRowPlaceholderRecord> queue) {
        assert (kernel != null);
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String driverClassName = jobConf.getDriverClassName();
                String connectUrl = jobConf.getConnectUrl();
                String mapOutputTableName = jobConf.getMapOutputTableName();
                MultiKeyRowPlaceholderRecord[] records = queue.toArray(MultiKeyRowPlaceholderRecord.class);
                assert (records.length > 0);
                final String createTableDDL;
                if(firstShuffleAttempt.compareAndSet(true, false)) {
                    MultiKeyRowPlaceholderRecord r = records[0];
                    tailCopyIntoQuery = "COPY INTO \"" + mapOutputTableName
                            + "\" FROM '<src>' USING DELIMITERS '"
                            + StringUtils.escape(r.getFieldSeparator()) + "', '"
                            + StringUtils.escape(r.getRecordSeparator()) + "', '"
                            + StringUtils.escape(r.getStringQuote()) + "' NULL AS '"
                            + StringUtils.escape(r.getNullString()) + '\'';
                    createTableDDL = jobConf.getCreateMapOutputTableDDL();
                } else {
                    createTableDDL = null;
                }

                DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, createTableDDL, mapOutputTableName, null, records);
                ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
                final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(MonetDBPrepareCopyIntoJob.class, ops);
                final Map<GridNode, MutableInt> map;
                try {
                    map = future.get(); // wait for execution
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                    throw new IllegalStateException(ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                    throw new IllegalStateException(ee);
                }
                final ConcurrentMap<GridNode, MutableInt> recMap = assignedRecMap;
                for(Map.Entry<GridNode, MutableInt> e : map.entrySet()) {
                    GridNode node = e.getKey();
                    MutableInt assigned = e.getValue();
                    final MutableInt prev = recMap.putIfAbsent(node, assigned);
                    if(prev != null) {
                        prev.add(assigned.intValue());
                    }
                }
            }
        });
    }

    @Override
    protected void postShuffle() {
        super.postShuffle();

        String driverClassName = jobConf.getDriverClassName();
        String connectUrl = jobConf.getConnectUrl();
        String tableName = jobConf.getMapOutputTableName();
        final MonetDBInvokeCopyIntoOperation ops = new MonetDBInvokeCopyIntoOperation(driverClassName, connectUrl, tableName, tailCopyIntoQuery);
        ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
        final Pair<MonetDBInvokeCopyIntoOperation, Map<GridNode, MutableInt>> pair = new Pair<MonetDBInvokeCopyIntoOperation, Map<GridNode, MutableInt>>(ops, assignedRecMap);

        final GridJobFuture<Long> future = kernel.execute(MonetDBInvokeCopyIntoJob.class, pair);
        final Long elapsed;
        try {
            elapsed = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie.getMessage(), ie);
            throw new IllegalStateException(ie);
        } catch (ExecutionException ee) {
            LOG.error(ee.getMessage(), ee);
            throw new IllegalStateException(ee);
        }
        assert (elapsed != null);
        LOG.info("Elapsed time for bulkload: " + StopWatch.elapsedTime(elapsed.longValue()));
    }

}
