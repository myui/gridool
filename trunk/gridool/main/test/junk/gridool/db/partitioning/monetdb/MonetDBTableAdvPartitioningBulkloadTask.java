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
import gridool.db.DBInsertOperation;
import gridool.db.record.MultiKeyRowPlaceholderRecord;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.task.DBMapShuffleTaskBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import xbird.util.collections.ArrayQueue;
import xbird.util.jdbc.JDBCUtils;
import xbird.util.primitive.AtomicFloat;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBTableAdvPartitioningBulkloadTask extends
        DBMapShuffleTaskBase<MultiKeyRowPlaceholderRecord, MultiKeyRowPlaceholderRecord> {
    private static final long serialVersionUID = 4820678759683359352L;

    private transient int[] pkeyIdxs = null;
    private transient int[] fkeyIdxs = null;

    private transient volatile boolean firstShuffleAttempt = true;
    private transient final AtomicFloat sumOverlapPerc = new AtomicFloat(0.0f);
    private transient final AtomicInteger cntShuffle = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public MonetDBTableAdvPartitioningBulkloadTask(GridJob job, DBMapReduceJobConf jobConf) {
        super(job, jobConf);
        setShuffleUnits(10000);
    }

    @Override
    protected void preprocess(final Connection conn, final ResultSet results) throws SQLException {
        final String inputTable = jobConf.getInputTable();
        Pair<int[], int[]> keys = JDBCUtils.getPartitioningKeys(conn, inputTable);
        this.pkeyIdxs = keys.getFirst();
        this.fkeyIdxs = keys.getSecond();
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
                final String createTableDDL;
                if(firstShuffleAttempt) {
                    createTableDDL = jobConf.getCreateMapOutputTableDDL();
                    firstShuffleAttempt = false;
                } else {
                    createTableDDL = null;
                }
                String mapOutputTableName = jobConf.getMapOutputTableName();
                MultiKeyRowPlaceholderRecord[] records = queue.toArray(MultiKeyRowPlaceholderRecord.class);
                DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, createTableDDL, mapOutputTableName, null, records);
                ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
                final GridJobFuture<Float> future = kernel.execute(MonetDBCopyIntoJob.class, ops);
                Float overlapPerc = null;
                try {
                    overlapPerc = future.get(); // wait for execution
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                }
                if(overlapPerc != null) {
                    sumOverlapPerc.addAndGet(overlapPerc.floatValue());
                    cntShuffle.incrementAndGet();
                }
            }
        });
    }

    @Override
    protected void postShuffle() {
        super.postShuffle();
        if(LOG.isInfoEnabled()) {
            float perc = (sumOverlapPerc.get() / cntShuffle.get()) * 100.0f;
            LOG.info("Percentage of overlapping records in the shuffled records: " + perc + "%");
        }
    }

}
