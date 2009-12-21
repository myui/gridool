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
package gridool.lib.db;

import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.task.DBMapShuffleTaskBase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import xbird.util.collections.ArrayQueue;
import xbird.util.primitive.AtomicFloat;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBTableAdvPartitioningTask extends
        DBMapShuffleTaskBase<MultiKeyGenericDBRecord, MultiKeyGenericDBRecord> {
    private static final long serialVersionUID = -8308742694304042395L;

    private transient int[] pkeyIdxs = null;
    private transient int[] fkeyIdxs = null;

    private transient boolean firstShuffleAttempt = true;
    private transient final AtomicFloat sumOverlapPerc = new AtomicFloat(0.0f);
    private transient final AtomicInteger cntShuffle = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public DBTableAdvPartitioningTask(GridJob job, DBMapReduceJobConf jobConf) {
        super(job, jobConf);
    }

    @Override
    protected void preprocess(final Connection conn, final ResultSet results) throws SQLException {
        final List<String> keys = new ArrayList<String>();
        final String inputTable = jobConf.getInputTable();
        final DatabaseMetaData meta = conn.getMetaData();
        final String catalog = conn.getCatalog();

        // primary key
        final ResultSet rs1 = meta.getPrimaryKeys(catalog, null, inputTable);
        try {
            while(rs1.next()) {
                String pk = rs1.getString("COLUMN_NAME");
                keys.add(pk);
            }
        } finally {
            rs1.close();
        }
        final int pkeyColumns = keys.size();
        if(pkeyColumns != 0) {
            final int[] idxs = new int[pkeyColumns];
            for(int i = 0; i < pkeyColumns; i++) {
                String label = keys.get(i);
                idxs[i] = results.findColumn(label);
            }
            this.pkeyIdxs = idxs;
        }
        keys.clear();

        // foreign key
        final ResultSet rs2 = meta.getImportedKeys(catalog, null, inputTable);
        try {
            while(rs2.next()) {
                String fk = rs2.getString("FKCOLUMN_NAME");
                keys.add(fk);
            }
        } finally {
            rs2.close();
        }
        final int fkeyColumns = keys.size();
        if(fkeyColumns != 0) {
            final int[] idxs = new int[fkeyColumns];
            for(int i = 0; i < fkeyColumns; i++) {
                String label = keys.get(i);
                idxs[i] = results.findColumn(label);
            }
            this.fkeyIdxs = idxs;
        }

        assert (pkeyIdxs != null || fkeyIdxs != null);
    }

    @Override
    protected void readFields(MultiKeyGenericDBRecord record, ResultSet results)
            throws SQLException {
        record.configureRecord(pkeyIdxs, fkeyIdxs);
        record.readFields(results);
    }

    @Override
    protected boolean process(MultiKeyGenericDBRecord record) {
        shuffle(record);
        return true;
    }

    @Override
    protected void invokeShuffle(final ExecutorService shuffleExecPool, final ArrayQueue<MultiKeyGenericDBRecord> queue) {
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
                String[] fieldNames = jobConf.getMapOutputFieldNames();
                MultiKeyGenericDBRecord[] records = queue.toArray(MultiKeyGenericDBRecord.class);
                DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, createTableDDL, mapOutputTableName, fieldNames, records);
                ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
                final GridJobFuture<Float> future = kernel.execute(DBInsertMultiKeyRecordJob.class, ops);
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
