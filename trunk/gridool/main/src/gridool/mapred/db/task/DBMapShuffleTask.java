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
package gridool.mapred.db.task;

import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.db.DBInsertOperation;
import gridool.db.DBInsertRecordJob;
import gridool.db.record.DBRecord;
import gridool.db.record.GenericDBRecord;
import gridool.mapred.db.DBMapReduceJobConf;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import xbird.util.collections.ArrayQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class DBMapShuffleTask extends DBMapShuffleTaskBase<DBRecord, DBRecord> {
    private static final long serialVersionUID = -269939175231317044L;

    private transient boolean firstShuffleAttempt = true;
    
    @SuppressWarnings("unchecked")
    public DBMapShuffleTask(GridJob job, DBMapReduceJobConf jobConf) {
        super(job, jobConf);
    }
    
    @Override
    protected boolean process(final DBRecord record) {
        shuffle(record);
        return true;
    }

    protected final void shuffle(@Nonnull final byte[] key, @Nonnull final Object... columns) {
        shuffle(new GenericDBRecord(key, columns));
    }

    protected final void shuffle(@Nonnull final byte[] key, @Nonnull final Object[] columns, @Nonnull final int[] sqlTypes) {
        shuffle(new GenericDBRecord(key, columns, sqlTypes));
    }
    
    @Override
    protected void invokeShuffle(final ExecutorService shuffleExecPool, final ArrayQueue<DBRecord> queue) {
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
                DBRecord[] records = queue.toArray(DBRecord.class);
                DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, createTableDDL, mapOutputTableName, fieldNames, records);
                ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
                final GridJobFuture<Serializable> future = kernel.execute(DBInsertRecordJob.class, ops);
                try {
                    future.get(); // wait for execution
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                }
            }
        });
    }

}
