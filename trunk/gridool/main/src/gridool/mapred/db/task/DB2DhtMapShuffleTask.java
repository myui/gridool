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
import gridool.directory.job.DirectoryAddRecordJob;
import gridool.directory.job.DirectoryAddRecordJob.AddRecordOps;
import gridool.lib.db.DBRecord;
import gridool.lib.db.GenericDBRecord;
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
public class DB2DhtMapShuffleTask extends DBMapShuffleTaskBase<DBRecord> {
    private static final long serialVersionUID = -697335089991627099L;

    @SuppressWarnings("unchecked")
    public DB2DhtMapShuffleTask(GridJob job, DBMapReduceJobConf jobConf) {
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
                String mapOutputTableName = jobConf.getMapOutputTableName();
                AddRecordOps ops = new AddRecordOps(mapOutputTableName, queue, jobConf.getMapOutputMarshaller());
                final GridJobFuture<Serializable> future = kernel.execute(DirectoryAddRecordJob.class, ops);
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
