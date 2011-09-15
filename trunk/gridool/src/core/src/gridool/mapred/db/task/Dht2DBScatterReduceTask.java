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
import gridool.GridKernel;
import gridool.annotation.GridKernelResource;
import gridool.db.DBInsertOperation;
import gridool.db.DBInsertRecordJob;
import gridool.db.record.DBRecord;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.util.collections.ArrayQueue;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class Dht2DBScatterReduceTask<IN_TYPE> extends
        Dht2DBReduceTaskBase<IN_TYPE, DBRecord> {
    private static final long serialVersionUID = 868218057173104016L;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------

    @SuppressWarnings("unchecked")
    public Dht2DBScatterReduceTask(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, DBMapReduceJobConf jobConf) {
        super(job, inputDhtName, destDhtName, removeInputDhtOnFinish, jobConf);
    }

    @Override
    protected void invokeShuffle(final ExecutorService shuffleExecPool, final ArrayQueue<DBRecord> queue) {
        assert (kernel != null);
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String driverClassName = jobConf.getDriverClassName();
                String connectUrl = jobConf.getConnectUrl();
                String mapOutputTableName = jobConf.getMapOutputTableName();
                String[] fieldNames = jobConf.getMapOutputFieldNames();
                DBRecord[] records = queue.toArray(DBRecord.class);
                DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, mapOutputTableName, fieldNames, records);
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
