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
package gridool.mapred.db;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridTask;
import gridool.annotation.GridKernelResource;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.mapred.dht.DhtReduceJob;
import gridool.mapred.dht.task.DhtMapShuffleTask;

import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBMapReduceJob extends DBMapJob {
    private static final long serialVersionUID = -8756855076582206285L;

    @GridKernelResource
    private transient GridKernel kernel;

    public DBMapReduceJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public String reduce() throws GridException {
        DhtMapReduceJobConf reduceJobConf = new ReduceOnDhtJobConf(mapOutputTableName, jobConf);
        final GridJobFuture<String> result = kernel.execute(DhtReduceJob.class, reduceJobConf);
        try {
            return result.get();
        } catch (InterruptedException ie) {
            LogFactory.getLog(getClass()).error(ie.getMessage(), ie);
            throw new GridException(ie);
        } catch (ExecutionException ee) {
            LogFactory.getLog(getClass()).error(ee.getMessage(), ee);
            throw new GridException(ee);
        }
    }

    private static final class ReduceOnDhtJobConf extends DhtMapReduceJobConf {
        private static final long serialVersionUID = 1129731669391900133L;

        private final DBMapReduceJobConf jobConf;

        public ReduceOnDhtJobConf(String inputTableName, DBMapReduceJobConf jobConf) {
            super(inputTableName);
            this.jobConf = jobConf;
        }

        @Override
        public String getOutputTableName() {
            return jobConf.getReduceOutputTableName();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected DhtMapShuffleTask makeMapShuffleTask(GridJob job, String inputTableName, String destTableName) {
            throw new IllegalStateException();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected GridTask makeReduceTask(GridJob job, String inputTableName, String destTableName) {
            return jobConf.makeReduceTask(job, inputTableName, destTableName);
        }

    }

}
