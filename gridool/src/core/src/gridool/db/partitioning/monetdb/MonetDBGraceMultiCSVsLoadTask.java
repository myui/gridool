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

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.util.primitive.MutableLong;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBGraceMultiCSVsLoadTask extends GridTaskAdapter {
    private static final long serialVersionUID = 1866000226009548190L;
    private static final Log LOG = LogFactory.getLog(MonetDBGraceMultiCSVsLoadTask.class);

    @Nonnull
    private final DBPartitioningJobConf jobConf;

    @GridKernelResource
    protected transient GridKernel kernel;

    @SuppressWarnings("unchecked")
    public MonetDBGraceMultiCSVsLoadTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected HashMap<GridNode, MutableLong> execute() throws GridException {
        GridJobFuture<HashMap<GridNode, MutableLong>> future = kernel.execute(MonetDBGraceMultiCSVsLoadJob.class, jobConf);
        HashMap<GridNode, MutableLong> assigned;
        try {
            assigned = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie.getMessage(), ie);
            throw new IllegalStateException(ie);
        } catch (ExecutionException ee) {
            LOG.error(ee.getMessage(), ee);
            throw new IllegalStateException(ee);
        }
        return assigned;
    }

}
