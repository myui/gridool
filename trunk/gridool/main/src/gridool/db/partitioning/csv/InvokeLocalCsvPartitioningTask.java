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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.util.GridUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nonnull;

import xbird.util.lang.ArrayUtils;
import xbird.util.primitive.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class InvokeLocalCsvPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -6863271080717798071L;

    private final String[] lines;
    private final String fileName;
    private final boolean isFirst;
    private final Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
    private final DBPartitioningJobConf jobConf;

    @GridKernelResource
    private transient GridKernel kernel;

    @SuppressWarnings("unchecked")
    public InvokeLocalCsvPartitioningTask(final GridJob job, @Nonnull final List<String> lineList, @Nonnull final PartitioningJobConf ops) {
        super(job, false);
        this.lines = ArrayUtils.toArray(lineList);
        this.fileName = ops.getFileName();
        this.isFirst = ops.isFirst();
        this.primaryForeignKeys = ops.getPrimaryForeignKeys();
        this.jobConf = ops.getJobConf();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected HashMap<GridNode, MutableInt> execute() throws GridException {
        PartitioningJobConf args = new PartitioningJobConf(lines, fileName, isFirst, primaryForeignKeys, jobConf);
        GridJobFuture<HashMap<GridNode, MutableInt>> future = kernel.execute(LocalCsvHashPartitioningJob.class, args);
        HashMap<GridNode, MutableInt> result = GridUtils.invokeGet(future);
        return result;
    }

}
