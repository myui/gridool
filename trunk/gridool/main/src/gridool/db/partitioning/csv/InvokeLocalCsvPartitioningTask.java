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
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nonnull;

import xbird.util.io.IOUtils;
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

    private transient/* final */String[] lines;
    private transient/* final */String fileName;
    private transient/* final */boolean isFirst;
    private transient/* final */Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
    private transient/* final */DBPartitioningJobConf jobConf;
    private transient/* final */GridTaskRouter router;

    @GridKernelResource
    private transient GridKernel kernel;

    @SuppressWarnings("unchecked")
    public InvokeLocalCsvPartitioningTask(final GridJob job, @Nonnull final List<String> lineList, @Nonnull final PartitioningJobConf ops, @Nonnull GridTaskRouter router) {
        super(job, false);
        this.lines = ArrayUtils.toArray(lineList);
        this.fileName = ops.getFileName();
        this.isFirst = ops.isFirst();
        this.primaryForeignKeys = ops.getPrimaryForeignKeys();
        this.jobConf = ops.getJobConf();
        this.router = router;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected HashMap<GridNode, MutableInt> execute() throws GridException {
        PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, isFirst, primaryForeignKeys, jobConf);
        Pair<PartitioningJobConf, GridTaskRouter> args = new Pair<PartitioningJobConf, GridTaskRouter>(conf, router);
        GridJobFuture<HashMap<GridNode, MutableInt>> future = kernel.execute(LocalCsvHashPartitioningJob.class, args);
        HashMap<GridNode, MutableInt> result = GridUtils.invokeGet(future);
        return result;
    }

    private void readObject(ObjectInputStream in) throws java.io.IOException,
            ClassNotFoundException {
        in.defaultReadObject();

        int numLines = in.readInt();
        final String[] lines = new String[numLines];
        for(int i = 0; i < numLines; i++) {
            lines[i] = IOUtils.readString(in);
        }
        this.lines = lines;
        this.fileName = IOUtils.readString(in);
        this.isFirst = in.readBoolean();
        PrimaryKey pkey = (PrimaryKey) in.readObject();
        final int numFkeys = in.readInt();
        final List<ForeignKey> fkeys = new ArrayList<ForeignKey>(numFkeys);
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = (ForeignKey) in.readObject();
            fkeys.add(fk);
        }
        this.primaryForeignKeys = new Pair<PrimaryKey, Collection<ForeignKey>>(pkey, fkeys);
        this.jobConf = (DBPartitioningJobConf) in.readObject();
        this.router = (GridTaskRouter) in.readObject();
    }

    private void writeObject(ObjectOutputStream out) throws java.io.IOException {
        out.defaultWriteObject();

        out.writeInt(lines.length);
        for(final String line : lines) {
            IOUtils.writeString(line, out);
        }
        IOUtils.writeString(fileName, out);
        out.writeBoolean(isFirst);
        PrimaryKey pkey = primaryForeignKeys.getFirst();
        out.writeObject(pkey);
        Collection<ForeignKey> fkeys = primaryForeignKeys.getSecond();
        int numFkeys = fkeys.size();
        out.writeInt(numFkeys);
        for(final ForeignKey fk : fkeys) {
            out.writeObject(fk);
        }
        out.writeObject(jobConf);
        out.writeObject(router);
    }
}
