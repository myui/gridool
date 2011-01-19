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
package gridool.db.partitioning.csv.dist;

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
import gridool.db.partitioning.PartitioningJobType;
import gridool.db.partitioning.csv.PartitioningJobConf;
import gridool.routing.GridRouter;
import gridool.util.GridUtils;
import gridool.util.io.IOUtils;
import gridool.util.lang.ArrayUtils;
import gridool.util.primitive.MutableInt;
import gridool.util.struct.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class InvokeLocalCsvPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -6863271080717798071L;

    private transient/* final */String[] lines;
    private transient/* final */String fileName;
    private transient/* final */boolean isFirst;
    private transient/* final */Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
    private transient/* final */DBPartitioningJobConf jobConf;
    private transient/* final */int bucketNumber;
    private transient/* final */GridRouter router;

    @GridKernelResource
    private transient GridKernel kernel;

    @SuppressWarnings("unchecked")
    public InvokeLocalCsvPartitioningTask(final GridJob job, @Nonnull final List<String> lineList, @Nonnull final PartitioningJobConf ops, @Nonnull GridRouter router) {
        super(job, false);
        this.lines = ArrayUtils.toArray(lineList);
        this.fileName = ops.getFileName();
        this.isFirst = ops.isFirst();
        this.primaryForeignKeys = ops.getPrimaryForeignKeys();
        this.jobConf = ops.getJobConf();
        this.bucketNumber = ops.getBucketNumber();
        this.router = router;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    protected HashMap<GridNode, MutableInt> execute() throws GridException {
        PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, isFirst, primaryForeignKeys, jobConf, bucketNumber);
        Pair<PartitioningJobConf, GridRouter> args = new Pair<PartitioningJobConf, GridRouter>(conf, router);
        PartitioningJobType jobType = jobConf.getJobType();
        Class<? extends GridJob<Pair<PartitioningJobConf, GridRouter>, HashMap<GridNode, MutableInt>>> jobClass = jobType.getLocalPartitioningJobClass();
        //GridJobFuture<HashMap<GridNode, MutableInt>> future = kernel.execute(LocalCsvHashPartitioningJob.class, args);
        //GridJobFuture<HashMap<GridNode, MutableInt>> future = kernel.execute(InMemoryLocalCsvHashPartitioningJob.class, args);
        GridJobFuture<HashMap<GridNode, MutableInt>> future = kernel.execute(jobClass, args);
        HashMap<GridNode, MutableInt> result = GridUtils.invokeGet(future);
        this.lines = null; // help GC
        return result;
    }

    private void readObject(ObjectInputStream in) throws java.io.IOException,
            ClassNotFoundException {
        in.defaultReadObject();

        final DataInput objIn = in;
        int numLines = in.readInt();
        final String[] lines = new String[numLines];
        for(int i = 0; i < numLines; i++) {
            lines[i] = IOUtils.readString(objIn);
        }
        this.lines = lines;
        this.fileName = IOUtils.readString(objIn);
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
        this.bucketNumber = in.readInt();
        this.router = (GridRouter) in.readObject();
    }

    private void writeObject(ObjectOutputStream out) throws java.io.IOException {
        out.defaultWriteObject();

        final DataOutput objOut = out;
        out.writeInt(lines.length);
        for(final String line : lines) {
            IOUtils.writeString(line, objOut);
        }
        IOUtils.writeString(fileName, objOut);
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
        out.writeInt(bucketNumber);
        out.writeObject(router);
    }
}
