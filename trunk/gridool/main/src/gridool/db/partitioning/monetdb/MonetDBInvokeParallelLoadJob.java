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
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridJobBase;
import gridool.db.DBTaskAdapter;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.catalog.RegisterPatitionInCatalogJob;
import gridool.routing.GridTaskRouter;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import xbird.util.primitive.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBInvokeParallelLoadJob extends
        GridJobBase<Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>>, Long> {
    private static final long serialVersionUID = 3356783271521697124L;

    private transient long numProcessed = 0L;
    private transient List<Pair<GridNode, List<GridNode>>> masterSlaves;

    @GridKernelResource
    private transient GridKernel kernel;

    public MonetDBInvokeParallelLoadJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>> args)
            throws GridException {
        final MonetDBParallelLoadOperation ops = args.getFirst();

        final Map<GridNode, MutableInt> assigned = args.getSecond();
        final GridNode[] allNodes = router.getAllNodes();
        for(GridNode node : allNodes) {// create empty table
            if(!assigned.containsKey(node)) {
                assigned.put(node, new MutableInt(0));
            }
        }
        final int mapsize = assigned.size();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(mapsize);
        for(final Map.Entry<GridNode, MutableInt> e : assigned.entrySet()) {
            GridNode node = e.getKey();
            int numRecords = e.getValue().intValue();
            String copyIntoQuery = (numRecords == 0) ? null : ops.getCopyIntoQuery(numRecords);
            MonetDBParallelLoadOperation shrinkedOps = new MonetDBParallelLoadOperation(ops, copyIntoQuery, numRecords);
            GridTask task = new DBTaskAdapter(this, shrinkedOps);
            map.put(task, node);
        }

        this.masterSlaves = new ArrayList<Pair<GridNode, List<GridNode>>>(mapsize);
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Integer processed = result.getResult();
        if(processed != null) {
            numProcessed += processed.longValue();
            GridNode masterNode = result.getExecutedNode();
            List<GridNode> replicatedNodes = result.getReplicatedNodes();
            masterSlaves.add(new Pair<GridNode, List<GridNode>>(masterNode, replicatedNodes));
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        // register mapping
        RegisterPatitionInCatalogJob.JobConf jobConf = new RegisterPatitionInCatalogJob.JobConf(DistributionCatalog.defaultDistributionKey, masterSlaves); // FIXME to use tableName
        final GridJobFuture<Boolean> future = kernel.execute(RegisterPatitionInCatalogJob.class, jobConf);
        try {
            future.get();
        } catch (InterruptedException ie) {
            throw new GridException(ie);
        } catch (ExecutionException ee) {
            throw new GridException(ee);
        }
        return numProcessed;
    }

}
