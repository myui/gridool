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

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridJobBase;
import gridool.db.helpers.GridDbUtils;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.routing.GridRouter;
import gridool.util.datetime.StopWatch;
import gridool.util.primitive.MutableLong;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBGraceMultiCSVsLoadJob extends
        GridJobBase<gridool.db.partitioning.DBPartitioningJobConf, HashMap<GridNode, MutableLong>> {
    private static final long serialVersionUID = -771692045224324516L;
    private static final Log LOG = LogFactory.getLog(MonetDBGraceMultiCSVsLoadJob.class);

    private transient HashMap<GridNode, MutableLong> asgginedMap;
    private transient DBPartitioningJobConf jobConf;

    @GridKernelResource
    private transient GridKernel kernel;
    @GridConfigResource
    private transient GridConfiguration config;

    public MonetDBGraceMultiCSVsLoadJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public Map<GridTask, GridNode> map(GridRouter router, DBPartitioningJobConf jobConf)
            throws GridException {
        final GridNode[] allNodes = router.getAllNodes();
        final int numNodes = allNodes.length;
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(numNodes);
        for(GridNode node : allNodes) {
            GridTask task = new MonetDBGraceCsvLoadTask(this, jobConf, false);
            map.put(task, node);
        }
        this.asgginedMap = new HashMap<GridNode, MutableLong>(numNodes);
        this.jobConf = jobConf;
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final GridTaskResultPolicy policy = super.result(result);

        HashMap<GridNode, MutableLong> map = result.getResult();
        for(final Map.Entry<GridNode, MutableLong> e : map.entrySet()) {
            GridNode node = e.getKey();
            MutableLong assigned = e.getValue();
            MutableLong prev = asgginedMap.get(node);
            if(prev == null) {
                asgginedMap.put(node, assigned);
            } else {
                long v = assigned.longValue();
                prev.add(v);
            }
        }
        return policy;
    }

    @Override
    public HashMap<GridNode, MutableLong> reduce() throws GridException {
        GridNode localNode = config.getLocalNode();
        String csvFileName = GridDbUtils.generateCsvFileName(jobConf, localNode);

        StopWatch sw = new StopWatch();
        long numInserted = GridDbUtils.invokeCsvLoadJob(kernel, csvFileName, asgginedMap, jobConf);
        if(LOG.isInfoEnabled()) {
            LOG.info("Inserted " + numInserted + " records into '" + jobConf.getTableName()
                    + "' table in " + sw.toString());
        }
        return asgginedMap;
    }

}
