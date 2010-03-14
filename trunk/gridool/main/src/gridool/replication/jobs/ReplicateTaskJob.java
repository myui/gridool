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
package gridool.replication.jobs;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.routing.GridTaskRouter;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.lang.ClassUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ReplicateTaskJob extends GridJobBase<ReplicateTaskJob.JobConf, Boolean> {
    private static final long serialVersionUID = -7198833169724654323L;
    private static final Log LOG = LogFactory.getLog(ReplicateTaskJob.class);

    private transient boolean succeed = false;
    private transient GridTask replicatedTask;
    private transient List<GridNode> replicaList;
    @Nonnull
    private transient final List<GridNode> replicated;

    public ReplicateTaskJob() {
        super();
        this.replicated = new ArrayList<GridNode>(4);
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf) throws GridException {
        final GridTask taskToReplicate = jobConf.getTask();
        final List<GridNode> destNodes = jobConf.getDestNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(destNodes.size());
        for(GridNode node : destNodes) {
            GridTask task = new ReplicatedGridTaskAdapter(this, taskToReplicate);
            map.put(task, node);
        }
        this.replicatedTask = taskToReplicate;
        this.replicaList = destNodes;

        if(LOG.isInfoEnabled()) {
            LOG.info("Start a replication [" + getJobId() + "] of a task "
                    + ClassUtils.getSimpleClassName(replicatedTask) + '['
                    + replicatedTask.getTaskId() + "] to slave nodes: " + replicaList);
        }
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        assert (replicatedTask != null);
        final GridNode executedNode = result.getExecutedNode();
        assert (executedNode != null);
        final Exception err = result.getException();
        if(err == null) {
            this.succeed = true;
            replicated.add(executedNode);
            //return GridTaskResultPolicy.RETURN; // REVIEWME if one of replication tasks succeed, then immediately return
        } else {
            LOG.warn("One of Replication of task '" + result.getTaskId() + "' failed", err);
            replicaList.remove(executedNode); // REVIEWME
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    /**
     * Is at least one replication succeeded?
     */
    public Boolean reduce() throws GridException {
        replicatedTask.setReplicatedNodes(replicated);
        return succeed;
    }

    public static final class JobConf {

        private final GridTask task;
        private final List<GridNode> destNodes;

        public JobConf(@Nonnull GridTask task, @Nonnull List<GridNode> destNodes) {
            this.task = task;
            this.destNodes = destNodes;
        }

        public GridTask getTask() {
            return task;
        }

        public List<GridNode> getDestNodes() {
            return destNodes;
        }

    }

}
