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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ReplicateTaskJob extends GridJobBase<ReplicateTaskJob.JobConf, Boolean> {
    private static final long serialVersionUID = -7198833169724654323L;
    private static final Log LOG = LogFactory.getLog(ReplicateTaskJob.class);

    private transient boolean succeed = false;

    public ReplicateTaskJob() {
        super();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf) throws GridException {
        final GridTask taskToReplicate = jobConf.getTask();
        final List<GridNode> destNodes = jobConf.getDestNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(destNodes.size());
        for(GridNode node : destNodes) {
            map.put(new ReplicatedGridTaskAdapter(this, taskToReplicate), node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        final Exception err = result.getException();
        if(err == null) {
            this.succeed = true;
        } else {
            LOG.warn("One of Replication of task '" + task + "'failed", err);
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    /**
     * Is at least one replication succeeded?
     */
    public Boolean reduce() throws GridException {
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
