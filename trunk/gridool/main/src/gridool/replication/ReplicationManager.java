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
package gridool.replication;

import gridool.GridConfiguration;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridTask;
import gridool.communication.payload.GridNodeInfo;
import gridool.processors.task.GridTaskProcessor;
import gridool.replication.jobs.ReplicateTaskJob;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
public final class ReplicationManager {
    private static final Log LOG = LogFactory.getLog(ReplicationManager.class);

    private final GridKernel kernel;
    private final ReplicaSelector replicaSelector;
    private final ReplicaCoordinator replicaCoordinator;

    public ReplicationManager(@Nonnull GridKernel kernel, @Nonnull GridConfiguration config) {
        this.kernel = kernel;
        this.replicaSelector = ReplicationModuleBuilder.createReplicaSelector();
        this.replicaCoordinator = ReplicationModuleBuilder.createReplicaCoordinator(config);
    }

    @Nonnull
    public ReplicaSelector getReplicaSelector() {
        return replicaSelector;
    }

    @Nonnull
    public ReplicaCoordinator getReplicaCoordinator() {
        return replicaCoordinator;
    }

    /**
     * @see GridTaskProcessor#processTask(GridTask)
     */
    public boolean replicateTask(@Nonnull GridTask task, @Nonnull GridNodeInfo masterNode) {
        if(!task.isReplicatable()) {// sanity check
            throw new IllegalStateException();
        }
        final List<GridNode> replicas = masterNode.getReplicas();
        if(replicas.isEmpty()) {
            return true; // there is no replica
        }

        final GridJobFuture<Boolean> future = kernel.execute(ReplicateTaskJob.class, new ReplicateTaskJob.JobConf(task, replicas));
        final Boolean succeed;
        try {
            succeed = future.get();
        } catch (InterruptedException e) {
            LOG.warn(e);
            return false;
        } catch (ExecutionException e) {
            LOG.error(e);
            return false;
        }
        if(succeed == null || !succeed.booleanValue()) {
            LOG.error("Replication of a task failed: " + task.getTaskId());
            return false;
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("Replicated a task '" + task.getTaskId() + "' to slave nodes: " + replicas);
        }
        return true;
    }

}
