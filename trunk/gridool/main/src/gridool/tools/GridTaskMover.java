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
package gridool.tools;

import gridool.Grid;
import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.communication.GridCommunicationManager;
import gridool.construct.GridJobBase;
import gridool.loadblancing.workstealing.GridTaskStealingTask;
import gridool.routing.GridNodeSelector;
import gridool.routing.GridTaskRouter;
import gridool.taskqueue.sender.SenderResponseTaskQueue;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridTaskMover {
    private static final Log LOG = LogFactory.getLog(GridTaskMover.class);

    @Nonnull
    private final Grid grid;

    public GridTaskMover(@CheckForNull Grid grid) {
        if(grid == null) {
            throw new IllegalArgumentException();
        }
        this.grid = grid;
    }

    /**
     * Attempt work stealing.
     */
    public boolean moveTask(@Nonnegative int tasks, @Nonnull GridNode fromNode, @Nonnull List<GridNode> destNodes, boolean async) {
        assert (tasks >= 0);
        if(tasks == 0) {
            return true;
        }
        if(destNodes.isEmpty()) {
            return false;
        }
        final TaskMoveOperation ops = new TaskMoveOperation(tasks, fromNode, destNodes, async);
        try {
            return grid.execute(GridTaskStealJob.class, ops);
        } catch (RemoteException e) {
            LOG.error("failed to move tasks: " + e.getMessage(), e);
            return false;
        }
    }

    public boolean moveTask(@Nonnull List<GridTask> tasks, @Nonnull List<GridNode> destNodes, boolean async) {
        if(tasks.isEmpty()) {
            return true;
        }
        if(destNodes.isEmpty()) {
            return false;
        }
        final TaskMoveOperation ops = new TaskMoveOperation(tasks, destNodes, async);
        try {
            return grid.execute(GridLocalTaskMoveJob.class, ops);
        } catch (RemoteException e) {
            LOG.error("failed to move tasks: " + e.getMessage(), e);
            return false;
        }
    }

    private static final class TaskMoveOperation implements Serializable {
        private static final long serialVersionUID = -9102771449534398177L;

        @Nullable
        private final List<GridTask> tasks;
        private final int numberOfMoveTasks;
        @Nonnull
        private final GridNode destNode;
        @Nullable
        private final List<GridNode> toNodes;
        private final boolean isAsyncOps;

        TaskMoveOperation(int tasks, GridNode fromNode, List<GridNode> toNodes, boolean isAsyncOps) {
            this.tasks = null;
            this.numberOfMoveTasks = tasks;
            this.destNode = fromNode;
            this.toNodes = toNodes;
            this.isAsyncOps = isAsyncOps;
        }

        TaskMoveOperation(List<GridTask> tasks, List<GridNode> toNodes, boolean isAsyncOps) {
            this.tasks = tasks;
            this.numberOfMoveTasks = tasks.size();
            this.destNode = null;
            this.toNodes = toNodes;
            this.isAsyncOps = isAsyncOps;
        }

        boolean isAsyncOps() {
            return isAsyncOps;
        }

        List<GridTask> getTasks() {
            return tasks;
        }

        int getNumberOfMoveTasks() {
            return numberOfMoveTasks;
        }

        GridNode getFromNode() {
            return destNode;
        }

        List<GridNode> getDestinationNodes() {
            return toNodes;
        }
    }

    private static final class GridLocalTaskMoveJob extends GridJobBase<TaskMoveOperation, Boolean> {
        private static final long serialVersionUID = 8663230291630251507L;

        private boolean isAsync = false;
        private transient boolean suceed = true;

        @GridConfigResource
        private GridConfiguration config;

        @SuppressWarnings("unused")
        public GridLocalTaskMoveJob() {
            super();
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        public boolean isAsyncOps() {
            return isAsync;
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, TaskMoveOperation ops)
                throws GridException {
            this.isAsync = ops.isAsyncOps();

            final List<GridTask> tasks = ops.getTasks();
            final List<GridNode> destNodes = ops.getDestinationNodes();

            final GridConfiguration conf = this.config;
            final GridNodeSelector selector = config.getNodeSelector();

            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(tasks.size());
            for(GridTask task : tasks) {
                if(task.isFinished()) {
                    continue;
                }
                if(!task.getRelocatability().isRelocatable()) {
                    continue;
                }
                if(task.cancel()) {
                    GridNode node = selector.selectNode(destNodes, conf);
                    map.put(task, node);
                }
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTask task, GridTaskResult result)
                throws GridException {
            final GridException error = result.getException();
            if(error != null) {
                if(LOG.isWarnEnabled()) {
                    LOG.warn(error);
                }
                this.suceed = false;
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public Boolean reduce() throws GridException {
            return suceed;
        }
    }

    public static final class GridTaskStealJob extends GridJobBase<TaskMoveOperation, Boolean> {
        private static final long serialVersionUID = 1293892419517866558L;

        private boolean isAsyncOps;
        private transient boolean suceed = true;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        public GridTaskStealJob() {
            super();
        }

        @Override
        public boolean isAsyncOps() {
            return isAsyncOps;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        public GridNode getJobNode() {
            GridCommunicationManager communicator = registry.getCommunicationManager();
            GridNode node = communicator.getLocalNode();
            return node;
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, TaskMoveOperation ops)
                throws GridException {
            this.isAsyncOps = ops.isAsyncOps();

            final List<GridNode> destNodes = ops.getDestinationNodes();
            final int destSize = destNodes.size();
            if(destSize == 0) {
                LOG.warn("No destination was specified for task-stealing");
                return Collections.emptyMap();
            }

            // step #1
            // steal tasks from the specified node
            GridNode fromNode = ops.getFromNode();
            int tasks = ops.getNumberOfMoveTasks();
            final GridTask[] stealedTasks = stealTasks(fromNode, tasks);

            // steps #2
            // send stealed tasks to destination nodes
            final int totalStealedTasks = stealedTasks.length;
            final int tasksPerNode = totalStealedTasks / destSize;
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(destSize);
            int i = 0;
            for(GridNode node : destNodes) {
                for(int j = 0; j < tasksPerNode; j++) {
                    map.put(stealedTasks[i++], node);
                }
            }
            for(int j = 0; i < totalStealedTasks; i++, j++) {
                GridNode node = destNodes.get(j);
                map.put(stealedTasks[i], node);
            }
            return map;
        }

        @Nullable
        private GridTask[] stealTasks(GridNode fromNode, int tasks) throws GridException {
            final GridCommunicationManager communicator = registry.getCommunicationManager();
            final SenderResponseTaskQueue responseQueue = registry.getTaskManager().getSenderResponseQueue();

            // register result queue
            BlockingQueue<GridTaskResult> resultQueue = new SynchronousQueue<GridTaskResult>();
            String jobId = "gridool_tasksteal" + responseQueue.hashCode();
            responseQueue.addResponseQueue(jobId, resultQueue);

            final GridTaskStealingTask stealTask = new GridTaskStealingTask(this, tasks);
            communicator.sendTaskRequest(stealTask, fromNode);

            // Receive a requested class
            final GridTaskResult result;
            try {
                result = resultQueue.take();
            } catch (InterruptedException e) {
                LOG.error(e);
                throw new GridException(e);
            }
            GridException error = result.getException();
            if(error != null) {
                throw error;
            }
            GridTask[] stealedTasks = result.getResult();
            return stealedTasks;
        }

        public GridTaskResultPolicy result(GridTask task, GridTaskResult result)
                throws GridException {
            final GridException error = result.getException();
            if(error != null) {
                if(LOG.isWarnEnabled()) {
                    LOG.warn(error);
                }
                this.suceed = false;
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public Boolean reduce() throws GridException {
            return suceed;
        }
    }

}
