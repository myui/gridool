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
package gridool.processors.job;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.communication.payload.GridTaskResultImpl;
import gridool.discovery.DiscoveryEvent;
import gridool.discovery.GridDiscoveryListener;
import gridool.util.struct.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeFailureHandler implements GridDiscoveryListener {

    private final Map<GridTask, GridNode> mappedTasks;
    private final Map<String, Pair<GridTask, List<Future<?>>>> remainingTaskMap;
    private final BlockingQueue<GridTaskResult> resultQueue;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private Map<GridNode, String> node2taskMap = null;

    public GridNodeFailureHandler(@Nonnull final Map<GridTask, GridNode> mappedTasks, @Nonnull final Map<String, Pair<GridTask, List<Future<?>>>> taskMap, @Nonnull final BlockingQueue<GridTaskResult> resultQueue) {
        this.mappedTasks = mappedTasks;
        this.remainingTaskMap = taskMap;
        this.resultQueue = resultQueue;
    }

    public void onChannelClosed() {} // nop

    public void onDiscovery(DiscoveryEvent event, GridNode node) {
        switch(event) {
            case leave:
            case dropout:
                handleNodeFailure(node, event);
                break;
            default:
                break;
        }
    }

    private void handleNodeFailure(@Nonnull GridNode node, @Nonnull DiscoveryEvent event) {
        final Map<GridNode, String> map;
        synchronized(lock) {
            if(node2taskMap == null) {
                map = new HashMap<GridNode, String>();
                for(Pair<GridTask, List<Future<?>>> e : remainingTaskMap.values()) {
                    GridTask task = e.getFirst();
                    GridNode mappedNode = mappedTasks.get(task);
                    String taskId = task.getTaskId();
                    map.put(mappedNode, taskId);
                }
                this.node2taskMap = map;
            } else {
                map = this.node2taskMap;
            }
        }
        final String taskId = map.remove(node);
        if(taskId != null && remainingTaskMap.containsKey(taskId)) {
            GridException err = new GridException("Node " + node + " dropped: " + event);
            GridTaskResult result = new GridTaskResultImpl(taskId, node, err).scheduleFailover();
            resultQueue.add(result);
        }
    }

}
