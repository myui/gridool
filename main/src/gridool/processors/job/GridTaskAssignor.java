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
import gridool.communication.GridCommunicationManager;
import gridool.communication.payload.GridNodeInfo;
import gridool.communication.payload.GridTaskResultImpl;
import gridool.processors.task.GridTaskProcessor;

import java.util.concurrent.BlockingQueue;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskAssignor implements Runnable {
    private static final Log LOG = LogFactory.getLog(GridTaskAssignor.class);

    private final GridTask task;
    private final GridNode dstNode;

    private final GridNodeInfo localNodeProbe;
    private final GridTaskProcessor taskProc;
    private final GridCommunicationManager communicationMgr;
    private final BlockingQueue<GridTaskResult> resultQueue;

    public GridTaskAssignor(@Nonnull GridTask task, @Nonnull GridNode dstNode, @CheckForNull GridTaskProcessor taskProc, @Nonnull GridCommunicationManager communicationMgr, @Nonnull BlockingQueue<GridTaskResult> resultQueue) {
        if(taskProc == null) {
            throw new IllegalArgumentException("GridTaskProcessor is illegally not set");
        }
        this.task = task;
        this.dstNode = dstNode;
        this.taskProc = taskProc;
        this.communicationMgr = communicationMgr;
        this.resultQueue = resultQueue;
        this.localNodeProbe = communicationMgr.getLocalNodeInfo();
        assert (localNodeProbe != null);
    }

    public void run() {
        if(localNodeProbe.equals(dstNode)) {
            try {
                taskProc.processTask(task); // shortcut for local task
            } catch (Throwable e) {
                LOG.error(e);
            }
            return;
        }
        try {
            communicationMgr.sendTaskRequest(task, dstNode);
        } catch (GridException ex) {
            LOG.error("Failed sending a Task[ " + task + " ] to node [ " + dstNode + " ].");
            String taskId = task.getTaskId();
            GridTaskResult result = new GridTaskResultImpl(taskId, dstNode, ex);
            resultQueue.add(result);
        } catch (Throwable e) {
            LOG.fatal("Failed sending a Task[ " + task + " ] to node [ " + dstNode + " ].");
            String taskId = task.getTaskId();
            GridTaskResult result = new GridTaskResultImpl(taskId, dstNode, new GridException(e));
            resultQueue.add(result);
        }
    }

}
