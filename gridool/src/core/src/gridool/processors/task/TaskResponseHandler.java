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
package gridool.processors.task;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.communication.GridCommunicationManager;
import gridool.communication.payload.GridTaskResultImpl;
import gridool.taskqueue.sender.TaskSenderListener;

import java.io.Serializable;
import java.util.List;

import javax.annotation.CheckForNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class TaskResponseHandler implements TaskResponseListener {
    private static final Log LOG = LogFactory.getLog(TaskResponseHandler.class);

    private final GridCommunicationManager communicationMgr;
    private final GridNode localNode;

    public TaskResponseHandler(@CheckForNull GridCommunicationManager communicationMgr, @CheckForNull TaskSenderListener senderListener) {
        if(communicationMgr == null) {
            throw new IllegalArgumentException("GridCommunicationManager is not set");
        }
        if(senderListener == null) {
            throw new IllegalArgumentException("TaskSenderListener is not set");
        }
        this.communicationMgr = communicationMgr;
        this.localNode = communicationMgr.getLocalNode();
    }

    @Override
    public void onResponse(GridTask task, Serializable result) {
        final GridNode dstNode = task.getSenderNode();
        final String taskId = task.getTaskId();

        if(LOG.isDebugEnabled()) {
            LOG.debug("Sending a normal response for a task [" + taskId + "] to a node [" + dstNode
                    + ']');
        }
        long taskExecTime = task.getFinishedTime() - task.getStartedTime();
        final List<GridNode> replicatedNodes = task.getReplicatedNodes();
        final GridTaskResult taskResult = new GridTaskResultImpl(taskId, localNode, replicatedNodes, result, taskExecTime);
        try {
            communicationMgr.sendTaskResponse(task, taskResult, dstNode);
        } catch (GridException e) {
            LOG.error("Failed to sending a response for task [" + taskId + "].");
        }
    }

    @Override
    public void onCaughtException(GridTask task, GridException exception) {
        final GridNode dstNode = task.getSenderNode();
        final String taskId = task.getTaskId();

        if(LOG.isWarnEnabled()) {
            LOG.warn("Sending an error response for a task [" + taskId + "] to a node [" + dstNode
                    + ']');
        }

        final GridTaskResult taskResult = new GridTaskResultImpl(taskId, localNode, exception).scheduleFailover();
        try {
            communicationMgr.sendTaskResponse(task, taskResult, dstNode);
        } catch (GridException e) {
            LOG.error("Failed to sending an error response for task [" + taskId + "].");
        }
    }

}
