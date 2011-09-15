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
package gridool.communication;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.communication.messages.GridTaskRequestMessage;
import gridool.communication.messages.GridTaskResponseMessage;
import gridool.marshaller.GridMarshaller;
import gridool.taskqueue.GridTaskQueueManager;
import gridool.taskqueue.sender.TaskSenderListener;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridCommunicationManager {

    private final GridCommunicationService service;
    private final TaskSenderListener senderListener;
    private final GridNode localNode;
    private final GridMarshaller marshaller;

    public GridCommunicationManager(@Nonnull GridResourceRegistry resourceRegistry, @Nonnull GridCommunicationService srvc) {
        this.service = srvc;
        GridTaskQueueManager taskMgr = resourceRegistry.getTaskManager();
        this.senderListener = taskMgr.getSenderResponseQueue();
        assert (senderListener != null);
        this.localNode = srvc.getLocalNode();
        this.marshaller = resourceRegistry.getMarshaller();
        resourceRegistry.setCommunicationManager(this);
    }

    public GridNode getLocalNode() {
        return localNode;
    }

    public void sendTaskRequest(@Nonnull GridTask task, @Nonnull GridNode dstNode)
            throws GridException {// TODO REVIEWME DESIGN create shortcut here?
        String taskId = task.getTaskId();
        byte[] b = marshaller.marshall(task);
        String deployGroup = task.getDeploymentGroup();
        GridTaskRequestMessage msg = new GridTaskRequestMessage(taskId, b, deployGroup);
        service.sendMessage(dstNode, msg);
    }

    public void sendTaskResponse(@Nonnull GridTask task, @Nonnull GridTaskResult result, @Nonnull GridNode dstNode)
            throws GridException {
        if(localNode.equals(dstNode)) {
            String jobId = task.getJobId();
            senderListener.onResponse(jobId, result);
        } else {
            String taskId = task.getTaskId();
            byte[] b = marshaller.marshall(result);
            String deployGroup = task.getDeploymentGroup();
            GridTaskResponseMessage msg = new GridTaskResponseMessage(taskId, b, deployGroup);
            service.sendMessage(dstNode, msg);
        }
    }

}
