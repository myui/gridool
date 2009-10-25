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
package gridool.communication.listeners;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTaskResult;
import gridool.communication.CommunicationMessageListener;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.GridCommunicationService;
import gridool.communication.GridTopic;
import gridool.communication.GridCommunicationMessage.GridMessageType;
import gridool.communication.messages.GridTaskCancelMessage;
import gridool.communication.messages.GridTaskMessage;
import gridool.communication.messages.GridTaskRequestMessage;
import gridool.communication.messages.GridTaskResponseMessage;
import gridool.communication.payload.GridTaskResultImpl;
import gridool.taskqueue.GridTaskQueueManager;
import gridool.taskqueue.receiver.ReceiverIncomingTaskQueue;
import gridool.taskqueue.sender.SenderResponseTaskQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class TaskMessageListener implements CommunicationMessageListener {
    private static final Log LOG = LogFactory.getLog(TaskMessageListener.class);

    @Nonnull
    private final GridCommunicationService communicationServ;
    @Nonnull
    private final SenderResponseTaskQueue senderRespListener;
    @Nonnull
    private final ReceiverIncomingTaskQueue receiverListener;

    public TaskMessageListener(@Nonnull GridCommunicationService srv, @Nonnull GridTaskQueueManager taskMgr) {
        assert (srv != null);
        assert (taskMgr != null);
        this.communicationServ = srv;
        this.senderRespListener = taskMgr.getSenderResponseQueue();
        this.receiverListener = taskMgr.getReceiverIncomingQueue();
    }

    public GridTopic getSubcribingTopic() {
        return GridTopic.TASK;
    }

    public void onMessage(@Nonnull GridCommunicationMessage msg, @Nonnull GridNode localNode) {
        assert (msg.getTopic() == GridTopic.TASK) : msg;
        final GridMessageType type = msg.getMessageType();
        switch(type) {
            case taskRequest:
                final GridTaskRequestMessage reqMsg = (GridTaskRequestMessage) msg;
                try {
                    receiverListener.onRequest(reqMsg);
                } catch (GridException e) {
                    onFailure(reqMsg, localNode, e);
                }
                break;
            case taskCancelRequest:
                @SuppressWarnings("unused")
                GridTaskCancelMessage cancelMsg = (GridTaskCancelMessage) msg;
                // TODO FIXME
                break;
            case taskResponse:
                GridTaskResponseMessage respMsg = (GridTaskResponseMessage) msg;
                senderRespListener.onResponse(respMsg);
                break;
            default:
                LOG.fatal("Detected an unexpected type: " + type);
                throw new IllegalStateException("Detected an unexpected type: " + type);
        }
    }

    private void onFailure(GridTaskMessage msg, GridNode localNode, GridException exception) {
        String taskId = msg.getTaskId();
        GridTaskResult result = new GridTaskResultImpl(taskId, localNode, exception);
        final GridTaskResponseMessage response = new GridTaskResponseMessage(result);
        final GridNode sender = msg.getSenderNode();
        try {
            communicationServ.sendMessage(sender, response);
        } catch (GridException e) {
            LOG.error("Exception caused while sending a task failure message (" + msg
                    + ") to node (" + sender + ").");
        }
    }

}
