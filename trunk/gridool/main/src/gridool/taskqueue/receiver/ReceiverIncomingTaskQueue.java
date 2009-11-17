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
package gridool.taskqueue.receiver;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.communication.messages.GridTaskCancelMessage;
import gridool.communication.messages.GridTaskRequestMessage;
import gridool.construct.GridTaskInfo;
import gridool.deployment.GridPerNodeClassLoader;
import gridool.deployment.PeerClassLoader;
import gridool.marshaller.GridMarshaller;

import java.util.concurrent.BlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.jsr166.LinkedTransferQueue;
import xbird.util.lang.PrintUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class ReceiverIncomingTaskQueue implements TaskReceiverListener {
    private static final Log LOG = LogFactory.getLog(ReceiverIncomingTaskQueue.class);

    private final BlockingQueue<GridTask> waitingTaskQueue;

    private final GridResourceRegistry resourceRegistry;
    private final GridMarshaller<GridTask> marshaller;

    public ReceiverIncomingTaskQueue(@Nonnull GridResourceRegistry resourceRegistry) {
        this.waitingTaskQueue = new LinkedTransferQueue<GridTask>();
        this.marshaller = resourceRegistry.getTaskMarshaller();
        this.resourceRegistry = resourceRegistry;
    }

    public void onRequest(@Nonnull GridTaskRequestMessage request) throws GridException {
        GridNode senderNode = request.getSenderNode();
        assert (senderNode != null);
        GridPerNodeClassLoader ldr = resourceRegistry.getNodeClassLoader(senderNode);
        long timestamp = request.getTimestamp();
        ClassLoader cl = new PeerClassLoader(ldr, senderNode, timestamp, resourceRegistry);
        byte[] b = request.getMessage();
        final GridTask task = marshaller.unmarshall(b, cl);
        try {
            waitingTaskQueue.put(task);
        } catch (InterruptedException ex) {
            LOG.warn(PrintUtils.prettyPrintStackTrace(ex, -1));
            throw new GridException("Failed to add a task [" + task.getTaskId()
                    + "] to task queue: " + request, ex);
        }
    }

    public void onCancelRequest(GridTaskCancelMessage request) {
        String taskId = request.getTaskId();
        GridTask taskProbe = new GridTaskInfo(taskId);
        waitingTaskQueue.remove(taskProbe);
    }

    @Nullable
    public GridTask consumeTask() throws InterruptedException {
        return waitingTaskQueue.take();
    }

    public void clear() {
        waitingTaskQueue.clear();
    }

}
