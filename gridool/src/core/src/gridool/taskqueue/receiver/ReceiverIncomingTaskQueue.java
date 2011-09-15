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
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.communication.messages.GridTaskCancelMessage;
import gridool.communication.messages.GridTaskRequestMessage;
import gridool.construct.GridTaskInfo;
import gridool.marshaller.GridMarshaller;
import gridool.util.concurrent.jsr166.LinkedTransferQueue;
import gridool.util.lang.PrintUtils;

import java.util.concurrent.BlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private final GridMarshaller marshaller;
    private final GridResourceRegistry registry;

    public ReceiverIncomingTaskQueue(@Nonnull GridResourceRegistry resourceRegistry) {
        this.waitingTaskQueue = new LinkedTransferQueue<GridTask>(); //new LinkedBlockingDeque<GridTask>();
        this.marshaller = resourceRegistry.getMarshaller();
        this.registry = resourceRegistry;
    }

    @Override
    public void onRequest(@Nonnull GridTaskRequestMessage request) throws GridException {
        byte[] b = request.getMessage();
        String deployGroup = request.getDeploymentGroup();
        ClassLoader ldr = registry.getDeploymentGroupClassLoader(deployGroup);
        final GridTask task = marshaller.unmarshall(b, ldr);
        try {
            waitingTaskQueue.put(task);
        } catch (InterruptedException ex) {
            LOG.warn(PrintUtils.prettyPrintStackTrace(ex, -1));
            throw new GridException("Failed to add a task [" + task.getTaskId()
                    + "] to task queue: " + request, ex);
        }
    }

    @Override
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
