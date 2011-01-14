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
package gridool.taskqueue.sender;

import gridool.GridNode;
import gridool.GridTaskResult;
import gridool.communication.messages.GridTaskResponseMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class SenderResponseTaskQueue implements TaskSenderListener {
    private static final Log LOG = LogFactory.getLog(SenderResponseTaskQueue.class);

    private final ConcurrentMap<String, BlockingQueue<GridTaskResult>> queueMap;

    public SenderResponseTaskQueue() {
        this.queueMap = new ConcurrentHashMap<String, BlockingQueue<GridTaskResult>>(16);
    }

    public void addResponseQueue(@Nonnull String jobId, @Nonnull BlockingQueue<GridTaskResult> queue) {
        queueMap.put(jobId, queue);
    }

    @Nullable
    @Deprecated
    public BlockingQueue<GridTaskResult> getResponseQueue(@Nonnull String jobId) {
        return queueMap.get(jobId);
    }

    public void onResponse(@Nonnull GridTaskResponseMessage response) {
        final String jobId = response.getJobId();
        final GridNode senderNode = response.getSenderNode();
        final BlockingQueue<GridTaskResult> queue = queueMap.get(jobId);
        if(queue != null) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Received a GridTaskResponseMessage for a task: " + response.getTaskId()
                        + " of Job [" + jobId + "] that was executed on "
                        + (senderNode == null ? "localhost" : senderNode));
            }
            GridTaskResult result = response.getMessage();
            queue.add(result);
        } else {
            LOG.error("SenderResponseQueue is not found for a task: " + response.getTaskId()
                    + " of Job [" + jobId + "] that was executed on "
                    + (senderNode == null ? "localhost" : senderNode));
        }
    }

}
