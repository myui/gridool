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
package gridool.taskqueue;

import javax.annotation.Nonnull;

import gridool.GridResourceRegistry;
import gridool.taskqueue.receiver.ReceiverIncomingTaskQueue;
import gridool.taskqueue.sender.SenderResponseTaskQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskQueueManager {

    @Nonnull
    private final SenderResponseTaskQueue senderResponseQueue;
    @Nonnull
    private final ReceiverIncomingTaskQueue receiverQueue;

    public GridTaskQueueManager(@Nonnull GridResourceRegistry resourceRegistry) {
        this.senderResponseQueue = new SenderResponseTaskQueue();        
        this.receiverQueue = new ReceiverIncomingTaskQueue(resourceRegistry);
        resourceRegistry.setTaskManager(this);
    }

    @Nonnull
    public SenderResponseTaskQueue getSenderResponseQueue() {
        return senderResponseQueue;
    }

    @Nonnull
    public ReceiverIncomingTaskQueue getReceiverIncomingQueue() {
        return receiverQueue;
    }

}
