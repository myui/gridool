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
package gridool.processors;

import gridool.GridConfiguration;
import gridool.GridResourceRegistry;
import gridool.communication.GridCommunicationManager;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.task.GridTaskProcessorService;
import gridool.processors.task.TaskResponseHandler;
import gridool.processors.task.TaskResponseListener;
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
public final class GridProcessorProvider {

    private GridProcessorProvider() {}

    @Nonnull
    public static GridTaskProcessorService createTaskProcessorService(@Nonnull GridCommunicationManager communicationMgr, @Nonnull GridExecutionMonitor monitor, @Nonnull GridResourceRegistry resourceRegistry, @Nonnull GridConfiguration config) {
        GridTaskQueueManager taskMgr = resourceRegistry.getTaskManager();
        TaskSenderListener senderListener = taskMgr.getSenderResponseQueue();
        TaskResponseListener respListener = new TaskResponseHandler(communicationMgr, senderListener);
        return new GridTaskProcessorService(resourceRegistry, respListener, monitor, config);
    }

}
