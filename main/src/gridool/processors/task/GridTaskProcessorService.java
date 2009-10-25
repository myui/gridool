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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.GridService;
import gridool.GridTask;
import gridool.monitor.GridExecutionMonitor;
import gridool.taskqueue.GridTaskQueueManager;
import gridool.taskqueue.receiver.ReceiverIncomingTaskQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskProcessorService implements GridService, Runnable {

    @Nonnull
    private final ReceiverIncomingTaskQueue incomingQueue;
    @Nonnull
    private final GridTaskProcessor proc;
    @Nullable
    private Thread runningThread = null;

    public GridTaskProcessorService(@Nonnull GridResourceRegistry resourceRegistry, @Nonnull TaskResponseListener respListener, @Nonnull GridExecutionMonitor monitor, @Nonnull GridConfiguration config) {
        GridTaskQueueManager taskManager = resourceRegistry.getTaskManager();
        this.incomingQueue = taskManager.getReceiverIncomingQueue();
        this.proc = new GridTaskProcessor(resourceRegistry, respListener, monitor, config);
    }

    public String getServiceName() {
        return GridTaskProcessorService.class.getName();
    }

    public void start() throws GridException {
        final Thread thread = new Thread(this, GridTaskProcessorService.class.getSimpleName());
        thread.setDaemon(true);
        this.runningThread = thread;
        thread.start();
    }

    /**
     * @link http://www.ibm.com/developerworks/java/library/j-jtp05236.html
     */
    public void run() {
        for(;;) {
            final GridTask task;
            try {
                task = incomingQueue.consumeTask();
            } catch (InterruptedException e) {
                // fall through and retry
                Thread.currentThread().interrupt();
                return;
            }
            if(task != null) {
                proc.processTask(task);
            }
        }
    }

    public void stop() throws GridException {
        if(runningThread != null) {
            runningThread.interrupt();
        }
        proc.stop(true); // TODO REVIEWME
        incomingQueue.clear();
    }

}
